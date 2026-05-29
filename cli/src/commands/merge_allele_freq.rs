use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::MergeAlleleFreqArgs;

/// Inner-join per-population allele-frequency TSVs on `locus_key`, reproducing
/// `04_population_level/fst_islands/scripts/01_load_merge.py`:
/// - `allele_number == 0` -> freq/number become missing (dropped),
/// - keep only loci present and non-missing in EVERY population,
/// - row order follows the first population's file order (pandas left-join),
/// - rsid is carried from the first population.
///
/// Output is content-equivalent to the Python merge and is consumed directly by
/// `bvs fst` (verified end-to-end). Float text may differ from pandas' repr; the
/// values are identical.
pub fn run_merge_allele_freq(args: MergeAlleleFreqArgs) -> Result<()> {
    if args.populations.len() < 2 {
        bail!("provide at least 2 --population LABEL=PATH entries");
    }
    let pops = parse_populations(&args.populations)?;

    // First population defines row order + rsid.
    let (order, mut rsid, mut freq_cols, mut num_cols) = {
        let rows = read_country(&pops[0].1)?;
        let mut order = Vec::with_capacity(rows.len());
        let mut rsid: HashMap<String, String> = HashMap::new();
        let mut f0: HashMap<String, f64> = HashMap::new();
        let mut n0: HashMap<String, f64> = HashMap::new();
        for r in rows {
            if !f0.contains_key(&r.locus) {
                order.push(r.locus.clone());
            }
            rsid.insert(r.locus.clone(), r.rsid);
            f0.insert(r.locus.clone(), r.freq);
            n0.insert(r.locus.clone(), r.number);
        }
        (order, rsid, vec![f0], vec![n0])
    };

    // Remaining populations as lookup maps.
    for (_, path) in &pops[1..] {
        let rows = read_country(path)?;
        let mut f: HashMap<String, f64> = HashMap::new();
        let mut n: HashMap<String, f64> = HashMap::new();
        for r in rows {
            f.insert(r.locus.clone(), r.freq);
            n.insert(r.locus, r.number);
        }
        freq_cols.push(f);
        num_cols.push(n);
    }

    let labels: Vec<&str> = pops.iter().map(|(l, _)| l.as_str()).collect();
    let mut out_freq = create(&args.merged_freq)?;
    let mut out_number = create(&args.merged_number)?;
    let mut out_annotated = match &args.merged_annotated {
        Some(p) => Some(create(p)?),
        None => None,
    };

    writeln!(out_freq, "locus_key\t{}", labels.join("\t"))?;
    writeln!(out_number, "locus_key\t{}", labels.join("\t"))?;
    if let Some(w) = out_annotated.as_mut() {
        writeln!(w, "locus_key\trsid\t{}", labels.join("\t"))?;
    }

    let mut kept = 0usize;
    'locus: for locus in &order {
        let mut freqs = Vec::with_capacity(labels.len());
        let mut nums = Vec::with_capacity(labels.len());
        for k in 0..labels.len() {
            // inner join: present in every population
            let (Some(&f), Some(&n)) = (freq_cols[k].get(locus), num_cols[k].get(locus)) else {
                continue 'locus;
            };
            // dropna: any missing (incl. allele_number==0 -> NaN) drops the locus
            if f.is_nan() || n.is_nan() {
                continue 'locus;
            }
            freqs.push(f);
            nums.push(n);
        }
        write!(out_freq, "{locus}")?;
        for f in &freqs {
            write!(out_freq, "\t{f}")?;
        }
        writeln!(out_freq)?;
        write!(out_number, "{locus}")?;
        for n in &nums {
            write!(out_number, "\t{n}")?;
        }
        writeln!(out_number)?;
        if let Some(w) = out_annotated.as_mut() {
            let rs = rsid.get(locus).map(|s| s.as_str()).unwrap_or("");
            write!(w, "{locus}\t{rs}")?;
            for f in &freqs {
                write!(w, "\t{f}")?;
            }
            writeln!(w)?;
        }
        kept += 1;
    }

    out_freq.flush()?;
    out_number.flush()?;
    if let Some(w) = out_annotated.as_mut() {
        w.flush()?;
    }
    rsid.clear();

    eprintln!(
        "✅ merge-allele-freq: {} populations, {} loci kept (all-population non-missing)",
        labels.len(),
        kept
    );
    println!("✅ Wrote merged matrices ({} loci)", kept);
    Ok(())
}

struct CountryRow {
    locus: String,
    freq: f64,
    number: f64,
    rsid: String,
}

/// Parse one per-population allele_freq TSV
/// (locus_key, allele_count, allele_number, num_homo, num_hetero, allele_freq, rsid).
/// allele_number == 0 -> freq and number set to NaN (missing).
fn read_country(path: &Path) -> Result<Vec<CountryRow>> {
    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let mut reader = BufReader::new(file);
    let mut header = String::new();
    if reader.read_line(&mut header)? == 0 {
        bail!("{:?} is empty", path);
    }
    let mut out = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        let f: Vec<&str> = trimmed.split('\t').collect();
        if f.len() < 7 {
            continue;
        }
        let number: f64 = f[2].trim().parse().unwrap_or(f64::NAN);
        let freq: f64 = f[5].trim().parse().unwrap_or(f64::NAN);
        let (freq, number) = if number == 0.0 {
            (f64::NAN, f64::NAN)
        } else {
            (freq, number)
        };
        out.push(CountryRow {
            locus: f[0].to_string(),
            freq,
            number,
            rsid: f[6].to_string(),
        });
    }
    Ok(out)
}

fn parse_populations(entries: &[String]) -> Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let (label, path) = e
            .split_once('=')
            .with_context(|| format!("--population must be LABEL=PATH, got {e:?}"))?;
        if label.is_empty() || path.is_empty() {
            bail!("--population must be LABEL=PATH, got {e:?}");
        }
        out.push((label.to_string(), PathBuf::from(path)));
    }
    Ok(out)
}

fn create(path: &Path) -> Result<BufWriter<File>> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Create directory {:?}", parent))?;
        }
    }
    Ok(BufWriter::new(
        File::create(path).with_context(|| format!("Create {:?}", path))?,
    ))
}
