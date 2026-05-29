use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::FstArgs;

/// Pairwise Weir & Cockerham 1984 FST (ratio-of-averages), reproducing
/// `04_population_level/fst_islands/scripts/02_compute_fst.py`. Reads the merged
/// allele-frequency and allele-number matrices and writes a population x
/// population matrix, `%.6f`, matching the Python output byte-for-byte.
pub fn run_fst(args: FstArgs) -> Result<()> {
    let (pops, freq) = read_matrix(&args.merged_freq)?;
    let (pops_n, number) = read_matrix(&args.merged_number)?;
    if pops != pops_n {
        bail!(
            "population columns differ between freq ({:?}) and number ({:?})",
            pops,
            pops_n
        );
    }
    let p = pops.len();
    if p < 2 {
        bail!("need at least 2 populations, got {}", p);
    }

    // Align rows by locus_key (pandas join semantics); iterate loci present in both.
    let mut fst = vec![vec![0.0f64; p]; p];
    for i in 0..p {
        for j in (i + 1)..p {
            let (mut a_sum, mut abc_sum) = (0.0f64, 0.0f64);
            for (locus, fr) in &freq {
                let Some(nr) = number.get(locus) else {
                    continue;
                };
                let (p1, n1, p2, n2) = (fr[i], nr[i], fr[j], nr[j]);
                if let Some((a, b, c)) = wc84(p1, n1, p2, n2) {
                    a_sum += a;
                    abc_sum += a + b + c;
                }
            }
            let value = if abc_sum != 0.0 {
                a_sum / abc_sum
            } else {
                f64::NAN
            };
            fst[i][j] = value;
            fst[j][i] = value;
        }
    }

    write_matrix(&args.output, &pops, &fst)?;
    eprintln!(
        "✅ fst: {} populations, {} loci -> {}",
        p,
        freq.len(),
        args.output.display()
    );
    println!("✅ Wrote FST matrix to {}", args.output.display());
    Ok(())
}

/// WC84 per-SNP (a, b, c) components for a pair. Returns None when any component
/// is non-finite (skipped from the genome-wide ratio, matching the numpy mask).
fn wc84(p1: f64, n1: f64, p2: f64, n2: f64) -> Option<(f64, f64, f64)> {
    let r = 2.0;
    let n_total = n1 + n2;
    if n_total == 0.0 {
        return None;
    }
    let n_bar = n_total / r;
    let p_bar = (n1 * p1 + n2 * p2) / n_total;
    let nc = (n_total - (n1 * n1 + n2 * n2) / n_total) / (r - 1.0);
    let s2 = (n1 * (p1 - p_bar).powi(2) + n2 * (p2 - p_bar).powi(2)) / ((r - 1.0) * n_bar);
    let h1 = if n1 > 1.0 {
        2.0 * n1 * p1 * (1.0 - p1) / (2.0 * n1 - 1.0)
    } else {
        f64::NAN
    };
    let h2 = if n2 > 1.0 {
        2.0 * n2 * p2 * (1.0 - p2) / (2.0 * n2 - 1.0)
    } else {
        f64::NAN
    };
    let h_bar = (h1 + h2) / r;
    let inner = p_bar * (1.0 - p_bar) - ((r - 1.0) / r) * s2 - h_bar / 4.0;
    let a = (n_bar / nc) * (s2 - inner / (n_bar - 1.0));
    let b = (n_bar / (n_bar - 1.0))
        * (p_bar * (1.0 - p_bar) - ((r - 1.0) / r) * s2 - (2.0 * n_bar - 1.0) * h_bar / (4.0 * n_bar));
    let c = h_bar / 2.0;
    if a.is_finite() && b.is_finite() && c.is_finite() {
        Some((a, b, c))
    } else {
        None
    }
}

/// Read a `locus_key<TAB>pop1<TAB>...` matrix. Returns (population names, map
/// locus_key -> per-population values). Empty / `nan` cells parse to NaN.
fn read_matrix(path: &Path) -> Result<(Vec<String>, HashMap<String, Vec<f64>>)> {
    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let mut reader = BufReader::new(file);
    let mut header = String::new();
    if reader.read_line(&mut header)? == 0 {
        bail!("{:?} is empty", path);
    }
    let pops: Vec<String> = header
        .trim_end()
        .split('\t')
        .skip(1)
        .map(|s| s.to_string())
        .collect();
    let mut rows: HashMap<String, Vec<f64>> = HashMap::new();
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
        let mut fields = trimmed.split('\t');
        let locus = match fields.next() {
            Some(l) => l.to_string(),
            None => continue,
        };
        let values: Vec<f64> = fields
            .map(|v| {
                let v = v.trim();
                if v.is_empty() || v.eq_ignore_ascii_case("nan") {
                    f64::NAN
                } else {
                    v.parse::<f64>().unwrap_or(f64::NAN)
                }
            })
            .collect();
        if values.len() == pops.len() {
            rows.insert(locus, values);
        }
    }
    Ok((pops, rows))
}

fn write_matrix(path: &Path, pops: &[String], fst: &[Vec<f64>]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Create directory {:?}", parent))?;
        }
    }
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("Create {:?}", path))?);
    // Header: empty first cell (unnamed index) then population names.
    writeln!(writer, "\t{}", pops.join("\t"))?;
    for (i, pop) in pops.iter().enumerate() {
        write!(writer, "{pop}")?;
        for value in &fst[i] {
            if value.is_nan() {
                write!(writer, "\t")?;
            } else {
                write!(writer, "\t{value:.6}")?;
            }
        }
        writeln!(writer)?;
    }
    writer.flush()?;
    Ok(())
}
