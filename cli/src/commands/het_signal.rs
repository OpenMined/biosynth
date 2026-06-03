use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;

use crate::genotype_reader::{detect_delimiter, open_text_reader, RowOutcome, RowParser};
use crate::HetSignalArgs;

const LOOKAHEAD_LINES: usize = 2048;
const SUBSAMPLE_STEP: u32 = 14; // autosomes only (matches sba.SUBSAMPLE_STEP)

/// Per-sample het-signal cache for the sex-biased admixture NMF, reproducing
/// `fast_sex_biased_admixture._signals_for_region`. For each sample it emits, per
/// region (autosomes 1-22 subsampled by rsid_num%14, and X), the kept
/// `(rsid_num, signal)` arrays where signal = 1 (het) / 0 (hom); no-calls dropped.
/// The NMF (sklearn, seed 42) then runs on these in Python -> identical results.
///
/// Output `.bvhs` binary per sample (replaces the per-sample `.npz` compact_cache):
///   magic "BVHS", version u8=1,
///   auto_n u32, auto_ids[u32], auto_sig[i8],
///   x_n u32,    x_ids[u32],    x_sig[i8]   (all little-endian)
pub fn run_het_signal(args: HetSignalArgs) -> Result<()> {
    let overall = Instant::now();
    let samples = discover_samples(&args.input)?;
    if samples.is_empty() {
        bail!(
            "No sample subdirectories with .txt files found under {:?}",
            args.input
        );
    }
    fs::create_dir_all(&args.out_dir).with_context(|| format!("Create {:?}", args.out_dir))?;
    eprintln!("▶️  het-signal: {} samples", samples.len());

    let out_dir = args.out_dir.clone();
    // A bad file must not abort the run: log it and skip its cache.
    let written: usize = samples
        .par_iter()
        .map(|(sid, path)| {
            match parse_sample(path)
                .and_then(|sig| write_bvhs(&out_dir.join(format!("{sid}.bvhs")), &sig))
            {
                Ok(()) => 1usize,
                Err(e) => {
                    eprintln!(
                        "⚠️  het-signal: skipping unreadable sample {sid} ({}): {e:#}",
                        path.display()
                    );
                    0
                }
            }
        })
        .sum();

    eprintln!(
        "✅ het-signal: wrote {} caches in {:.2}s",
        written,
        overall.elapsed().as_secs_f64()
    );
    println!(
        "✅ Wrote {} .bvhs caches to {}",
        written,
        args.out_dir.display()
    );
    Ok(())
}

#[derive(Default)]
struct SampleSignals {
    auto_ids: Vec<u32>,
    auto_sig: Vec<i8>,
    x_ids: Vec<u32>,
    x_sig: Vec<i8>,
    // Per-sample stats (over ALL rows, matching _compact_stats) for the results table.
    auto_snps: u64,
    x_snps: u64,
    auto_het: f64,
    x_het: f64,
    auto_lrr_mean: f64,
    x_lrr_mean: f64,
}

#[derive(Default, Clone, Copy)]
struct ChromAcc {
    count: u64,
    het: u64,
    lrr_sum: f64,
    lrr_cnt: u64,
}

/// is_het per `read_txt`: 2-char, alleles differ, no '0'. (Note: only '0' is
/// excluded here, unlike the signal's '0'/'-' check — matches the Python.)
fn is_het(gt: &str) -> bool {
    let c: Vec<char> = gt.chars().collect();
    c.len() == 2 && c[0] != c[1] && !c.contains(&'0')
}

/// mean of finite values; NaN if none (matches `_finite_mean`).
fn finite_mean(v: &[f64]) -> f64 {
    let f: Vec<f64> = v.iter().copied().filter(|x| x.is_finite()).collect();
    if f.is_empty() {
        f64::NAN
    } else {
        f.iter().sum::<f64>() / f.len() as f64
    }
}

/// rs<digits> -> Some(number); anything else None. Matches `_rsid_num` + the
/// `^rs\d+$` filter (full match: "rs" then all digits).
fn rsid_num(rsid: &str) -> Option<u32> {
    let rest = rsid.strip_prefix("rs")?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse::<u32>().ok().filter(|&n| n > 0)
}

/// `_genotype_signal`: 2-char, no '0'/'-', -> 1 (het) / 0 (hom); else None (no-call).
fn genotype_signal(gt: &str) -> Option<i8> {
    let g = gt.trim().to_ascii_uppercase();
    let chars: Vec<char> = g.chars().collect();
    if chars.len() != 2 || chars.contains(&'0') || chars.contains(&'-') {
        return None;
    }
    Some(if chars[0] != chars[1] { 1 } else { 0 })
}

fn parse_sample(path: &Path) -> Result<SampleSignals> {
    let mut reader = open_text_reader(path)?;
    let mut buffered: Vec<String> = Vec::new();
    let mut buf = String::new();
    while buffered.len() < LOOKAHEAD_LINES {
        buf.clear();
        if reader.read_line(&mut buf)? == 0 {
            break;
        }
        buffered.push(buf.clone());
    }
    if buffered.is_empty() {
        return Ok(SampleSignals::default());
    }
    let delim = detect_delimiter(&buffered);
    let mut parser = RowParser::new(delim);
    let mut out = SampleSignals::default();
    // dedup rsid keep-first, per region (regions are disjoint by chrom).
    let mut seen_auto: HashSet<u32> = HashSet::new();
    let mut seen_x: HashSet<u32> = HashSet::new();
    // Per-chrom stats over ALL rows (autosomes 1-22 + X), matching _compact_stats.
    let mut chrom_acc: std::collections::HashMap<u32, ChromAcc> = std::collections::HashMap::new();
    let mut x_acc = ChromAcc::default();

    let mut handle = |parser: &mut RowParser, line: &str| -> Result<()> {
        // A bad row must not break the file: skip it and keep going.
        let outcome = match parser.consume_line(line) {
            Ok(o) => o,
            Err(_) => return Ok(()),
        };
        if let RowOutcome::Parsed(row) = outcome {
            let chrom_int = row.chrom.parse::<u32>().ok();
            let is_auto = chrom_int.is_some_and(|c| (1..=22).contains(&c));
            let is_x = row.chrom == "X";
            // Stats accumulation over ALL rows of autosomes/X (before any filter).
            if is_auto || is_x {
                let acc = if is_auto {
                    chrom_acc.entry(chrom_int.unwrap()).or_default()
                } else {
                    &mut x_acc
                };
                acc.count += 1;
                if let Some(g) = &row.genotype {
                    if is_het(&g.to_ascii_uppercase()) {
                        acc.het += 1;
                    }
                }
                if let Some(lrr) = row
                    .lrr
                    .as_deref()
                    .and_then(|s| s.trim().parse::<f64>().ok())
                {
                    if lrr.is_finite() {
                        acc.lrr_sum += lrr;
                        acc.lrr_cnt += 1;
                    }
                }
            }
            let Some(num) = rsid_num(&row.rsid) else {
                return Ok(());
            };
            if !is_auto && !is_x {
                return Ok(());
            }
            // dedup by rsid keep-first within region
            if is_auto {
                if !seen_auto.insert(num) {
                    return Ok(());
                }
                if num % SUBSAMPLE_STEP != 0 {
                    return Ok(());
                }
            } else if !seen_x.insert(num) {
                return Ok(());
            }
            let gt = match &row.genotype {
                Some(g) => g,
                None => return Ok(()),
            };
            let Some(sig) = genotype_signal(gt) else {
                return Ok(()); // no-call dropped
            };
            if is_auto {
                out.auto_ids.push(num);
                out.auto_sig.push(sig);
            } else {
                out.x_ids.push(num);
                out.x_sig.push(sig);
            }
        }
        Ok(())
    };

    for line in &buffered {
        handle(&mut parser, line)?;
    }
    loop {
        buf.clear();
        if reader.read_line(&mut buf)? == 0 {
            break;
        }
        handle(&mut parser, &buf)?;
    }
    // Per-chrom means over autosomes; auto_het/auto_lrr = mean of per-chrom means.
    let mut auto_het_means = Vec::new();
    let mut auto_lrr_means = Vec::new();
    for c in 1..=22u32 {
        if let Some(acc) = chrom_acc.get(&c) {
            if acc.count > 0 {
                out.auto_snps += acc.count;
                auto_het_means.push(acc.het as f64 / acc.count as f64);
                auto_lrr_means.push(if acc.lrr_cnt > 0 {
                    acc.lrr_sum / acc.lrr_cnt as f64
                } else {
                    f64::NAN
                });
            }
        }
    }
    out.auto_het = finite_mean(&auto_het_means);
    out.auto_lrr_mean = finite_mean(&auto_lrr_means);
    out.x_snps = x_acc.count;
    out.x_het = if x_acc.count > 0 {
        x_acc.het as f64 / x_acc.count as f64
    } else {
        f64::NAN
    };
    out.x_lrr_mean = if x_acc.lrr_cnt > 0 {
        x_acc.lrr_sum / x_acc.lrr_cnt as f64
    } else {
        f64::NAN
    };
    Ok(out)
}

fn write_bvhs(path: &Path, sig: &SampleSignals) -> Result<()> {
    let mut w = BufWriter::new(File::create(path).with_context(|| format!("Create {:?}", path))?);
    w.write_all(b"BVHS")?;
    w.write_all(&[2u8])?; // v2: signal arrays + per-sample stats
    write_arrays(&mut w, &sig.auto_ids, &sig.auto_sig)?;
    write_arrays(&mut w, &sig.x_ids, &sig.x_sig)?;
    // stats: auto_snps u64, x_snps u64, then auto_het/x_het/auto_lrr_mean/x_lrr_mean f64
    w.write_all(&sig.auto_snps.to_le_bytes())?;
    w.write_all(&sig.x_snps.to_le_bytes())?;
    for v in [sig.auto_het, sig.x_het, sig.auto_lrr_mean, sig.x_lrr_mean] {
        w.write_all(&v.to_le_bytes())?;
    }
    w.flush()?;
    Ok(())
}

fn write_arrays<W: Write>(w: &mut W, ids: &[u32], sigs: &[i8]) -> Result<()> {
    w.write_all(&(ids.len() as u32).to_le_bytes())?;
    for &id in ids {
        w.write_all(&id.to_le_bytes())?;
    }
    for &s in sigs {
        w.write_all(&[s as u8])?;
    }
    Ok(())
}

/// Discover samples: sorted subdirs, first sorted *.txt in each (matches the flows).
fn discover_samples(data_dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut dirs: Vec<PathBuf> = fs::read_dir(data_dir)
        .with_context(|| format!("Read dir {:?}", data_dir))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    let mut samples = Vec::new();
    for dir in dirs {
        let mut txts: Vec<PathBuf> = fs::read_dir(&dir)
            .with_context(|| format!("Read dir {:?}", dir))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file() && is_genotype_text_file(p))
            .collect();
        txts.sort();
        if let Some(first) = txts.into_iter().next() {
            let sid = dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();
            samples.push((sid, first));
        }
    }
    Ok(samples)
}

fn is_genotype_text_file(path: &Path) -> bool {
    match path.extension().and_then(|x| x.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("txt") => true,
        Some(ext) if ext.eq_ignore_ascii_case("gz") => path
            .file_stem()
            .and_then(|stem| Path::new(stem).extension())
            .and_then(|x| x.to_str())
            .is_some_and(|inner| inner.eq_ignore_ascii_case("txt")),
        _ => false,
    }
}
