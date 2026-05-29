use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;

use crate::genotype_reader::{detect_delimiter, RowOutcome, RowParser};
use crate::ProjectBedArgs;

const LOOKAHEAD_LINES: usize = 2048;

/// Build a gnomAD-loadings-oriented PLINK `.bed/.bim/.fam` for PCA projection,
/// reproducing `gnomad_projection_fast/fast_convert_ddna_to_plink.py --loadings-npz`
/// byte-for-byte, but with a fast parallel Rust parse.
///
/// Unlike `cohort-bed` (cohort minor/major): the panel is the fixed loadings set,
/// A1=alt / A2=ref, samples are matched by variant key (chrom*1e11+pos), and the
/// projection-specific filters apply (gs>=min_gs with NaN->1.0, autosomes 1-22,
/// 2-char ACGT). Low-overlap samples are dropped like fast_convert.
pub fn run_project_bed(args: ProjectBedArgs) -> Result<()> {
    let overall = Instant::now();
    let panel = load_panel(&args.loadings_variants)?;
    eprintln!(
        "📋 project-bed: {} loadings panel variants",
        panel.vars.len()
    );

    let samples = discover_samples(&args.input)?;
    if samples.is_empty() {
        bail!(
            "No sample subdirectories with .txt files found under {:?}",
            args.input
        );
    }
    eprintln!("▶️  project-bed: {} samples", samples.len());

    // Parallel parse — the panel map is read-only and shared, so there is no
    // interner contention (each sample matches against the fixed 76k panel).
    let min_gs = args.min_gs;
    let parse_start = Instant::now();
    let parsed: Vec<SampleHits> = samples
        .par_iter()
        .map(|(_sid, path)| parse_sample(path, &panel, min_gs))
        .collect::<Result<Vec<_>>>()?;
    eprintln!(
        "🧬 project-bed: parsed {} samples in {:.2}s",
        samples.len(),
        parse_start.elapsed().as_secs_f64()
    );

    let n_panel = panel.vars.len();
    let n_samples = samples.len();
    // Panel-major allele matrices (0 = missing), filled at matched positions.
    let mut a1 = vec![0u8; n_panel * n_samples];
    let mut a2 = vec![0u8; n_panel * n_samples];
    let mut matched = vec![0i32; n_samples];
    for (s, hits) in parsed.iter().enumerate() {
        matched[s] = hits.rows.len() as i32;
        for &(idx, c1, c2) in &hits.rows {
            a1[idx as usize * n_samples + s] = c1;
            a2[idx as usize * n_samples + s] = c2;
        }
    }
    drop(parsed);

    // Keep panel variants with valid ACGT ref/alt (fixed_a1>0 & fixed_a2>0).
    let keep_idx: Vec<usize> = (0..n_panel)
        .filter(|&i| panel.vars[i].a1_code > 0 && panel.vars[i].a2_code > 0)
        .collect();
    eprintln!(
        "🧮 project-bed: {} / {} panel SNPs with valid ref/alt",
        keep_idx.len(),
        n_panel
    );

    // Projection overlap filter: drop samples below ceil(expected * ratio).
    let min_observed =
        (args.expected_loadings_overlap as f64 * args.min_loadings_ratio).ceil() as i32;
    let keep_sample: Vec<bool> = matched.iter().map(|&m| m >= min_observed).collect();
    let n_keep = keep_sample.iter().filter(|&&k| k).count();
    eprintln!(
        "🔎 project-bed: overlap filter expected={}, ratio={:.3}, min_observed={}; keeping {}/{} samples",
        args.expected_loadings_overlap, args.min_loadings_ratio, min_observed, n_keep, n_samples
    );
    if n_keep == 0 {
        bail!(
            "no samples passed the projection overlap filter; requires >= {} observed loading positions",
            min_observed
        );
    }
    let kept_samples: Vec<usize> = (0..n_samples).filter(|&s| keep_sample[s]).collect();

    write_outputs(
        &args,
        &panel,
        &keep_idx,
        &a1,
        &a2,
        &samples,
        &kept_samples,
        n_samples,
    )?;
    eprintln!(
        "✅ project-bed: done in {:.2}s",
        overall.elapsed().as_secs_f64()
    );
    println!(
        "✅ Wrote {}.bed/.bim/.fam ({} SNPs x {} samples)",
        args.out_prefix.display(),
        keep_idx.len(),
        n_keep
    );
    Ok(())
}

struct PanelVar {
    rsid: String, // "chrom:pos:ref:alt"
    chrom: String,
    pos: i64,
    a1_code: u8, // alt
    a2_code: u8, // ref
}

struct Panel {
    vars: Vec<PanelVar>,
    key_to_idx: HashMap<i64, u32>,
}

struct SampleHits {
    rows: Vec<(u32, u8, u8)>, // (panel_idx, a1_code, a2_code)
}

/// Load the loadings panel from a TSV (chrom, pos, ref, alt, [locuskey]).
/// Sorted by variant key (chrom*1e11+pos), dedup keeping the first occurrence —
/// matching fast_convert's stable-sort + np.unique(first) on the npz.
fn load_panel(path: &Path) -> Result<Panel> {
    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let reader = BufReader::new(file);
    // (key, original_order, chrom, pos, ref, alt)
    let mut rows: Vec<(i64, usize, String, i64, String, String)> = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        let t = line.trim_end_matches(['\n', '\r']);
        if t.is_empty() {
            continue;
        }
        let f: Vec<&str> = t.split('\t').collect();
        if f.len() < 4 {
            continue;
        }
        // skip a header line if present
        let pos: i64 = match f[1].trim().parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let chrom = f[0].trim().to_string();
        let chrom_int: i64 = match chrom.parse() {
            Ok(c) => c,
            Err(_) => continue, // loadings are autosomal ints
        };
        let key = chrom_int * 100_000_000_000 + pos;
        rows.push((
            key,
            i,
            chrom,
            pos,
            f[2].trim().to_string(),
            f[3].trim().to_string(),
        ));
    }
    // stable sort by key, then dedup keeping the first original-order row per key.
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut vars = Vec::new();
    let mut key_to_idx = HashMap::new();
    let mut last_key: Option<i64> = None;
    for (key, _ord, chrom, pos, r, a) in rows {
        if last_key == Some(key) {
            continue; // dedup: keep first
        }
        last_key = Some(key);
        let a1_code = base_code(&a); // alt
        let a2_code = base_code(&r); // ref
        key_to_idx.insert(key, vars.len() as u32);
        vars.push(PanelVar {
            rsid: format!("{chrom}:{pos}:{r}:{a}"),
            chrom,
            pos,
            a1_code,
            a2_code,
        });
    }
    Ok(Panel { vars, key_to_idx })
}

/// Parse one sample: apply fast_convert's projection filters and match to the
/// panel by variant key. Dedup repeated keys keep-first. Returns matched rows.
fn parse_sample(path: &Path, panel: &Panel, min_gs: f32) -> Result<SampleHits> {
    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let mut reader = BufReader::new(file);
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
        return Ok(SampleHits { rows: Vec::new() });
    }
    let delim = detect_delimiter(&buffered);
    let mut parser = RowParser::new(delim);
    let mut seen_keys: HashMap<i64, ()> = HashMap::new();
    let mut rows: Vec<(u32, u8, u8)> = Vec::new();

    let mut handle = |parser: &mut RowParser, line: &str| -> Result<()> {
        if let RowOutcome::Parsed(row) = parser.consume_line(line)? {
            // autosome 1-22 (chrom already cleaned by RowParser)
            let chrom_int: i64 = match row.chrom.parse() {
                Ok(c) if (1..=22).contains(&c) => c,
                _ => return Ok(()),
            };
            if row.pos <= 0 {
                return Ok(());
            }
            // gs >= min_gs; missing/unparseable gs -> 1.0 (matches fillna(1.0))
            let gs: f32 = row
                .gs
                .as_deref()
                .and_then(|s| s.trim().parse::<f32>().ok())
                .unwrap_or(1.0);
            if !matches!(
                gs.partial_cmp(&min_gs),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ) {
                return Ok(());
            }
            // genotype must be 2-char ACGT
            let gt = match &row.genotype {
                Some(g) => g.to_ascii_uppercase(),
                None => return Ok(()),
            };
            let (c1, c2) = match acgt_pair(&gt) {
                Some(p) => p,
                None => return Ok(()),
            };
            let key = chrom_int * 100_000_000_000 + row.pos;
            let Some(&idx) = panel.key_to_idx.get(&key) else {
                return Ok(());
            };
            if seen_keys.insert(key, ()).is_some() {
                return Ok(()); // dedup keep-first
            }
            rows.push((idx, c1, c2));
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
    Ok(SampleHits { rows })
}

/// 2-char genotype -> (a1,a2) codes, only if both chars are ACGT; else None.
fn acgt_pair(gt: &str) -> Option<(u8, u8)> {
    let b = gt.as_bytes();
    if b.len() < 2 {
        return None;
    }
    let c = |x: u8| match x {
        b'A' => 1,
        b'C' => 2,
        b'G' => 3,
        b'T' => 4,
        _ => 0,
    };
    let (a, d) = (c(b[0]), c(b[1]));
    if a == 0 || d == 0 {
        None
    } else {
        Some((a, d))
    }
}

fn base_code(s: &str) -> u8 {
    match s.as_bytes().first() {
        Some(b'A') => 1,
        Some(b'C') => 2,
        Some(b'G') => 3,
        Some(b'T') => 4,
        _ => 0,
    }
}

fn base_label(code: u8) -> char {
    match code {
        1 => 'A',
        2 => 'C',
        3 => 'G',
        4 => 'T',
        _ => '.',
    }
}

#[allow(clippy::too_many_arguments)]
fn write_outputs(
    args: &ProjectBedArgs,
    panel: &Panel,
    keep_idx: &[usize],
    a1: &[u8],
    a2: &[u8],
    samples: &[(String, PathBuf)],
    kept_samples: &[usize],
    n_samples: usize,
) -> Result<()> {
    let out_prefix = &args.out_prefix;
    if let Some(parent) = out_prefix.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("Create {:?}", parent))?;
        }
    }
    let p = |ext: &str| {
        let mut s = out_prefix.as_os_str().to_owned();
        s.push(ext);
        PathBuf::from(s)
    };

    // .fam — note FID is "0" (matches fast_convert), IID = sample id.
    let mut fam = BufWriter::new(File::create(p(".fam"))?);
    for &s in kept_samples {
        writeln!(fam, "0\t{}\t0\t0\t0\t-9", samples[s].0)?;
    }
    fam.flush()?;

    // .bim — variant id = "chrom:pos:ref:alt", A1=alt label, A2=ref label.
    let mut bim = BufWriter::new(File::create(p(".bim"))?);
    let mut bim_buf = String::new();
    for &i in keep_idx {
        let v = &panel.vars[i];
        bim_buf.push_str(&format!(
            "{}\t{}\t0\t{}\t{}\t{}\n",
            v.chrom,
            v.rsid,
            v.pos,
            base_label(v.a1_code),
            base_label(v.a2_code)
        ));
    }
    bim.write_all(bim_buf.as_bytes())?;
    bim.flush()?;

    // .bed — 2-bit, kept variants x kept samples; dosage = count of A1(alt).
    let mut bed = BufWriter::new(File::create(p(".bed"))?);
    bed.write_all(&[0x6c, 0x1b, 0x01])?;
    let nk = kept_samples.len();
    let bytes_per_var = nk.div_ceil(4);
    let mut codes = vec![0u8; bytes_per_var * 4];
    for &i in keep_idx {
        let v = &panel.vars[i];
        let base = i * n_samples;
        for (j, &s) in kept_samples.iter().enumerate() {
            let (x, y) = (a1[base + s], a2[base + s]);
            let missing = x == 0 || y == 0;
            let in_set = (x == v.a1_code || x == v.a2_code) && (y == v.a1_code || y == v.a2_code);
            let two_bit = if missing || !in_set {
                0b01
            } else {
                let n_a1 = (x == v.a1_code) as u8 + (y == v.a1_code) as u8;
                match n_a1 {
                    2 => 0b00,
                    1 => 0b10,
                    _ => 0b11,
                }
            };
            codes[j] = two_bit;
        }
        for slot in codes.iter_mut().take(bytes_per_var * 4).skip(nk) {
            *slot = 0;
        }
        for chunk in codes.chunks(4) {
            bed.write_all(&[chunk[0] | (chunk[1] << 2) | (chunk[2] << 4) | (chunk[3] << 6)])?;
        }
    }
    bed.flush()?;
    Ok(())
}

/// Discover samples like fast_convert/fast_pipeline: sorted subdirs, first *.txt.
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
            .filter(|p| p.is_file() && p.extension().and_then(|x| x.to_str()) == Some("txt"))
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
