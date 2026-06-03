use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use rayon::prelude::*;

use crate::genotype_reader::{detect_delimiter, open_text_reader, RowOutcome, RowParser};
use crate::CohortBedArgs;

const LOOKAHEAD_LINES: usize = 2048;

/// Build a cohort PLINK `.bed/.bim/.fam` from DDNA/Illumina genotype files,
/// reproducing `pca_qc_fast/scripts/fast_pipeline.py`'s parse -> cohort SNP
/// universe -> biallelic choice -> PLINK writer byte-for-byte, but fast (parallel
/// Rust parse) and bounded-RAM (rsids interned, no per-sample string blow-up).
///
/// NOTE: unlike emit-long/fast-allele-freq this does NOT resolve against the
/// reference DB. It uses the genotype files' own rsid/chrom/pos and a cohort-union
/// SNP universe with cohort minor/major allele coding, exactly like fast_pipeline.
pub fn run_cohort_bed(args: CohortBedArgs) -> Result<()> {
    let overall = Instant::now();
    let samples = discover_samples(&args.input)?;
    if samples.is_empty() {
        bail!(
            "No sample subdirectories with .txt files found under {:?}",
            args.input
        );
    }
    eprintln!("▶️  cohort-bed: {} samples", samples.len());

    // Phase 1: parallel parse. Each sample maps its rsids to global interned ids
    // via a shared interner, returning compact (id, a1, a2) rows. Interning keeps
    // memory ~ (total rows x 6 bytes) + one copy of each distinct variant, instead
    // of holding every rsid string per sample.
    let interner = Interner::new();
    let parse_start = Instant::now();
    // map_init gives each rayon worker a reusable per-thread cache (rsid -> global
    // id). After a worker sees the panel once, subsequent samples resolve rsids
    // from this local map with zero shared-map access — so the ~1e9 lookups stop
    // contending on the DashMap shards (which all samples hit identically).
    // A bad file must not abort the cohort: log it and treat it as an empty
    // sample (all-missing column) so the rest of the pipeline keeps running.
    let per_sample: Vec<Vec<(u32, u8, u8)>> = samples
        .par_iter()
        .map_init(HashMap::<String, u32>::new, |local, (sid, path)| {
            parse_sample(path, &interner, local).unwrap_or_else(|e| {
                eprintln!(
                    "⚠️  cohort-bed: skipping unreadable sample {sid} ({}): {e:#}",
                    path.display()
                );
                Vec::new()
            })
        })
        .collect::<Vec<_>>();
    eprintln!(
        "🧬 cohort-bed: parsed {} samples in {:.2}s",
        samples.len(),
        parse_start.elapsed().as_secs_f64()
    );

    let meta = interner.into_meta(); // id -> (rsid, chrom, pos)

    // Phase 2: cohort SNP universe = first-seen rsid order across samples in
    // sample order (matches fast_pipeline's pass 1).
    let n_samples = samples.len();
    let mut final_idx: Vec<i32> = vec![-1; meta.len()];
    let mut universe: Vec<u32> = Vec::new(); // interned id in universe order
    for sample in &per_sample {
        for &(id, _, _) in sample {
            if final_idx[id as usize] < 0 {
                final_idx[id as usize] = universe.len() as i32;
                universe.push(id);
            }
        }
    }
    let n_snps = universe.len();
    eprintln!(
        "🗺️  cohort-bed: {} SNP universe x {} samples",
        n_snps, n_samples
    );

    // Optional snp_info.tsv = full cohort universe in first-seen order, matching
    // fast_pipeline.build_snp_universe's output (header rsid/chromosome/position).
    if let Some(snp_info_path) = &args.snp_info {
        let mut w = BufWriter::new(
            File::create(snp_info_path).with_context(|| format!("Create {:?}", snp_info_path))?,
        );
        writeln!(w, "rsid\tchromosome\tposition")?;
        for &id in &universe {
            let (rsid, chrom, pos) = &meta[id as usize];
            writeln!(w, "{rsid}\t{chrom}\t{pos}")?;
        }
        w.flush()?;
    }

    // SNP-major allele matrices (contiguous per SNP for counting), 0 = missing.
    let mut a1 = vec![0u8; n_snps * n_samples];
    let mut a2 = vec![0u8; n_snps * n_samples];
    for (s, sample) in per_sample.iter().enumerate() {
        for &(id, c1, c2) in sample {
            let snp = final_idx[id as usize] as usize;
            a1[snp * n_samples + s] = c1;
            a2[snp * n_samples + s] = c2;
        }
    }
    drop(per_sample);

    // Choose biallelic/mono variants + cohort minor/major coding.
    let mut keep: Vec<KeptVariant> = Vec::new();
    for snp in 0..n_snps {
        let base = snp * n_samples;
        let mut counts = [0i64; 4];
        for s in 0..n_samples {
            let (x, y) = (a1[base + s], a2[base + s]);
            if x != 0 && y != 0 {
                counts[(x - 1) as usize] += 1;
                counts[(y - 1) as usize] += 1;
            }
        }
        let n_distinct = counts.iter().filter(|&&c| c > 0).count();
        if n_distinct == 0 || n_distinct > 2 {
            continue;
        }
        // Sort base codes 1..4 by count desc, ties by base order (stable) -> A<C<G<T.
        let mut order = [0usize, 1, 2, 3];
        order.sort_by(|&i, &j| counts[j].cmp(&counts[i]).then(i.cmp(&j)));
        let major = (order[0] + 1) as u8;
        let mut minor = (order[1] + 1) as u8;
        if counts[order[1]] == 0 {
            minor = major; // monomorphic
        }
        keep.push(KeptVariant { snp, major, minor });
    }
    eprintln!(
        "🧮 cohort-bed: {} biallelic/mono variants kept ({} dropped)",
        keep.len(),
        n_snps - keep.len()
    );

    write_outputs(
        &args.out_prefix,
        &samples,
        &meta,
        &universe,
        &a1,
        &a2,
        &keep,
        n_samples,
    )?;
    eprintln!(
        "✅ cohort-bed: done in {:.2}s",
        overall.elapsed().as_secs_f64()
    );
    println!(
        "✅ Wrote {}.bed/.bim/.fam ({} variants x {} samples)",
        args.out_prefix.display(),
        keep.len(),
        n_samples
    );
    Ok(())
}

struct KeptVariant {
    snp: usize,
    major: u8,
    minor: u8,
}

#[allow(clippy::too_many_arguments)]
fn write_outputs(
    out_prefix: &Path,
    samples: &[(String, PathBuf)],
    meta: &[(String, String, i64)],
    universe: &[u32],
    a1: &[u8],
    a2: &[u8],
    keep: &[KeptVariant],
    n_samples: usize,
) -> Result<()> {
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

    // .fam
    let mut fam = BufWriter::new(File::create(p(".fam"))?);
    for (sid, _) in samples {
        writeln!(fam, "{sid}\t{sid}\t0\t0\t0\t-9")?;
    }
    fam.flush()?;

    // .bim
    let mut bim = BufWriter::new(File::create(p(".bim"))?);
    for kv in keep {
        let (rsid, chrom, pos) = &meta[universe[kv.snp] as usize];
        writeln!(
            bim,
            "{}\t{}\t0\t{}\t{}\t{}",
            chrom_code(chrom),
            rsid,
            pos,
            base_label(kv.minor),
            base_label(kv.major)
        )?;
    }
    bim.flush()?;

    // .bed (2-bit, 4 samples/byte, magic 6c 1b 01)
    let mut bed = BufWriter::new(File::create(p(".bed"))?);
    bed.write_all(&[0x6c, 0x1b, 0x01])?;
    let bytes_per_var = n_samples.div_ceil(4);
    let mut codes = vec![0u8; bytes_per_var * 4];
    for kv in keep {
        let base = kv.snp * n_samples;
        for s in 0..n_samples {
            let (x, y) = (a1[base + s], a2[base + s]);
            let called = x != 0 && y != 0;
            let in_set = (x == kv.major || x == kv.minor) && (y == kv.major || y == kv.minor);
            // dosage = count of MINOR allele
            let two_bit = if !called || !in_set {
                0b01 // missing
            } else {
                let d = (x == kv.minor) as u8 + (y == kv.minor) as u8;
                match d {
                    2 => 0b00,
                    1 => 0b10,
                    _ => 0b11,
                }
            };
            codes[s] = two_bit;
        }
        for code in codes.iter_mut().skip(n_samples) {
            *code = 0; // pad slots
        }
        for chunk in codes.chunks(4) {
            let byte = chunk[0] | (chunk[1] << 2) | (chunk[2] << 4) | (chunk[3] << 6);
            bed.write_all(&[byte])?;
        }
    }
    bed.flush()?;
    Ok(())
}

/// Map a chromosome label to PLINK's numeric code, matching fast_pipeline's
/// `_chrom_code` (ordered replacements: XY->25, MT->26, M->26, X->23, Y->24).
fn chrom_code(value: &str) -> String {
    let mut c = value.replace("chr", "").replace("CHR", "");
    c = c.replace("XY", "25");
    c = c.replace("MT", "26");
    c = c.replace('M', "26");
    c = c.replace('X', "23");
    c = c.replace('Y', "24");
    c
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

/// Concurrent rsid interner: rsid string -> stable id, plus id -> (rsid, chrom, pos)
/// captured at first insertion (so .bim recovers the original strings).
type SnpMeta = (String, String, i64);

/// Sharded `DashMap` so the parallel parse doesn't serialize on one lock. The
/// previous global `Mutex` was taken once per row (~1e9 times at 1400x692k),
/// which collapsed parallelism (~23 min at 1400 files). Sharded locks spread the
/// ~1e9 lookups (almost all hits — every sample shares the same rsids); only the
/// ~1e6 distinct rsids ever insert.
struct Interner {
    map: DashMap<String, u32>,
    meta: DashMap<u32, SnpMeta>,
    next: AtomicU32,
}

impl Interner {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
            meta: DashMap::new(),
            next: AtomicU32::new(0),
        }
    }

    fn intern(&self, rsid: &str, chrom: &str, pos: i64) -> u32 {
        // Fast path: already-seen rsid -> sharded read, no exclusive lock.
        if let Some(id) = self.map.get(rsid) {
            return *id;
        }
        // First sighting: insert under the shard's entry lock. or_insert_with only
        // runs (and bumps the id counter) if still vacant, so ids stay unique even
        // if two threads race on the same new rsid.
        *self.map.entry(rsid.to_string()).or_insert_with(|| {
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            self.meta
                .insert(id, (rsid.to_string(), chrom.to_string(), pos));
            id
        })
    }

    fn into_meta(self) -> Vec<SnpMeta> {
        let n = self.next.load(Ordering::Relaxed) as usize;
        let mut out: Vec<SnpMeta> = vec![(String::new(), String::new(), 0); n];
        for (id, m) in self.meta.into_iter() {
            out[id as usize] = m;
        }
        out
    }
}

/// Parse one sample file: dedup rsid keep-first, return (interned_id, a1, a2).
/// Matches `read_sample`: keep no-call rows (they define the SNP), code A/C/G/T=1..4.
fn parse_sample(
    path: &Path,
    interner: &Interner,
    local: &mut HashMap<String, u32>,
) -> Result<Vec<(u32, u8, u8)>> {
    use std::io::BufRead;
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
        return Ok(Vec::new());
    }
    let delim = detect_delimiter(&buffered);
    let mut parser = RowParser::new(delim);
    let mut seen: HashMap<String, ()> = HashMap::new();
    let mut out: Vec<(u32, u8, u8)> = Vec::new();

    let mut handle = |parser: &mut RowParser, line: &str| -> Result<()> {
        // A bad row must not break the file: skip it and keep going.
        let outcome = match parser.consume_line(line) {
            Ok(o) => o,
            Err(_) => return Ok(()),
        };
        if let RowOutcome::Parsed(row) = outcome {
            // variant_id = rsid, else chrom:pos (matches read_pipeline_genotypes)
            let rsid = if row.rsid.is_empty() {
                format!("{}:{}", row.chrom, row.pos)
            } else {
                row.rsid.clone()
            };
            let gt = match &row.genotype {
                Some(g) => g.to_ascii_uppercase(),
                None => return Ok(()), // dropna genotype
            };
            if seen.insert(rsid.clone(), ()).is_some() {
                return Ok(()); // drop_duplicates keep first
            }
            let (c1, c2) = allele_codes(&gt);
            // Per-thread cache first; only fall to the shared interner on a miss.
            let id = match local.get(&rsid) {
                Some(&id) => id,
                None => {
                    let id = interner.intern(&rsid, &row.chrom, row.pos);
                    local.insert(rsid.clone(), id);
                    id
                }
            };
            out.push((id, c1, c2));
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
    Ok(out)
}

/// First two chars of the genotype -> allele codes (A=1,C=2,G=3,T=4, else 0).
fn allele_codes(gt: &str) -> (u8, u8) {
    let bytes = gt.as_bytes();
    let code = |b: Option<&u8>| match b {
        Some(b'A') => 1,
        Some(b'C') => 2,
        Some(b'G') => 3,
        Some(b'T') => 4,
        _ => 0,
    };
    (code(bytes.first()), code(bytes.get(1)))
}

/// Discover samples like fast_pipeline: sorted subdirs, first sorted *.txt in each.
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
