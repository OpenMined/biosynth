use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use crate::commands::long_emit::{
    collect_merged_long_rows, default_participant, summarize_warning_counts, MissingRefLogger,
    ReferenceResolver, SharedReference,
};
use crate::download::ensure_reference_db;
use crate::long_rows::LongRow;
use crate::plink::{
    clean_plink_id, plink_paths, read_bim, read_fam, variant_locus_key, PlinkScanStats,
};
use crate::stats::StatsStore;
use crate::FastAlleleFreqArgs;

/// Per-locus accumulator. Mirrors the running counters in
/// `long_aggregate::merge_chunks` exactly so the emitted TSV is byte-for-byte
/// identical to `emit-long -> aggregate-long --threads 1`.
struct Accum {
    rsid: String,
    /// Rank (participant sort order) of the participant whose non-empty rsid is
    /// currently kept. `u32::MAX` until a non-empty rsid is seen. The smallest
    /// rank wins, reproducing aggregate's "first non-empty rsid by participant".
    rsid_rank: u32,
    allele_count: i64,
    n_obs: i64,
    num_homo: i64,
    num_hetero: i64,
}

type LociMap = BTreeMap<String, Accum>;
type Counts = BTreeMap<String, u64>;

pub fn run_fast_allele_freq(args: FastAlleleFreqArgs) -> Result<()> {
    let overall_start = Instant::now();
    let has_genotype_inputs = !args.inputs.is_empty();
    let has_plink_inputs = !args.plink_prefixes.is_empty();
    let has_plink2_inputs = args.plink2_dir.is_some();
    let input_modes = [has_genotype_inputs, has_plink_inputs, has_plink2_inputs]
        .iter()
        .filter(|&&enabled| enabled)
        .count();
    if input_modes != 1 {
        bail!("Provide exactly one of --input genotype files/directories, --plink-prefix, or --plink2-dir");
    }

    if has_plink_inputs {
        let (loci, total_stats) = run_plink_prefixes(&args.plink_prefixes)?;
        write_allele_freq(&args.allele_freq_tsv, &loci)?;
        eprintln!(
            "✅ fast-allele-freq (plink): {} prefixes, {} loci, {} rows, {} missing calls, {:.2}s",
            args.plink_prefixes.len(),
            loci.len(),
            total_stats.rows_emitted,
            total_stats.missing_calls,
            overall_start.elapsed().as_secs_f64()
        );
        if total_stats.skipped_non_snp > 0 || total_stats.skipped_bad_position > 0 {
            eprintln!(
                "⚠️  fast-allele-freq (plink): skipped non_snp={}, bad_position={}",
                total_stats.skipped_non_snp, total_stats.skipped_bad_position
            );
        }
        println!(
            "✅ Wrote allele frequencies to {}",
            args.allele_freq_tsv.display()
        );
        return Ok(());
    }

    if has_plink2_inputs {
        let suffix = args.plink2_info_suffix.trim();
        let loci_filter = match args.loci_filter.as_ref() {
            Some(path) => Some(load_loci_filter(path)?),
            None => None,
        };
        let stats = write_plink2_info_allele_freq(
            args.plink2_dir.as_ref().expect("plink2 dir"),
            suffix,
            args.plink2_include_non_snv,
            loci_filter.as_ref(),
            &args.allele_freq_tsv,
        )?;
        eprintln!(
            "✅ fast-allele-freq (plink2-info): {} pvar files, {} loci, {} variants scanned, {} skipped, {:.2}s",
            stats.files,
            stats.alleles_emitted,
            stats.variants_seen,
            stats.skipped,
            overall_start.elapsed().as_secs_f64()
        );
        println!(
            "✅ Wrote allele frequencies to {}",
            args.allele_freq_tsv.display()
        );
        return Ok(());
    }

    let sqlite_path = ensure_reference_db(Some(&args.sqlite), args.force_download)?;
    let store = StatsStore::connect_read_only(&sqlite_path)?;
    // Preload the whole reference into memory once. Lets worker threads resolve
    // rsid/position with lock-free reads instead of contending on SQLite.
    let preload_start = Instant::now();
    let shared = Arc::new(SharedReference::load(&store)?);
    eprintln!(
        "📚 fast-allele-freq: reference preloaded in {:.2}s",
        preload_start.elapsed().as_secs_f64()
    );

    let files = collect_input_files(&args.inputs)?;
    if files.is_empty() {
        bail!("No input genotype files found under the provided --input paths");
    }
    // Sort by participant id. aggregate-long --threads 1 merges rows sorted by
    // (locus_key, participant_id), so the rsid kept per locus is from the
    // alphabetically-first participant with a non-empty rsid. The rank we assign
    // here = that participant order, and the rank-min rule in `fold_row`
    // reproduces the choice regardless of parallel parse order.
    let mut tasks: Vec<(String, PathBuf)> = files
        .into_iter()
        .map(|path| (default_participant(&path), path))
        .collect();
    tasks.sort_by(|a, b| a.0.cmp(&b.0));
    let tasks: Vec<(u32, String, PathBuf)> = tasks
        .into_iter()
        .enumerate()
        .map(|(rank, (pid, path))| (rank as u32, pid, path))
        .collect();

    // Per-row TSV log can't be written safely from many threads -> single thread.
    let force_single = args.missing_ref_log.is_some();
    let requested = if force_single {
        1
    } else {
        resolve_threads(args.threads)
    };
    // Peak RAM ~= threads x one full-panel accumulator. Cap threads so it fits the
    // budget; each worker's map fills toward the whole panel regardless of file count.
    let threads = cap_threads_for_ram(requested, args.max_ram_gb, shared.reference_count());
    if threads < requested {
        eprintln!(
            "🧠 fast-allele-freq: capping threads {requested} -> {threads} to stay under {:.0} GB",
            args.max_ram_gb
        );
    }
    eprintln!(
        "▶️  fast-allele-freq: {} input file(s), threads={}",
        tasks.len(),
        threads
    );

    let (loci, counts) = if threads <= 1 {
        run_sequential(&tasks, &shared, &args)?
    } else {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .context("build fast-allele-freq thread pool")?;
        pool.install(|| run_parallel(&tasks, &shared, threads))?
    };

    write_allele_freq(&args.allele_freq_tsv, &loci)?;

    if args.warn_detail != crate::WarnDetail::None {
        let summary = summarize_warning_counts(&counts);
        if !summary.is_empty() {
            eprintln!("⚠️  fast-allele-freq warnings: {summary}");
        }
    }
    eprintln!(
        "✅ fast-allele-freq: {} files, {} loci, {:.2}s",
        tasks.len(),
        loci.len(),
        overall_start.elapsed().as_secs_f64()
    );
    println!(
        "✅ Wrote allele frequencies to {}",
        args.allele_freq_tsv.display()
    );
    Ok(())
}

#[derive(Default)]
struct PvarInfoStats {
    files: u64,
    variants_seen: u64,
    alleles_emitted: u64,
    skipped: u64,
}

fn write_plink2_info_allele_freq(
    data_dir: &Path,
    suffix: &str,
    include_non_snv: bool,
    loci_filter: Option<&HashSet<String>>,
    output_path: &Path,
) -> Result<PvarInfoStats> {
    let pvars = discover_pvar_files(data_dir)?;
    if pvars.is_empty() {
        bail!("No .pvar or .pvar.zst files found in {:?}", data_dir);
    }

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("Create directory {:?}", parent))?;
        }
    }
    let mut writer = BufWriter::new(
        File::create(output_path).with_context(|| format!("Create {:?}", output_path))?,
    );
    writeln!(
        writer,
        "locus_key\tallele_count\tallele_number\tnum_homo\tnum_hetero\tallele_freq\trsid"
    )?;

    let mut stats = PvarInfoStats::default();
    for path in pvars {
        eprintln!("▶️  fast-allele-freq (plink2-info): {}", path.display());
        fold_pvar_info_file(
            &path,
            suffix,
            include_non_snv,
            loci_filter,
            &mut writer,
            &mut stats,
        )?;
        stats.files += 1;
    }
    writer.flush()?;
    Ok(stats)
}

fn discover_pvar_files(data_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(data_dir).with_context(|| format!("Read directory {:?}", data_dir))? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.ends_with(".pvar") || name.ends_with(".pvar.zst") {
            files.push(path);
        }
    }
    files.sort_by(|a, b| pvar_sort_key(a).cmp(&pvar_sort_key(b)));
    Ok(files)
}

fn pvar_sort_key(path: &Path) -> (u8, String) {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let rank = if let Some(rest) = name.strip_prefix("chr") {
        let chrom = rest.split('_').next().unwrap_or(rest);
        match chrom {
            "X" => 23,
            "Y" => 24,
            "M" | "MT" => 25,
            _ => chrom.parse::<u8>().unwrap_or(26),
        }
    } else {
        26
    };
    (rank, name.to_string())
}

fn fold_pvar_info_file(
    path: &Path,
    suffix: &str,
    include_non_snv: bool,
    loci_filter: Option<&HashSet<String>>,
    writer: &mut BufWriter<File>,
    stats: &mut PvarInfoStats,
) -> Result<()> {
    if is_zst(path) {
        let mut child = Command::new("zstd")
            .arg("-dcf")
            .arg(path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("Spawn zstd for {:?}", path))?;
        let stdout = child
            .stdout
            .take()
            .context("zstd stdout was not captured")?;
        let result = {
            let reader = BufReader::new(stdout);
            fold_pvar_info_reader(
                reader,
                path,
                suffix,
                include_non_snv,
                loci_filter,
                writer,
                stats,
            )
        };
        let output = child
            .wait_with_output()
            .with_context(|| format!("Wait for zstd {:?}", path))?;
        if !output.status.success() {
            bail!(
                "zstd failed for {:?}: {}",
                path,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        result
    } else {
        let reader = BufReader::new(File::open(path).with_context(|| format!("Open {:?}", path))?);
        fold_pvar_info_reader(
            reader,
            path,
            suffix,
            include_non_snv,
            loci_filter,
            writer,
            stats,
        )
    }
}

fn fold_pvar_info_reader<R: BufRead>(
    reader: R,
    path: &Path,
    suffix: &str,
    include_non_snv: bool,
    loci_filter: Option<&HashSet<String>>,
    writer: &mut BufWriter<File>,
    stats: &mut PvarInfoStats,
) -> Result<()> {
    let mut header: Option<Vec<String>> = None;
    for raw in reader.lines() {
        let line = raw?;
        if line.starts_with("##") || line.is_empty() {
            continue;
        }
        if line.starts_with('#') {
            header = Some(
                line.trim_start_matches('#')
                    .split('\t')
                    .map(|s| s.to_string())
                    .collect(),
            );
            continue;
        }
        let header = header
            .as_ref()
            .with_context(|| format!("No #CHROM header found before data rows in {:?}", path))?;
        let fields: Vec<&str> = line.split('\t').collect();
        let row = PvarRow::from_fields(header, &fields)?;
        stats.variants_seen += 1;
        let emitted = write_pvar_row_af(&row, suffix, include_non_snv, loci_filter, writer)?;
        if emitted == 0 {
            stats.skipped += 1;
        } else {
            stats.alleles_emitted += emitted as u64;
        }
    }
    Ok(())
}

struct PvarRow<'a> {
    chrom: &'a str,
    pos: i64,
    id: &'a str,
    reference: &'a str,
    alternates: Vec<&'a str>,
    info: BTreeMap<&'a str, &'a str>,
}

impl<'a> PvarRow<'a> {
    fn from_fields(header: &[String], fields: &[&'a str]) -> Result<Self> {
        let get = |name: &str| -> Option<&'a str> {
            header
                .iter()
                .position(|h| h == name)
                .and_then(|idx| fields.get(idx).copied())
        };
        let chrom = get("CHROM").context("pvar row missing CHROM")?;
        let pos = get("POS")
            .context("pvar row missing POS")?
            .parse::<i64>()
            .context("invalid pvar POS")?;
        let id = get("ID").unwrap_or("");
        let reference = get("REF").context("pvar row missing REF")?;
        let alternates = get("ALT")
            .context("pvar row missing ALT")?
            .split(',')
            .filter(|alt| !alt.is_empty() && *alt != ".")
            .collect();
        let info = parse_info_map(get("INFO").unwrap_or("."));
        Ok(Self {
            chrom,
            pos,
            id,
            reference,
            alternates,
            info,
        })
    }
}

fn write_pvar_row_af(
    row: &PvarRow<'_>,
    suffix: &str,
    include_non_snv: bool,
    loci_filter: Option<&HashSet<String>>,
    writer: &mut BufWriter<File>,
) -> Result<usize> {
    let ac_key = info_key("AC", suffix);
    let an_key = info_key("AN", suffix);
    let het_key = info_key("AC_Het", suffix);
    let hom_key = info_key("AC_Hom", suffix);
    let Some(an) = info_i64(&row.info, &an_key, 0) else {
        return Ok(0);
    };

    let mut emitted = 0usize;
    for (idx, alt) in row.alternates.iter().enumerate() {
        if !include_non_snv && !is_snv(row.reference, alt) {
            continue;
        }
        let Some(ac) = info_i64(&row.info, &ac_key, idx) else {
            continue;
        };
        let num_hetero = info_i64(&row.info, &het_key, idx).unwrap_or(0);
        let hom_alt_alleles = info_i64(&row.info, &hom_key, idx).unwrap_or(0);
        let locus = crate::long_rows::locus_key(row.chrom, row.pos, row.reference, alt);
        if loci_filter.is_some_and(|filter| !filter.contains(&locus)) {
            continue;
        }
        let allele_number = an;
        let allele_freq = if allele_number > 0 {
            ac as f64 / allele_number as f64
        } else {
            0.0
        };
        writeln!(
            writer,
            "{locus}\t{ac}\t{allele_number}\t{}\t{num_hetero}\t{allele_freq:.6}\t{}",
            hom_alt_alleles / 2,
            clean_variant_id(row.id)
        )?;
        emitted += 1;
    }
    Ok(emitted)
}

fn parse_info_map(info: &str) -> BTreeMap<&str, &str> {
    let mut map = BTreeMap::new();
    if info == "." {
        return map;
    }
    for item in info.split(';') {
        if let Some((key, value)) = item.split_once('=') {
            map.insert(key, value);
        }
    }
    map
}

fn info_key(base: &str, suffix: &str) -> String {
    if suffix.is_empty() {
        base.to_string()
    } else {
        format!("{base}_{suffix}")
    }
}

fn info_i64(info: &BTreeMap<&str, &str>, key: &str, alt_idx: usize) -> Option<i64> {
    let value = *info.get(key)?;
    let piece = value
        .split(',')
        .nth(alt_idx)
        .or_else(|| value.split(',').next())?;
    if piece.is_empty() || piece == "." {
        return None;
    }
    piece.parse::<i64>().ok()
}

fn clean_variant_id(id: &str) -> String {
    if id == "." {
        String::new()
    } else {
        id.to_string()
    }
}

fn is_snv(reference: &str, alternate: &str) -> bool {
    reference.len() == 1
        && alternate.len() == 1
        && matches!(reference, "A" | "C" | "G" | "T")
        && matches!(alternate, "A" | "C" | "G" | "T")
}

fn is_zst(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some("zst")
}

fn load_loci_filter(path: &Path) -> Result<HashSet<String>> {
    let file = File::open(path).with_context(|| format!("Open loci filter {:?}", path))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let header = loop {
        match lines.next() {
            Some(line) => {
                let line = line?;
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                break line;
            }
            None => bail!("Loci filter {:?} is empty", path),
        }
    };
    let columns: Vec<String> = header.split('\t').map(normalize_filter_column).collect();
    let locus_idx = columns.iter().position(|c| c == "locus_key");
    let chrom_idx = find_column(&columns, &["chrom", "chromosome", "#chrom"]);
    let pos_idx = find_column(&columns, &["pos", "position"]);
    let ref_idx = find_column(&columns, &["ref", "reference"]);
    let alt_idx = find_column(&columns, &["alt", "alternate", "alternates"]);
    if locus_idx.is_none()
        && !(chrom_idx.is_some() && pos_idx.is_some() && ref_idx.is_some() && alt_idx.is_some())
    {
        bail!(
            "Loci filter {:?} must have a locus_key column or chrom/pos/ref/alt columns",
            path
        );
    }

    let mut filter = HashSet::new();
    for line in lines {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        if let Some(idx) = locus_idx {
            if let Some(key) = fields.get(idx).map(|v| v.trim()).filter(|v| !v.is_empty()) {
                filter.insert(key.to_string());
            }
        } else {
            let chrom = fields
                .get(chrom_idx.expect("chrom idx"))
                .map(|v| v.trim())
                .unwrap_or("");
            let pos = fields
                .get(pos_idx.expect("pos idx"))
                .map(|v| v.trim())
                .unwrap_or("");
            let reference = fields
                .get(ref_idx.expect("ref idx"))
                .map(|v| v.trim())
                .unwrap_or("");
            let alternate = fields
                .get(alt_idx.expect("alt idx"))
                .map(|v| v.trim())
                .unwrap_or("");
            if !chrom.is_empty()
                && !pos.is_empty()
                && !reference.is_empty()
                && !alternate.is_empty()
            {
                filter.insert(format!("{chrom}-{pos}-{reference}-{alternate}"));
            }
        }
    }
    if filter.is_empty() {
        bail!("Loci filter {:?} did not contain any loci", path);
    }
    eprintln!(
        "🔎 loci-filter: loaded {} loci from {}",
        filter.len(),
        path.display()
    );
    Ok(filter)
}

fn find_column(columns: &[String], aliases: &[&str]) -> Option<usize> {
    columns
        .iter()
        .position(|column| aliases.iter().any(|alias| column == alias))
}

fn normalize_filter_column(value: &str) -> String {
    value
        .trim()
        .trim_start_matches('\u{feff}')
        .to_ascii_lowercase()
}

fn run_plink_prefixes(prefixes: &[PathBuf]) -> Result<(LociMap, PlinkScanStats)> {
    let mut ordered_prefixes = prefixes.to_vec();
    ordered_prefixes.sort();

    let mut loci: LociMap = BTreeMap::new();
    let mut total = PlinkScanStats::default();
    for (prefix_rank, prefix) in ordered_prefixes.iter().enumerate() {
        eprintln!("▶️  fast-allele-freq (plink): {}", prefix.display());
        let stats = fold_plink_prefix(prefix, prefix_rank as u32, &mut loci)?;
        total.variants_seen += stats.variants_seen;
        total.variants_emitted += stats.variants_emitted;
        total.rows_emitted += stats.rows_emitted;
        total.missing_calls += stats.missing_calls;
        total.skipped_non_snp += stats.skipped_non_snp;
        total.skipped_bad_position += stats.skipped_bad_position;
    }
    Ok((loci, total))
}

fn fold_plink_prefix(prefix: &Path, rank: u32, loci: &mut LociMap) -> Result<PlinkScanStats> {
    let (bed_path, bim_path, fam_path) = plink_paths(prefix);
    let samples = read_fam(&fam_path)?;
    let variants = read_bim(&bim_path)?;
    if samples.is_empty() {
        bail!("PLINK FAM has no samples: {:?}", fam_path);
    }
    let n_samples = samples.len();
    let bytes_per_variant = n_samples.div_ceil(4);
    let expected_bed_bytes = 3 + (variants.len() * bytes_per_variant) as u64;
    let info = crate::plink::inspect_plink_prefix(prefix)?;
    if info.expected_bed_bytes != expected_bed_bytes {
        bail!("internal PLINK dimension mismatch for {}", prefix.display());
    }

    let mut bed =
        File::open(&bed_path).with_context(|| format!("Open PLINK BED {:?}", bed_path))?;
    let mut header = [0u8; 3];
    bed.read_exact(&mut header)
        .with_context(|| format!("Read PLINK BED header {:?}", bed_path))?;

    let mut row_bytes = vec![0u8; bytes_per_variant];
    let mut stats = PlinkScanStats::default();
    for variant in variants {
        bed.read_exact(&mut row_bytes)
            .with_context(|| format!("Read BED genotypes for {}", variant.id))?;
        stats.variants_seen += 1;
        let locus = match variant_locus_key(&variant) {
            Some(locus) => locus,
            None if variant.pos <= 0 => {
                stats.skipped_bad_position += 1;
                continue;
            }
            None => {
                stats.skipped_non_snp += 1;
                continue;
            }
        };

        let entry = loci.entry(locus).or_insert_with(|| Accum {
            rsid: String::new(),
            rsid_rank: u32::MAX,
            allele_count: 0,
            n_obs: 0,
            num_homo: 0,
            num_hetero: 0,
        });
        if rank < entry.rsid_rank {
            entry.rsid = clean_plink_id(&variant.id);
            entry.rsid_rank = rank;
        }

        stats.variants_emitted += 1;
        stats.rows_emitted += n_samples as u64;
        for sample_idx in 0..n_samples {
            let two_bit = (row_bytes[sample_idx / 4] >> ((sample_idx % 4) * 2)) & 0b11;
            match two_bit {
                0b00 => {
                    entry.allele_count += 2;
                    entry.n_obs += 1;
                    entry.num_homo += 1;
                }
                0b10 => {
                    entry.allele_count += 1;
                    entry.n_obs += 1;
                    entry.num_hetero += 1;
                }
                0b11 => {
                    entry.n_obs += 1;
                }
                0b01 => {
                    stats.missing_calls += 1;
                }
                _ => unreachable!(),
            }
        }
    }
    Ok(stats)
}

fn run_sequential(
    tasks: &[(u32, String, PathBuf)],
    shared: &Arc<SharedReference>,
    args: &FastAlleleFreqArgs,
) -> Result<(LociMap, Counts)> {
    let mut resolver = ReferenceResolver::shared(shared.clone());
    let mut logger = MissingRefLogger::new(args.missing_ref_log.as_deref(), args.warn_detail)?;
    let mut loci: LociMap = BTreeMap::new();
    for (idx, (rank, participant, path)) in tasks.iter().enumerate() {
        match collect_merged_long_rows(path, participant, &mut resolver, &mut logger) {
            Ok((rows, _stats)) => {
                for row in rows {
                    fold_row(&mut loci, row, *rank);
                }
            }
            Err(err) => eprintln!("⚠️  skipping {}: {err:#}", path.display()),
        }
        if (idx + 1) % 100 == 0 {
            eprintln!(
                "🧮 fast-allele-freq: {} files, {} loci so far",
                idx + 1,
                loci.len()
            );
        }
    }
    Ok((loci, logger.counts().clone()))
}

fn run_parallel(
    tasks: &[(u32, String, PathBuf)],
    shared: &Arc<SharedReference>,
    threads: usize,
) -> Result<(LociMap, Counts)> {
    // Split into exactly `threads` contiguous chunks so there are exactly `threads`
    // accumulators (one full-panel map each). Using par_iter().fold() instead lets
    // rayon create many more accumulators than threads, so peak RAM grew with file
    // count (e.g. 32 GB at 1400 files). Chunking pins peak RAM to threads x panel.
    // Each chunk folds its files into a local map + counts; maps are merged. Counts
    // are order-independent; rsid uses the rank-min rule so the merge is
    // deterministic and matches the single-threaded result.
    let chunk_size = tasks.len().div_ceil(threads.max(1)).max(1);
    let chunks: Vec<&[(u32, String, PathBuf)]> = tasks.chunks(chunk_size).collect();
    let result = chunks
        .into_par_iter()
        .map(|chunk| {
            let mut st = ThreadState::new(shared.clone());
            for (rank, participant, path) in chunk {
                match collect_merged_long_rows(path, participant, &mut st.resolver, &mut st.logger)
                {
                    Ok((rows, _stats)) => {
                        for row in rows {
                            fold_row(&mut st.loci, row, *rank);
                        }
                    }
                    Err(err) => eprintln!("⚠️  skipping {}: {err:#}", path.display()),
                }
            }
            (st.loci, st.logger.counts().clone())
        })
        .reduce(
            || (BTreeMap::new(), BTreeMap::new()),
            |(mut m1, mut c1), (m2, c2)| {
                merge_loci(&mut m1, m2);
                merge_counts(&mut c1, c2);
                (m1, c1)
            },
        );
    Ok(result)
}

/// Cap worker threads so peak RAM (~threads x one full-panel accumulator) fits the
/// budget. Estimate per-thread bytes from the reference rsid count (output loci are
/// ~1.5x that; ~1600 B/rsid covers the map entry + transient row buffers).
///
/// `max_ram_gb <= 0` => auto: 80% of detected available RAM (honors container cgroup
/// limits). If RAM can't be detected, fall back to the requested thread count.
fn cap_threads_for_ram(requested: usize, max_ram_gb: f64, ref_count: usize) -> usize {
    let budget = if max_ram_gb > 0.0 {
        max_ram_gb * 1e9
    } else {
        match crate::util::available_memory_bytes() {
            Some(bytes) => bytes as f64 * 0.8,
            None => return requested.max(1),
        }
    };
    let per_thread = (ref_count as f64 * 1600.0).max(1.0);
    // Keep ~0.5 GB headroom for the final merged map + I/O buffers.
    let usable = (budget - 0.5e9).max(per_thread);
    let cap = (usable / per_thread).floor() as usize;
    requested.min(cap.max(1)).max(1)
}

struct ThreadState {
    resolver: ReferenceResolver,
    logger: MissingRefLogger,
    loci: LociMap,
}

impl ThreadState {
    fn new(shared: Arc<SharedReference>) -> Self {
        // Lock-free in-memory resolver per thread (cheap Arc clone). No per-row
        // TSV in parallel mode (counts only); WarnDetail::None keeps worker
        // threads from interleaving per-row stderr. Construction cannot fail.
        Self {
            resolver: ReferenceResolver::shared(shared),
            logger: MissingRefLogger::new(None, crate::WarnDetail::None)
                .expect("count-only logger has no file handle and cannot fail"),
            loci: BTreeMap::new(),
        }
    }
}

fn fold_row(loci: &mut LociMap, row: LongRow, rank: u32) {
    let entry = loci.entry(row.locus_key).or_insert_with(|| Accum {
        rsid: String::new(),
        rsid_rank: u32::MAX,
        allele_count: 0,
        n_obs: 0,
        num_homo: 0,
        num_hetero: 0,
    });
    if !row.rsid.is_empty() && rank < entry.rsid_rank {
        entry.rsid = row.rsid;
        entry.rsid_rank = rank;
    }
    if row.dosage != -1 {
        entry.allele_count += row.dosage as i64;
        entry.n_obs += 1;
        if row.dosage == 2 {
            entry.num_homo += 1;
        } else if row.dosage == 1 {
            entry.num_hetero += 1;
        }
    }
}

fn merge_loci(into: &mut LociMap, from: LociMap) {
    for (locus, b) in from {
        match into.get_mut(&locus) {
            Some(a) => {
                a.allele_count += b.allele_count;
                a.n_obs += b.n_obs;
                a.num_homo += b.num_homo;
                a.num_hetero += b.num_hetero;
                if b.rsid_rank < a.rsid_rank {
                    a.rsid = b.rsid;
                    a.rsid_rank = b.rsid_rank;
                }
            }
            None => {
                into.insert(locus, b);
            }
        }
    }
}

fn merge_counts(into: &mut Counts, from: Counts) {
    for (code, n) in from {
        *into.entry(code).or_insert(0) += n;
    }
}

fn resolve_threads(requested: usize) -> usize {
    if requested > 0 {
        return requested;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1)
}

fn write_allele_freq(path: &Path, loci: &LociMap) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Create directory {:?}", parent))?;
        }
    }
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("Create {:?}", path))?);
    writeln!(
        writer,
        "locus_key\tallele_count\tallele_number\tnum_homo\tnum_hetero\tallele_freq\trsid"
    )?;
    for (locus, acc) in loci {
        let allele_number = 2 * acc.n_obs;
        let allele_freq = if allele_number > 0 {
            acc.allele_count as f64 / allele_number as f64
        } else {
            0.0
        };
        writeln!(
            writer,
            "{locus}\t{}\t{allele_number}\t{}\t{}\t{allele_freq:.6}\t{}",
            acc.allele_count, acc.num_homo, acc.num_hetero, acc.rsid
        )?;
    }
    writer.flush()?;
    Ok(())
}

pub(crate) fn collect_input_files(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for input in inputs {
        if input.is_file() {
            files.push(input.clone());
        } else if input.is_dir() {
            // follow_links: flow 04 stages genotype files as symlinks; without
            // this they'd be reported as symlink (not file) and skipped.
            for entry in walkdir::WalkDir::new(input)
                .follow_links(true)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let path = entry.path();
                    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if name.starts_with('.') {
                        continue;
                    }
                    if path.extension().and_then(|e| e.to_str()) == Some("bvlr") {
                        continue;
                    }
                    files.push(path.to_path_buf());
                }
            }
        } else {
            eprintln!("⚠️  Input path {:?} is not a file or directory", input);
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}
