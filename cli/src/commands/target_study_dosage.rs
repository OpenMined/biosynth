use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use crate::commands::fast_allele_freq::collect_input_files;
use crate::commands::long_emit::{
    collect_merged_long_rows, default_participant, summarize_warning_counts, MissingRefLogger,
    ReferenceResolver, SharedReference,
};
use crate::download::ensure_reference_db;
use crate::long_rows::LongRow;
use crate::stats::StatsStore;
use crate::TargetStudyDosageArgs;

const MISSING: u8 = 255;

#[derive(Clone)]
struct Variant {
    chrom: String,
    pos: i64,
    rsid: String,
    reference: String,
    alternate: String,
    locus_key: String,
}

struct SampleResult {
    row_idx: usize,
    hits: Vec<(usize, u8)>,
    counts: std::collections::BTreeMap<String, u64>,
}

struct OutputState<'a> {
    n_variants: usize,
    matrix: &'a mut [u8],
    allele_count: &'a mut [i64],
    n_obs: &'a mut [i64],
    num_homo: &'a mut [i64],
    num_hetero: &'a mut [i64],
    warning_counts: &'a mut std::collections::BTreeMap<String, u64>,
}

pub fn run_target_study_dosage(args: TargetStudyDosageArgs) -> Result<()> {
    let overall_start = Instant::now();

    let sqlite_path = ensure_reference_db(Some(&args.sqlite), args.force_download)?;
    let store = StatsStore::connect_read_only(&sqlite_path)?;
    let preload_start = Instant::now();
    let shared = Arc::new(SharedReference::load(&store)?);
    eprintln!(
        "📚 target-study-dosage: reference preloaded in {:.2}s",
        preload_start.elapsed().as_secs_f64()
    );

    let variants = read_variants(&args.variants_tsv)?;
    if variants.is_empty() {
        bail!("No variants found in {}", args.variants_tsv.display());
    }
    let key_to_col: HashMap<String, usize> = variants
        .iter()
        .enumerate()
        .map(|(idx, variant)| (variant.locus_key.clone(), idx))
        .collect();

    let files = collect_input_files(&args.inputs)?;
    if files.is_empty() {
        bail!("No input genotype files found under the provided --input paths");
    }
    let mut tasks: Vec<(String, PathBuf)> = files
        .into_iter()
        .map(|path| (dosage_participant(&path), path))
        .collect();
    tasks.sort_by(|a, b| a.0.cmp(&b.0));
    let tasks: Vec<(usize, String, PathBuf)> = tasks
        .into_iter()
        .enumerate()
        .map(|(idx, (pid, path))| (idx, pid, path))
        .collect();

    let force_single = args.missing_ref_log.is_some();
    let threads = if force_single {
        1
    } else {
        resolve_threads(args.threads)
    };
    let batch_samples = if args.batch_samples > 0 {
        args.batch_samples
    } else {
        threads.max(1)
    };
    eprintln!(
        "▶️  target-study-dosage: {} files, {} variants, threads={}, batch_samples={}",
        tasks.len(),
        variants.len(),
        threads,
        batch_samples
    );

    let n_samples = tasks.len();
    let n_variants = variants.len();
    let mut matrix = vec![MISSING; n_samples * n_variants];
    let mut allele_count = vec![0_i64; n_variants];
    let mut n_obs = vec![0_i64; n_variants];
    let mut num_homo = vec![0_i64; n_variants];
    let mut num_hetero = vec![0_i64; n_variants];
    let mut warning_counts = std::collections::BTreeMap::new();

    if threads <= 1 {
        let mut resolver = ReferenceResolver::shared(shared.clone());
        let mut logger = MissingRefLogger::new(args.missing_ref_log.as_deref(), args.warn_detail)?;
        for (batch_idx, batch) in tasks.chunks(batch_samples).enumerate() {
            for (row_idx, participant, path) in batch {
                match parse_sample(
                    *row_idx,
                    participant,
                    path,
                    &key_to_col,
                    &mut resolver,
                    &mut logger,
                ) {
                    Ok(mut result) => {
                        // The sequential logger is cumulative and may also write
                        // per-row details, so merge its counts once after the loop.
                        result.counts.clear();
                        let mut output = OutputState {
                            n_variants,
                            matrix: &mut matrix,
                            allele_count: &mut allele_count,
                            n_obs: &mut n_obs,
                            num_homo: &mut num_homo,
                            num_hetero: &mut num_hetero,
                            warning_counts: &mut warning_counts,
                        };
                        apply_sample_result(result, &mut output)
                    }
                    Err(err) => eprintln!("⚠️  skipping {}: {err:#}", path.display()),
                }
            }
            eprintln!(
                "🧬 target-study-dosage: parsed {}/{} samples",
                ((batch_idx + 1) * batch_samples).min(n_samples),
                n_samples
            );
        }
        merge_counts(&mut warning_counts, logger.counts().clone());
    } else {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .context("build target-study-dosage thread pool")?;
        for (batch_idx, batch) in tasks.chunks(batch_samples).enumerate() {
            let results: Vec<Result<SampleResult>> = pool.install(|| {
                batch
                    .par_iter()
                    .map(|(row_idx, participant, path)| {
                        let mut resolver = ReferenceResolver::shared(shared.clone());
                        let mut logger = MissingRefLogger::new(None, crate::WarnDetail::None)
                            .expect("count-only logger has no file handle and cannot fail");
                        parse_sample(
                            *row_idx,
                            participant,
                            path,
                            &key_to_col,
                            &mut resolver,
                            &mut logger,
                        )
                    })
                    .collect()
            });
            for (task, result) in batch.iter().zip(results) {
                match result {
                    Ok(result) => {
                        let mut output = OutputState {
                            n_variants,
                            matrix: &mut matrix,
                            allele_count: &mut allele_count,
                            n_obs: &mut n_obs,
                            num_homo: &mut num_homo,
                            num_hetero: &mut num_hetero,
                            warning_counts: &mut warning_counts,
                        };
                        apply_sample_result(result, &mut output)
                    }
                    Err(err) => eprintln!("⚠️  skipping {}: {err:#}", task.2.display()),
                }
            }
            eprintln!(
                "🧬 target-study-dosage: parsed {}/{} samples",
                ((batch_idx + 1) * batch_samples).min(n_samples),
                n_samples
            );
        }
    }

    write_npy_u8_2d(&args.dosage_npy, n_samples, n_variants, &matrix)?;
    write_samples(&args.samples_tsv, &tasks)?;
    write_variants(&args.study_variants_tsv, &variants)?;
    write_allele_freq(
        &args.allele_freq_tsv,
        &variants,
        &allele_count,
        &n_obs,
        &num_homo,
        &num_hetero,
    )?;

    if args.warn_detail != crate::WarnDetail::None {
        let summary = summarize_warning_counts(&warning_counts);
        if !summary.is_empty() {
            eprintln!("⚠️  target-study-dosage warnings: {summary}");
        }
    }
    eprintln!(
        "✅ target-study-dosage: {} samples x {} variants, {:.2}s",
        n_samples,
        n_variants,
        overall_start.elapsed().as_secs_f64()
    );
    println!("✅ Wrote dosage matrix to {}", args.dosage_npy.display());
    Ok(())
}

fn parse_sample(
    row_idx: usize,
    participant: &str,
    path: &Path,
    key_to_col: &HashMap<String, usize>,
    resolver: &mut ReferenceResolver,
    logger: &mut MissingRefLogger,
) -> Result<SampleResult> {
    let (rows, _stats) = collect_merged_long_rows(path, participant, resolver, logger)?;
    let mut non_target_alt_present: HashSet<String> = HashSet::new();
    for row in &rows {
        if row.dosage > 0 && !key_to_col.contains_key(&row.locus_key) {
            if let Some(base_key) = locus_base_key(&row.locus_key) {
                non_target_alt_present.insert(base_key);
            }
        }
    }
    let mut hits = Vec::new();
    for row in rows {
        push_hit(&row, key_to_col, &non_target_alt_present, &mut hits);
    }
    Ok(SampleResult {
        row_idx,
        hits,
        counts: logger.counts().clone(),
    })
}

fn push_hit(
    row: &LongRow,
    key_to_col: &HashMap<String, usize>,
    non_target_alt_present: &HashSet<String>,
    hits: &mut Vec<(usize, u8)>,
) {
    if row.dosage < 0 {
        return;
    }
    if let Some(base_key) = locus_base_key(&row.locus_key) {
        if non_target_alt_present.contains(&base_key) {
            return;
        }
    }
    if let Some(&col) = key_to_col.get(&row.locus_key) {
        hits.push((col, row.dosage as u8));
    }
}

fn locus_base_key(locus_key: &str) -> Option<String> {
    let (base, _alt) = locus_key.rsplit_once('-')?;
    Some(base.to_string())
}

fn apply_sample_result(result: SampleResult, output: &mut OutputState<'_>) {
    merge_counts(output.warning_counts, result.counts);
    let row_offset = result.row_idx * output.n_variants;
    for (col, dosage) in result.hits {
        let cell = &mut output.matrix[row_offset + col];
        if *cell != MISSING {
            continue;
        }
        *cell = dosage;
        output.allele_count[col] += dosage as i64;
        output.n_obs[col] += 1;
        if dosage == 2 {
            output.num_homo[col] += 1;
        } else if dosage == 1 {
            output.num_hetero[col] += 1;
        }
    }
}

fn read_variants(path: &Path) -> Result<Vec<Variant>> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_path(path)
        .with_context(|| format!("Open variants TSV {}", path.display()))?;
    let headers = reader.headers()?.clone();
    let col = |name: &str| -> Result<usize> {
        headers
            .iter()
            .position(|h| h == name)
            .with_context(|| format!("{}: missing {name} column", path.display()))
    };
    let chrom_col = col("chrom")?;
    let pos_col = col("pos")?;
    let rsid_col = col("rsid")?;
    let ref_col = col("ref")?;
    let alt_col = col("alt")?;

    let mut variants = Vec::new();
    for record in reader.records() {
        let record = record?;
        let chrom = normalize_chrom(record.get(chrom_col).unwrap_or(""));
        let pos: i64 = record
            .get(pos_col)
            .unwrap_or("")
            .parse()
            .with_context(|| format!("{}: invalid pos in variants TSV", path.display()))?;
        let rsid = record.get(rsid_col).unwrap_or("").to_string();
        let reference = record
            .get(ref_col)
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        let alternate = record
            .get(alt_col)
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        if reference.len() != 1 || alternate.len() != 1 {
            continue;
        }
        let locus_key = format!("{chrom}-{pos}-{reference}-{alternate}");
        variants.push(Variant {
            chrom,
            pos,
            rsid,
            reference,
            alternate,
            locus_key,
        });
    }
    Ok(variants)
}

fn normalize_chrom(value: &str) -> String {
    let text = value.trim();
    let text = text
        .strip_prefix("chr")
        .or_else(|| text.strip_prefix("CHR"))
        .unwrap_or(text);
    if text == "M" {
        "MT".to_string()
    } else {
        text.to_ascii_uppercase()
    }
}

fn dosage_participant(path: &Path) -> String {
    if let Some(parent) = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|p| p.to_str())
    {
        if !matches!(parent, "" | "." | "geno" | "input" | "participants") {
            return parent.replace(' ', "_");
        }
    }
    default_participant(path)
}

fn write_npy_u8_2d(path: &Path, rows: usize, cols: usize, data: &[u8]) -> Result<()> {
    if data.len() != rows * cols {
        bail!("matrix data length does not match requested shape");
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Create directory {:?}", parent))?;
        }
    }
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("Create {:?}", path))?);
    let mut header =
        format!("{{'descr': '|u1', 'fortran_order': False, 'shape': ({rows}, {cols}), }}");
    let preamble_len = 10;
    let padding = (16 - ((preamble_len + header.len() + 1) % 16)) % 16;
    header.push_str(&" ".repeat(padding));
    header.push('\n');
    if header.len() > u16::MAX as usize {
        bail!("NPY v1 header is too large");
    }
    writer.write_all(b"\x93NUMPY")?;
    writer.write_all(&[1, 0])?;
    writer.write_all(&(header.len() as u16).to_le_bytes())?;
    writer.write_all(header.as_bytes())?;
    writer.write_all(data)?;
    writer.flush()?;
    Ok(())
}

fn write_samples(path: &Path, tasks: &[(usize, String, PathBuf)]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Create directory {:?}", parent))?;
        }
    }
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("Create {:?}", path))?);
    writeln!(writer, "sample_id\tfile")?;
    for (_idx, sample_id, file) in tasks {
        writeln!(writer, "{sample_id}\t{}", file.display())?;
    }
    writer.flush()?;
    Ok(())
}

fn write_variants(path: &Path, variants: &[Variant]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Create directory {:?}", parent))?;
        }
    }
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("Create {:?}", path))?);
    writeln!(writer, "chrom\tpos\trsid\tref\talt\tlocus_key")?;
    for variant in variants {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}\t{}\t{}",
            variant.chrom,
            variant.pos,
            variant.rsid,
            variant.reference,
            variant.alternate,
            variant.locus_key
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_allele_freq(
    path: &Path,
    variants: &[Variant],
    allele_count: &[i64],
    n_obs: &[i64],
    num_homo: &[i64],
    num_hetero: &[i64],
) -> Result<()> {
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
    for (idx, variant) in variants.iter().enumerate() {
        let allele_number = 2 * n_obs[idx];
        let allele_freq = if allele_number > 0 {
            allele_count[idx] as f64 / allele_number as f64
        } else {
            0.0
        };
        writeln!(
            writer,
            "{}\t{}\t{allele_number}\t{}\t{}\t{allele_freq:.6}\t{}",
            variant.locus_key, allele_count[idx], num_homo[idx], num_hetero[idx], variant.rsid
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn merge_counts(
    into: &mut std::collections::BTreeMap<String, u64>,
    from: std::collections::BTreeMap<String, u64>,
) {
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
