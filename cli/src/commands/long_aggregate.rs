use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use crate::long_rows::{LongRow, LongRowReader, LongRowWriter};
use crate::AggregateLongArgs;

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok())
}

pub fn run_long_aggregate(args: AggregateLongArgs) -> Result<()> {
    let overall_start = Instant::now();
    let thread_count = resolve_thread_count(args.threads)?;
    let write_matrix = args.matrix_tsv.is_some();
    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("build aggregate-long thread pool")?;

    let input_files = collect_long_files(
        &args.inputs,
        args.input_list.as_ref(),
        args.input_glob.as_deref(),
    )?;
    if input_files.is_empty() {
        bail!(format_input_error(&args));
    }
    eprintln!(
        "▶️  aggregate-long: {} input file(s), threads={}",
        input_files.len(),
        thread_count
    );

    let tmp_dir = args
        .tmp_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("bvs-long"));
    fs::create_dir_all(&tmp_dir).with_context(|| format!("Create temp dir {:?}", tmp_dir))?;

    let chunk_records =
        resolve_chunk_records(args.chunk_records, args.max_ram_percent, thread_count)?;
    eprintln!(
        "🧵 aggregate-long: building chunks + participants (single pass), chunk_records={}",
        chunk_records
    );
    let chunk_build_start = Instant::now();
    let progress_every = env_u64("BVS_AGG_PROGRESS_EVERY").unwrap_or(1_000_000);
    let chunk_result = pool.install(|| {
        build_sharded_chunks(
            &input_files,
            &tmp_dir,
            thread_count,
            chunk_records,
            thread_count,
            progress_every,
            write_matrix,
        )
    })?;
    if chunk_result.total_rows == 0 {
        bail!("No records found in long-row inputs");
    }
    eprintln!(
        "📦 aggregate-long: scanned {} files, {} rows, {} shards",
        input_files.len(),
        chunk_result.total_rows,
        chunk_result.chunk_files.len()
    );
    eprintln!(
        "⏱️  aggregate-long: chunk build took {:.2}s",
        chunk_build_start.elapsed().as_secs_f64()
    );

    let participant_list = if write_matrix {
        let mut list: Vec<String> = chunk_result.participants.into_iter().collect();
        list.sort();
        list
    } else {
        Vec::new()
    };
    let participant_index: HashMap<String, usize> = if write_matrix {
        participant_list
            .iter()
            .enumerate()
            .map(|(idx, pid)| (pid.clone(), idx))
            .collect()
    } else {
        HashMap::new()
    };

    if write_matrix && participant_list.is_empty() {
        bail!("No participants found in long-row inputs");
    }

    if thread_count <= 1 {
        eprintln!("🧵 aggregate-long: single-threaded merge path");
        let ctx = AggregateContext::new(&participant_list, &participant_index);
        aggregate_chunks(
            &ctx,
            &chunk_result.chunk_files[0],
            args.matrix_tsv.as_deref(),
            &args.allele_freq_tsv,
            MergeProgress {
                total_rows: chunk_result.total_rows,
                global_merged: None,
                global_next_log_at: None,
            },
        )?;
    } else {
        eprintln!("🧵 aggregate-long: parallel shard merge path");
        let shard_start = Instant::now();
        let progress = MergeProgress {
            total_rows: chunk_result.total_rows,
            global_merged: Some(Arc::new(AtomicU64::new(0))),
            global_next_log_at: Some(Arc::new(AtomicU64::new(1_000_000))),
        };
        let part_outputs = pool.install(|| {
            chunk_result
                .chunk_files
                .par_iter()
                .enumerate()
                .filter(|(_, chunks)| !chunks.is_empty())
                .map(|(idx, shard_chunks)| {
                    eprintln!(
                        "🧩 aggregate-long: merging shard {} ({} chunks)",
                        idx,
                        shard_chunks.len()
                    );
                    let part_dir = tmp_dir.join(format!("part-{}", idx));
                    fs::create_dir_all(&part_dir)
                        .with_context(|| format!("Create partition output dir {:?}", part_dir))?;
                    let allele_path = part_dir.join("allele.tsv");
                    let matrix_path = args
                        .matrix_tsv
                        .as_ref()
                        .map(|_| part_dir.join("matrix.tsv"));
                    let ctx = AggregateContext::new(&participant_list, &participant_index);
                    aggregate_chunks(
                        &ctx,
                        shard_chunks,
                        matrix_path.as_deref(),
                        &allele_path,
                        progress.clone(),
                    )?;
                    Ok(PartitionOutput {
                        index: idx,
                        matrix: matrix_path,
                        allele: allele_path,
                    })
                })
                .collect::<Result<Vec<PartitionOutput>>>()
        })?;
        eprintln!(
            "⏱️  aggregate-long: shard aggregation took {:.2}s",
            shard_start.elapsed().as_secs_f64()
        );

        let concat_start = Instant::now();
        concat_partition_outputs(
            &part_outputs,
            &participant_list,
            args.matrix_tsv.as_deref(),
            &args.allele_freq_tsv,
        )?;
        eprintln!(
            "⏱️  aggregate-long: concatenation took {:.2}s",
            concat_start.elapsed().as_secs_f64()
        );
    }

    eprintln!(
        "✅ aggregate-long: total elapsed {:.2}s",
        overall_start.elapsed().as_secs_f64()
    );
    Ok(())
}

#[derive(Clone)]
struct PartitionOutput {
    index: usize,
    matrix: Option<PathBuf>,
    allele: PathBuf,
}

struct AggregateContext<'a> {
    participant_list: &'a [String],
    participant_index: &'a HashMap<String, usize>,
}

impl<'a> AggregateContext<'a> {
    fn new(participant_list: &'a [String], participant_index: &'a HashMap<String, usize>) -> Self {
        Self {
            participant_list,
            participant_index,
        }
    }
}

#[derive(Clone)]
struct MergeProgress {
    total_rows: u64,
    global_merged: Option<Arc<AtomicU64>>,
    global_next_log_at: Option<Arc<AtomicU64>>,
}

fn aggregate_chunks(
    ctx: &AggregateContext<'_>,
    chunk_files: &[PathBuf],
    matrix_path: Option<&Path>,
    allele_path: &Path,
    progress: MergeProgress,
) -> Result<()> {
    let mut matrix_writer = if let Some(path) = matrix_path {
        let mut writer =
            BufWriter::new(File::create(path).with_context(|| format!("Create {:?}", path))?);
        write_matrix_header(&mut writer, ctx.participant_list)?;
        Some(writer)
    } else {
        None
    };

    let mut allele_writer = BufWriter::new(
        File::create(allele_path).with_context(|| format!("Create {:?}", allele_path))?,
    );
    write_allele_header(&mut allele_writer)?;

    merge_chunks(
        chunk_files,
        ctx.participant_index,
        ctx.participant_list,
        matrix_writer.as_mut(),
        &mut allele_writer,
        &progress,
    )?;

    if let Some(writer) = matrix_writer.as_mut() {
        writer.flush()?;
    }
    allele_writer.flush()?;
    Ok(())
}

struct ShardedChunks {
    participants: HashSet<String>,
    chunk_files: Vec<Vec<PathBuf>>,
    total_rows: u64,
}

fn build_sharded_chunks(
    input_files: &[PathBuf],
    tmp_dir: &Path,
    shard_count: usize,
    chunk_records: usize,
    thread_count: usize,
    progress_every: u64,
    collect_participants: bool,
) -> Result<ShardedChunks> {
    let shard_count = shard_count.max(1);
    let shard_chunks: Mutex<Vec<Vec<PathBuf>>> =
        Mutex::new((0..shard_count).map(|_| Vec::new()).collect());
    let participants: Option<Mutex<HashSet<String>>> =
        collect_participants.then(|| Mutex::new(HashSet::new()));
    let total_rows = AtomicU64::new(0);
    let next_log_at = AtomicU64::new(progress_every.max(1));
    let start = Instant::now();

    input_files
        .par_iter()
        .enumerate()
        .try_for_each(|(file_idx, path)| -> Result<()> {
            let file_start = Instant::now();
            eprintln!(
                "📥 aggregate-long: reading {} ({})",
                file_idx + 1,
                path.display()
            );
            let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
            let mut reader = LongRowReader::new(file);
            let mut buffers: Vec<Vec<LongRow>> = (0..shard_count)
                .map(|_| Vec::with_capacity(10_000))
                .collect();
            let mut local_participants: Option<HashSet<String>> =
                collect_participants.then(HashSet::new);
            let mut chunk_indices: Vec<usize> = vec![0; shard_count];
            let mut local_rows: u64 = 0;
            while let Some(row) = reader.read_row()? {
                if let Some(local) = local_participants.as_mut() {
                    local.insert(row.participant_id.clone());
                }
                let shard = shard_for(&row.locus_key, shard_count);
                buffers[shard].push(row);
                local_rows += 1;

                if buffers[shard].len() >= chunk_records {
                    let chunk_path = write_shard_chunk(
                        tmp_dir,
                        shard,
                        file_idx,
                        chunk_indices[shard],
                        &mut buffers[shard],
                        thread_count,
                    )?;
                    {
                        let mut guard = shard_chunks.lock().expect("shard chunk mutex poisoned");
                        guard[shard].push(chunk_path);
                    }
                    chunk_indices[shard] += 1;
                }
            }

            for shard in 0..shard_count {
                if !buffers[shard].is_empty() {
                    let chunk_path = write_shard_chunk(
                        tmp_dir,
                        shard,
                        file_idx,
                        chunk_indices[shard],
                        &mut buffers[shard],
                        thread_count,
                    )?;
                    let mut guard = shard_chunks.lock().expect("shard chunk mutex poisoned");
                    guard[shard].push(chunk_path);
                    chunk_indices[shard] += 1;
                }
            }

            if let (Some(participants), Some(local_participants)) =
                (participants.as_ref(), local_participants)
            {
                let mut guard = participants.lock().expect("participants mutex poisoned");
                guard.extend(local_participants);
            }

            let new_total = total_rows.fetch_add(local_rows, AtomicOrdering::Relaxed) + local_rows;
            if progress_every > 0 {
                let mut next = next_log_at.load(AtomicOrdering::Relaxed);
                while new_total >= next {
                    if next_log_at
                        .compare_exchange(
                            next,
                            next + progress_every,
                            AtomicOrdering::Relaxed,
                            AtomicOrdering::Relaxed,
                        )
                        .is_ok()
                    {
                        eprintln!(
                            "?? aggregate-long: partitioned {} rows ({:.1}s elapsed)",
                            next,
                            start.elapsed().as_secs_f64()
                        );
                        break;
                    }
                    next = next_log_at.load(AtomicOrdering::Relaxed);
                }
            }

            eprintln!(
                "?? aggregate-long: finished {} rows from {} ({:.2}s)",
                local_rows,
                path.display(),
                file_start.elapsed().as_secs_f64()
            );

            Ok(())
        })?;

    let participants = if let Some(participants) = participants {
        participants
            .into_inner()
            .expect("participants mutex poisoned")
    } else {
        HashSet::new()
    };
    let chunk_files = shard_chunks
        .into_inner()
        .expect("shard chunk mutex poisoned");

    Ok(ShardedChunks {
        participants,
        chunk_files,
        total_rows: total_rows.load(AtomicOrdering::Relaxed),
    })
}

fn shard_for(value: &str, shard_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

fn concat_partition_outputs(
    outputs: &[PartitionOutput],
    participants: &[String],
    matrix_path: Option<&Path>,
    allele_path: &Path,
) -> Result<()> {
    let mut ordered = outputs.to_vec();
    ordered.sort_by_key(|out| out.index);

    if let Some(path) = matrix_path {
        let mut matrix_writer =
            BufWriter::new(File::create(path).with_context(|| format!("Create {:?}", path))?);
        write_matrix_header(&mut matrix_writer, participants)?;
        for part in &ordered {
            if let Some(part_matrix) = &part.matrix {
                append_without_header(part_matrix, &mut matrix_writer)?;
            }
        }
        matrix_writer.flush()?;
    }

    let mut allele_writer = BufWriter::new(
        File::create(allele_path).with_context(|| format!("Create {:?}", allele_path))?,
    );
    write_allele_header(&mut allele_writer)?;
    for part in &ordered {
        append_without_header(&part.allele, &mut allele_writer)?;
    }
    allele_writer.flush()?;
    Ok(())
}

fn append_without_header(path: &Path, writer: &mut BufWriter<File>) -> Result<()> {
    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut is_first = true;
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        if is_first {
            is_first = false;
            continue;
        }
        writer.write_all(line.as_bytes())?;
    }
    Ok(())
}

fn collect_long_files(
    inputs: &[PathBuf],
    input_list: Option<&PathBuf>,
    input_glob: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if let Some(list_path) = input_list {
        let list_files = read_input_list(list_path)?;
        for path in list_files {
            if path.is_file() {
                if is_long_file(&path) {
                    files.push(path);
                }
            } else if path.is_dir() {
                collect_from_dir(&path, &mut files)?;
            } else {
                eprintln!("⚠️ Missing input path from list: {}", path.display());
            }
        }
    }

    if let Some(pattern) = input_glob {
        match glob::glob(pattern) {
            Ok(paths) => {
                for entry in paths {
                    match entry {
                        Ok(path) => {
                            if path.is_file() && is_long_file(&path) {
                                files.push(path);
                            } else if path.is_dir() {
                                collect_from_dir(&path, &mut files)?;
                            }
                        }
                        Err(err) => eprintln!("⚠️ Glob error: {err}"),
                    }
                }
            }
            Err(err) => eprintln!("⚠️ Invalid glob pattern '{pattern}': {err}"),
        }
    }

    for input in inputs {
        if input.is_file() {
            if is_long_file(input) {
                files.push(input.to_path_buf());
            }
            continue;
        }
        if input.is_dir() {
            collect_from_dir(input, &mut files)?;
            continue;
        }
        eprintln!("⚠️ Input path {:?} is not a file or directory", input);
    }

    if files.is_empty() && inputs.is_empty() && input_list.is_none() && input_glob.is_none() {
        bail!("Provide --input, --input-list, or --input-glob");
    }

    files.sort();
    files.dedup();
    Ok(files)
}

fn is_long_file(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some("bvlr")
}

fn collect_from_dir(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if is_long_file(path) {
            files.push(path.to_path_buf());
        }
    }
    Ok(())
}

fn read_input_list(path: &Path) -> Result<Vec<PathBuf>> {
    let file = File::open(path).with_context(|| format!("Open input list {:?}", path))?;
    let reader = BufReader::new(file);
    let mut paths = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        paths.push(PathBuf::from(trimmed));
    }
    Ok(paths)
}

fn format_input_error(args: &AggregateLongArgs) -> String {
    let mut lines = Vec::new();
    lines.push("No .bvlr files found.".to_string());
    if !args.inputs.is_empty() {
        lines.push(format!("--input count: {}", args.inputs.len()));
    }
    if let Some(list) = &args.input_list {
        lines.push(format!("--input-list: {}", list.display()));
    }
    if let Some(pattern) = &args.input_glob {
        lines.push(format!("--input-glob: {pattern}"));
    }
    lines.join(" ")
}

fn write_shard_chunk(
    tmp_dir: &Path,
    shard: usize,
    file_idx: usize,
    idx: usize,
    buffer: &mut Vec<LongRow>,
    thread_count: usize,
) -> Result<PathBuf> {
    if thread_count > 1 {
        buffer.par_sort_by(compare_row);
    } else {
        buffer.sort_by(compare_row);
    }
    let filename = format!(
        "shard-{shard}-file-{file_idx}-chunk-{idx}-{}.bvlr",
        unique_suffix()
    );
    let path = tmp_dir.join(filename);
    let file = File::create(&path).with_context(|| format!("Create {:?}", path))?;
    let mut writer = LongRowWriter::new(BufWriter::new(file));
    for row in buffer.iter() {
        writer.write_row(row)?;
    }
    writer.flush()?;
    buffer.clear();
    Ok(path)
}

fn merge_chunks(
    chunks: &[PathBuf],
    participant_index: &HashMap<String, usize>,
    participants: &[String],
    mut matrix_writer: Option<&mut BufWriter<File>>,
    allele_writer: &mut BufWriter<File>,
    progress: &MergeProgress,
) -> Result<()> {
    let mut readers: Vec<LongRowReader<File>> = Vec::new();
    for path in chunks {
        let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
        readers.push(LongRowReader::new(file));
    }

    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = reader.read_row()? {
            heap.push(HeapItem::new(idx, row));
        }
    }

    let mut current_locus: Option<String> = None;
    let mut current_rsid = String::new();
    let write_matrix = matrix_writer.is_some();
    let mut current_row: Vec<i8> = Vec::new();

    let mut allele_count: i64 = 0;
    let mut n_obs: i64 = 0;
    let mut num_homo: i64 = 0;
    let mut num_hetero: i64 = 0;

    let mut merged_rows: u64 = 0;
    let mut next_log_at: u64 = 1_000_000;
    while let Some(mut item) = heap.pop() {
        let row = &item.row;
        if current_locus.as_deref() != Some(&row.locus_key) {
            if let Some(locus) = current_locus.take() {
                if let Some(writer) = matrix_writer.as_mut() {
                    flush_matrix_row(writer, &locus, &current_rsid, &current_row)?;
                }
                flush_allele_row(
                    allele_writer,
                    &locus,
                    allele_count,
                    n_obs,
                    num_homo,
                    num_hetero,
                    &current_rsid,
                )?;
            }
            current_locus = Some(row.locus_key.clone());
            current_rsid = row.rsid.clone();
            if write_matrix {
                current_row = vec![-1; participants.len()];
            }
            allele_count = 0;
            n_obs = 0;
            num_homo = 0;
            num_hetero = 0;
        } else if current_rsid.is_empty() && !row.rsid.is_empty() {
            current_rsid = row.rsid.clone();
        }

        if write_matrix {
            if let Some(idx) = participant_index.get(&row.participant_id) {
                if current_row[*idx] == -1 && row.dosage != -1 {
                    current_row[*idx] = row.dosage;
                }
            }
        }

        if row.dosage != -1 {
            allele_count += row.dosage as i64;
            n_obs += 1;
            if row.dosage == 2 {
                num_homo += 1;
            } else if row.dosage == 1 {
                num_hetero += 1;
            }
        }

        if let Some(next_row) = readers[item.reader_idx].read_row()? {
            item.row = next_row;
            heap.push(item);
        }
        merged_rows += 1;
        if let (Some(global), Some(next_at)) =
            (&progress.global_merged, &progress.global_next_log_at)
        {
            let new_total = global.fetch_add(1, AtomicOrdering::Relaxed) + 1;
            let mut next = next_at.load(AtomicOrdering::Relaxed);
            while new_total >= next {
                if next_at
                    .compare_exchange(
                        next,
                        next + 1_000_000,
                        AtomicOrdering::Relaxed,
                        AtomicOrdering::Relaxed,
                    )
                    .is_ok()
                {
                    if progress.total_rows > 0 {
                        let pct = (new_total as f64 / progress.total_rows as f64) * 100.0;
                        eprintln!("🔀 aggregate-long: merged {} rows ({:.1}%)", new_total, pct);
                    } else {
                        eprintln!("🔀 aggregate-long: merged {} rows", new_total);
                    }
                    break;
                }
                next = next_at.load(AtomicOrdering::Relaxed);
            }
        } else if merged_rows >= next_log_at {
            if progress.total_rows > 0 {
                let pct = (merged_rows as f64 / progress.total_rows as f64) * 100.0;
                eprintln!(
                    "🔀 aggregate-long: merged {} rows ({:.1}%)",
                    merged_rows, pct
                );
            } else {
                eprintln!("🔀 aggregate-long: merged {} rows", merged_rows);
            }
            next_log_at += 1_000_000;
        }
    }

    if let Some(locus) = current_locus.take() {
        if let Some(writer) = matrix_writer.as_mut() {
            flush_matrix_row(writer, &locus, &current_rsid, &current_row)?;
        }
        flush_allele_row(
            allele_writer,
            &locus,
            allele_count,
            n_obs,
            num_homo,
            num_hetero,
            &current_rsid,
        )?;
    }

    if let Some(global) = progress.global_merged.as_ref() {
        let global_rows = global.load(AtomicOrdering::Relaxed);
        if progress.total_rows > 0 {
            let pct = (global_rows as f64 / progress.total_rows as f64) * 100.0;
            eprintln!(
                "✅ aggregate-long: merge complete (shard {} rows, overall {:.1}%)",
                merged_rows, pct
            );
        } else {
            eprintln!(
                "✅ aggregate-long: merge complete (shard {} rows)",
                merged_rows
            );
        }
    } else if progress.total_rows > 0 {
        let pct = (merged_rows as f64 / progress.total_rows as f64) * 100.0;
        eprintln!(
            "✅ aggregate-long: merge complete ({} rows, {:.1}%)",
            merged_rows, pct
        );
    } else {
        eprintln!("✅ aggregate-long: merge complete ({} rows)", merged_rows);
    }
    Ok(())
}

fn write_matrix_header(writer: &mut BufWriter<File>, participants: &[String]) -> Result<()> {
    write!(writer, "locus_key\trsid")?;
    for pid in participants {
        write!(writer, "\t{pid}")?;
    }
    writeln!(writer)?;
    Ok(())
}

fn write_allele_header(writer: &mut BufWriter<File>) -> Result<()> {
    writeln!(
        writer,
        "locus_key\tallele_count\tallele_number\tnum_homo\tnum_hetero\tallele_freq\trsid"
    )?;
    Ok(())
}

fn resolve_thread_count(requested: usize) -> Result<usize> {
    if requested > 0 {
        return Ok(requested);
    }
    let count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    Ok(count.max(1))
}

fn resolve_chunk_records(requested: usize, max_ram_percent: u8, threads: usize) -> Result<usize> {
    if requested > 0 {
        return Ok(requested);
    }
    let available = detect_available_memory_bytes().unwrap_or(512 * 1024 * 1024);
    let percent = max_ram_percent.clamp(10, 95) as u64;
    let budget = available.saturating_mul(percent) / 100;
    // Conservative row size estimate for BVLR rows (strings + overhead).
    let bytes_per_row: u64 = 128;
    let total_rows = budget / bytes_per_row;
    let per_thread = (total_rows / threads.max(1) as u64).max(10_000);
    Ok(per_thread as usize)
}

fn detect_available_memory_bytes() -> Option<u64> {
    // cgroups v2
    if let Ok(value) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = value.trim();
        if trimmed != "max" {
            if let Ok(bytes) = trimmed.parse::<u64>() {
                if bytes > 0 {
                    return Some(bytes);
                }
            }
        }
    }
    // cgroups v1
    if let Ok(value) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(bytes) = value.trim().parse::<u64>() {
            if bytes > 0 {
                return Some(bytes);
            }
        }
    }
    // /proc/meminfo fallback
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if let Some(rest) = line.strip_prefix("MemAvailable:") {
                let parts: Vec<&str> = rest.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[0].parse::<u64>() {
                        return Some(kb * 1024);
                    }
                }
            }
        }
    }
    None
}

fn flush_matrix_row(
    writer: &mut BufWriter<File>,
    locus: &str,
    rsid: &str,
    row: &[i8],
) -> Result<()> {
    write!(writer, "{locus}\t{rsid}")?;
    for val in row {
        write!(writer, "\t{val}")?;
    }
    writeln!(writer)?;
    Ok(())
}

fn flush_allele_row(
    writer: &mut BufWriter<File>,
    locus: &str,
    allele_count: i64,
    n_obs: i64,
    num_homo: i64,
    num_hetero: i64,
    rsid: &str,
) -> Result<()> {
    let allele_number = 2 * n_obs;
    let allele_freq = if allele_number > 0 {
        allele_count as f64 / allele_number as f64
    } else {
        0.0
    };
    writeln!(
        writer,
        "{locus}\t{allele_count}\t{allele_number}\t{num_homo}\t{num_hetero}\t{allele_freq:.6}\t{rsid}"
    )?;
    Ok(())
}

fn compare_row(a: &LongRow, b: &LongRow) -> Ordering {
    let locus_cmp = a.locus_key.cmp(&b.locus_key);
    if locus_cmp != Ordering::Equal {
        return locus_cmp;
    }
    a.participant_id.cmp(&b.participant_id)
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[derive(Debug)]
struct HeapItem {
    reader_idx: usize,
    row: LongRow,
}

impl HeapItem {
    fn new(reader_idx: usize, row: LongRow) -> Self {
        Self { reader_idx, row }
    }
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        compare_row(&self.row, &other.row) == Ordering::Equal
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap behavior.
        compare_row(&other.row, &self.row)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn write_rows(path: &Path, rows: &[LongRow]) {
        let file = File::create(path).unwrap();
        let mut writer = LongRowWriter::new(BufWriter::new(file));
        for row in rows {
            writer.write_row(row).unwrap();
        }
        writer.flush().unwrap();
    }

    #[test]
    fn aggregate_emits_matrix_and_allele_freq() {
        let tmp = std::env::temp_dir().join(format!("bvs-test-{}", unique_suffix()));
        fs::create_dir_all(&tmp).unwrap();
        let file1 = tmp.join("a.bvlr");
        let file2 = tmp.join("b.bvlr");

        write_rows(
            &file1,
            &[
                LongRow {
                    locus_key: "1-100-A-G".to_string(),
                    rsid: "rs1".to_string(),
                    participant_id: "P1".to_string(),
                    dosage: 2,
                },
                LongRow {
                    locus_key: "1-100-A-G".to_string(),
                    rsid: "".to_string(),
                    participant_id: "P2".to_string(),
                    dosage: 1,
                },
            ],
        );
        write_rows(
            &file2,
            &[LongRow {
                locus_key: "1-200-C-T".to_string(),
                rsid: "rs2".to_string(),
                participant_id: "P1".to_string(),
                dosage: -1,
            }],
        );

        let args = AggregateLongArgs {
            inputs: vec![file1.clone(), file2.clone()],
            input_list: None,
            input_glob: None,
            matrix_tsv: Some(tmp.join("matrix.tsv")),
            allele_freq_tsv: tmp.join("allele.tsv"),
            tmp_dir: Some(tmp.join("tmp")),
            chunk_records: 2,
            threads: 0,
            max_ram_percent: 80,
        };
        run_long_aggregate(args).unwrap();

        let mut matrix = String::new();
        File::open(tmp.join("matrix.tsv"))
            .unwrap()
            .read_to_string(&mut matrix)
            .unwrap();
        assert!(matrix.contains("locus_key\trsid\tP1\tP2"));
        assert!(matrix.contains("1-100-A-G\trs1\t2\t1"));

        let mut allele = String::new();
        File::open(tmp.join("allele.tsv"))
            .unwrap()
            .read_to_string(&mut allele)
            .unwrap();
        assert!(allele.contains(
            "locus_key\tallele_count\tallele_number\tnum_homo\tnum_hetero\tallele_freq\trsid"
        ));
        assert!(allele.contains("1-100-A-G\t3\t4\t1\t1\t0.750000\trs1"));
    }
}
