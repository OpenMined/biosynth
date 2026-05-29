use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
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

fn collect_input_files(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
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
