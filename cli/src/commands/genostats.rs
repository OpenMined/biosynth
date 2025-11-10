use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use thiserror::Error;

use crate::genotype::process_file;
use crate::stats::StatsStore;
use crate::util::collect_input_files;
use crate::GenostatsArgs;

pub fn run_genostats(args: GenostatsArgs) -> Result<()> {
    if args.inputs.is_empty() {
        bail!("Provide at least one --input path");
    }

    let mut files = collect_input_files(&args.inputs)?;
    if let Some(max) = args.max_files {
        files.truncate(max);
    }

    if files.is_empty() {
        bail!("No genotype files discovered in the provided inputs");
    }

    println!("🧬 Discovered {} candidate files", files.len());

    let store = Arc::new(StatsStore::connect(&args.sqlite)?);
    let pb = Arc::new(ProgressBar::new(files.len() as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner} {pos}/{len} [{wide_bar}] {msg}")
            .expect("valid progress template")
            .progress_chars("=>-"),
    );

    let failures: Arc<Mutex<Vec<(PathBuf, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let threads = args.threads.max(1);
    let pool = ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .context("build rayon thread pool")?;

    pool.install(|| {
        files.par_iter().for_each(|path| {
            let pb = pb.clone();
            let store = store.clone();
            let failures = failures.clone();
            let skip_existing = args.skip_recorded_files;

            let result = process_single_file(&store, path, skip_existing);
            if let Err(err) = result {
                if err.downcast_ref::<SkipFile>().is_none() {
                    let mut guard = failures.lock().expect("poisoned failures mutex");
                    guard.push((path.clone(), err.to_string()));
                }
            }
            pb.inc(1);
        });
    });

    pb.finish_with_message("genotype parsing complete");

    let failures = Arc::try_unwrap(failures)
        .map(|mutex| mutex.into_inner().unwrap_or_default())
        .unwrap_or_else(|arc| arc.lock().expect("poisoned failures mutex").clone());

    if !failures.is_empty() {
        eprintln!("⚠️ Encountered {} errors:", failures.len());
        for (path, message) in &failures {
            eprintln!("   - {:?}: {}", path, message);
        }
    }

    let summary = store.summary()?;
    println!(
        "✅ Stored stats for {} files ({} variants; {} skipped rows)",
        summary.files_processed, summary.total_variants, summary.skipped_rows
    );
    println!(
        "📁 SQLite database ready at {}",
        summary.sqlite_path.display()
    );

    if let Some(summary_json) = args.summary_json {
        write_summary_json(&summary_json, &summary)?;
        println!("📝 Summary JSON written to {}", summary_json.display());
    }

    Ok(())
}

fn write_summary_json(path: &PathBuf, summary: &crate::stats::SummaryReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).with_context(|| format!("Create {:?}", parent))?;
        }
    }
    let mut file = File::create(path).with_context(|| format!("Create {:?}", path))?;
    serde_json::to_writer_pretty(&mut file, summary)?;
    file.write_all(b"\n")?;
    Ok(())
}

#[derive(Debug, Error)]
#[error("skip file")]
struct SkipFile;

fn process_single_file(store: &StatsStore, path: &Path, skip_if_recorded: bool) -> Result<()> {
    if skip_if_recorded && store.has_file(path)? {
        return Err(SkipFile.into());
    }

    let start = Instant::now();
    let mut conn = store.open_connection()?;
    let tx = conn.transaction()?;
    let parsed = process_file(path, |variant, metadata| {
        StatsStore::record_variant_in_tx(&tx, variant, metadata)
    })?;
    tx.commit()?;
    store.record_file(
        &conn,
        &parsed.metadata,
        &parsed.summary,
        start.elapsed(),
        path,
    )?;

    Ok(())
}
