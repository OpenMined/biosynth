use std::fs::File;
use std::io::{BufWriter, Write};

use anyhow::{bail, Context, Result};

use crate::rsid_cache::{default_cache_path, normalize_rsid, RsidCache};
use crate::stats::StatsStore;
use crate::ListMissingCacheArgs;

pub fn run_list_missing_cache(args: ListMissingCacheArgs) -> Result<()> {
    let cache_path = args.cache.unwrap_or_else(default_cache_path);
    if !cache_path.exists() {
        bail!("Cache file not found: {:?}", cache_path);
    }

    let cache = RsidCache::load(&cache_path)?;
    let store = StatsStore::connect(&args.sqlite)?;
    let known = store.known_rsids()?;

    let mut missing = Vec::new();
    for rsid in cache.entries.keys() {
        if rsid.trim().is_empty() || rsid == "." {
            continue;
        }
        let rsid_norm = normalize_rsid(rsid);
        let rsid_int = match rsid_norm.trim_start_matches("rs").parse::<i64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !known.contains(&rsid_int) {
            missing.push(rsid_norm);
        }
    }

    missing.sort();
    missing.dedup();

    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| format!("Create {:?}", parent))?;
        }
    }

    let mut writer = BufWriter::new(
        File::create(&args.output).with_context(|| format!("Create {:?}", args.output))?,
    );
    for rsid in &missing {
        writeln!(writer, "{rsid}")?;
    }
    writer.flush()?;

    println!(
        "✅ Wrote {} missing rsids to {}",
        missing.len(),
        args.output.display()
    );
    Ok(())
}
