use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::rsid_cache::RsidCache;
use crate::stats::{ReferenceVariant, StatsStore};
use crate::SyncRsidCacheArgs;

#[derive(Default)]
struct SyncStats {
    total: usize,
    inserted: usize,
    updated: usize,
    skipped_missing_pos: usize,
    skipped_unusable: usize,
    unchanged: usize,
}

pub fn run_sync_rsid_cache(args: SyncRsidCacheArgs) -> Result<()> {
    if !args.cache.exists() {
        bail!("Cache file not found: {:?}", args.cache);
    }
    let mut cache = RsidCache::load(&args.cache)?;
    let mut pos_map = HashMap::new();
    if let Some(missing_file) = &args.missing_file {
        pos_map = load_missing_positions(missing_file)?;
    }

    let store = StatsStore::connect(&args.sqlite)?;
    let existing = load_existing_map(&store)?;

    let mut stats = SyncStats::default();
    let mut conn = store.open_connection()?;
    let tx = conn.transaction()?;

    for (rsid, entry) in cache.entries.drain() {
        stats.total += 1;
        let Some(mut entry) = entry else {
            continue;
        };
        if entry.chromosome.is_none() || entry.position.is_none() {
            if let Some((chrom, pos)) = pos_map.get(&rsid) {
                entry.chromosome = Some(chrom.clone());
                entry.position = Some(*pos);
            }
        }
        let (chrom, pos) = match (entry.chromosome.clone(), entry.position) {
            (Some(chrom), Some(pos)) => (chrom, pos),
            _ => {
                stats.skipped_missing_pos += 1;
                continue;
            }
        };
        if !is_entry_usable(&entry) {
            stats.skipped_unusable += 1;
            continue;
        }
        let rsid_int = match rsid.trim_start_matches("rs").parse::<i64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let reference = ReferenceVariant {
            rsid: rsid_int,
            chromosome: chrom,
            position: pos,
            reference: entry.reference.clone(),
            alternates: entry.alternates.clone(),
        };

        let existing_entry = existing.get(&reference.rsid);
        let is_same = existing_entry.map(|existing| {
            existing.chromosome == reference.chromosome
                && existing.position == reference.position
                && existing.reference == reference.reference
                && existing.alternates == reference.alternates
        });

        if is_same == Some(true) {
            stats.unchanged += 1;
            continue;
        }

        if args.apply {
            StatsStore::upsert_user_reference_in_tx(&tx, &reference, entry.source.as_deref())?;
        }
        match existing_entry {
            Some(_) => stats.updated += 1,
            None => stats.inserted += 1,
        }
    }

    if args.apply {
        tx.commit()?;
    }

    println!(
        "✅ Sync complete (total {}, inserted {}, updated {}, unchanged {}, skipped missing pos {}, skipped unusable {})",
        stats.total,
        stats.inserted,
        stats.updated,
        stats.unchanged,
        stats.skipped_missing_pos,
        stats.skipped_unusable
    );
    if !args.apply {
        println!("(dry-run) Use --apply to write changes.");
    }
    Ok(())
}

fn load_existing_map(store: &StatsStore) -> Result<HashMap<i64, ReferenceVariant>> {
    let references = store.all_references_with_overrides()?;
    let mut map = HashMap::with_capacity(references.len());
    for reference in references {
        map.insert(reference.rsid, reference);
    }
    Ok(map)
}

fn load_missing_positions(path: &Path) -> Result<HashMap<String, (String, i64)>> {
    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let mut reader = BufReader::new(file);
    let mut buffer = String::new();
    let mut header: Option<Vec<String>> = None;
    let mut map = HashMap::new();
    while reader.read_line(&mut buffer)? > 0 {
        let line = buffer.trim().to_string();
        buffer.clear();
        if line.is_empty() {
            continue;
        }
        let parts = line.split('\t').collect::<Vec<_>>();
        if header.is_none() {
            header = Some(parts.iter().map(|v| v.to_lowercase()).collect());
            continue;
        }
        let header = header.as_ref().unwrap();
        let rsid = get_field(&parts, header, "rsid").unwrap_or_default();
        let chrom = get_field(&parts, header, "chrom").unwrap_or_default();
        let pos = get_field(&parts, header, "pos")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        if rsid.is_empty() || chrom.is_empty() || pos == 0 {
            continue;
        }
        map.insert(rsid.to_string(), (chrom.to_string(), pos));
    }
    Ok(map)
}

fn get_field<'a>(parts: &'a [&str], header: &[String], name: &str) -> Option<&'a str> {
    let idx = header.iter().position(|h| h == name)?;
    parts.get(idx).copied()
}

fn is_entry_usable(entry: &crate::rsid_cache::CacheEntry) -> bool {
    let reference = entry.reference.trim();
    if reference.is_empty() || reference == "-" || reference == "." {
        return false;
    }
    let alternates = entry
        .alternates
        .split(',')
        .map(|alt| alt.trim())
        .collect::<Vec<_>>();
    if alternates.is_empty() {
        return false;
    }
    if alternates
        .iter()
        .any(|alt| alt.is_empty() || *alt == "-" || *alt == ".")
    {
        return false;
    }
    true
}
