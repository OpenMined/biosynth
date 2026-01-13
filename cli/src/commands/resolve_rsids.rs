use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use noodles::core::Region;
use noodles::vcf;
use noodles::vcf::io::indexed_reader::Builder as IndexedReaderBuilder;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use reqwest::blocking::Client;
use serde_json::Value;

use crate::rsid_cache::{default_cache_path, normalize_rsid, CacheEntry, RsidCache};
use crate::ResolveRsidsArgs;

const REFSEQ_GRCH38_MAP: &[(&str, &str)] = &[
    ("1", "NC_000001.11"),
    ("2", "NC_000002.12"),
    ("3", "NC_000003.12"),
    ("4", "NC_000004.12"),
    ("5", "NC_000005.10"),
    ("6", "NC_000006.12"),
    ("7", "NC_000007.14"),
    ("8", "NC_000008.11"),
    ("9", "NC_000009.12"),
    ("10", "NC_000010.11"),
    ("11", "NC_000011.10"),
    ("12", "NC_000012.12"),
    ("13", "NC_000013.11"),
    ("14", "NC_000014.9"),
    ("15", "NC_000015.10"),
    ("16", "NC_000016.10"),
    ("17", "NC_000017.11"),
    ("18", "NC_000018.10"),
    ("19", "NC_000019.10"),
    ("20", "NC_000020.11"),
    ("21", "NC_000021.9"),
    ("22", "NC_000022.11"),
    ("X", "NC_000023.11"),
    ("23", "NC_000023.11"),
    ("Y", "NC_000024.10"),
    ("24", "NC_000024.10"),
    ("MT", "NC_012920.1"),
    ("M", "NC_012920.1"),
    ("25", "NC_012920.1"),
    ("26", "NC_012920.1"),
];

#[derive(Debug, Clone)]
struct MissingRecord {
    rsid: String,
    chrom: String,
    pos: i64,
}

pub fn run_resolve_rsids(args: ResolveRsidsArgs) -> Result<()> {
    if !args.missing_file.exists() {
        bail!("Missing rsid file not found: {:?}", args.missing_file);
    }

    let cache_path = args.cache.clone().unwrap_or_else(default_cache_path);
    let mut cache = RsidCache::load(&cache_path)?;

    if let Some(local_file) = &args.local_file {
        import_local_cache(local_file, &mut cache)?;
    }

    let mut records = load_missing_records(&args.missing_file)?;
    if records.is_empty() {
        println!("No missing rsids found in {}", args.missing_file.display());
        return Ok(());
    }

    let mut targets: Vec<MissingRecord> = Vec::new();
    let mut seen = HashSet::new();
    for record in records.drain(..) {
        let rsid = normalize_rsid(&record.rsid);
        if seen.insert(rsid.clone()) {
            targets.push(MissingRecord {
                rsid,
                chrom: record.chrom,
                pos: record.pos,
            });
        }
    }

    let mut unresolved: Vec<MissingRecord> = Vec::new();
    let mut resolved_count = 0usize;
    for target in &targets {
        if let Some(entry) = cache.get(&target.rsid) {
            if !entry.reference.is_empty() && !entry.alternates.is_empty() {
                resolved_count += 1;
                continue;
            }
        }
        unresolved.push(target.clone());
    }

    if args.cache_only {
        for target in &unresolved {
            cache.mark_unresolved(&target.rsid);
        }
        cache.save(&cache_path)?;
        println!(
            "✅ Cache-only mode: {} resolved, {} unresolved",
            resolved_count,
            unresolved.len()
        );
        return Ok(());
    }

    let mut updated = 0usize;
    if let Some(dbsnp) = &args.dbsnp {
        eprintln!(
            "dbSNP lookup: {} rsids against {}",
            unresolved.len(),
            dbsnp.display()
        );
        let dbsnp_results = resolve_with_dbsnp(dbsnp, &unresolved)?;
        for (rsid, entry) in dbsnp_results {
            cache.insert(&rsid, entry);
            updated += 1;
        }
        unresolved.retain(|record| cache.get(&record.rsid).is_none());
    }

    if args.web && !unresolved.is_empty() {
        eprintln!(
            "Ensembl lookup: {} rsids (threads {})",
            unresolved.len(),
            args.threads
        );
        let web_results = resolve_with_ensembl(&unresolved, args.threads, args.retry)?;
        for (rsid, entry) in web_results {
            cache.insert(&rsid, entry);
            updated += 1;
        }
        unresolved.retain(|record| cache.get(&record.rsid).is_none());
    }

    for record in &unresolved {
        cache.mark_unresolved(&record.rsid);
    }

    cache.save(&cache_path)?;

    println!(
        "✅ Updated cache {} (resolved {}, unresolved {})",
        cache_path.display(),
        resolved_count + updated,
        unresolved.len()
    );
    Ok(())
}

fn load_missing_records(path: &Path) -> Result<Vec<MissingRecord>> {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext == "jsonl" {
        return load_missing_jsonl(path);
    }
    load_missing_tsv(path)
}

fn load_missing_tsv(path: &Path) -> Result<Vec<MissingRecord>> {
    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let mut reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut buffer = String::new();
    let mut header: Option<Vec<String>> = None;
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
        rows.push(MissingRecord {
            rsid: rsid.to_string(),
            chrom: chrom.to_string(),
            pos,
        });
    }
    Ok(rows)
}

fn load_missing_jsonl(path: &Path) -> Result<Vec<MissingRecord>> {
    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)?;
        let rsid = value.get("rsid").and_then(|v| v.as_str()).unwrap_or("");
        let chrom = value.get("chrom").and_then(|v| v.as_str()).unwrap_or("");
        let pos = value.get("pos").and_then(|v| v.as_i64()).unwrap_or(0);
        if rsid.is_empty() || chrom.is_empty() || pos == 0 {
            continue;
        }
        rows.push(MissingRecord {
            rsid: rsid.to_string(),
            chrom: chrom.to_string(),
            pos,
        });
    }
    Ok(rows)
}

fn get_field<'a>(parts: &'a [&str], header: &[String], name: &str) -> Option<&'a str> {
    let idx = header.iter().position(|h| h == name)?;
    parts.get(idx).copied()
}

fn resolve_with_dbsnp(
    path: &PathBuf,
    targets: &[MissingRecord],
) -> Result<Vec<(String, CacheEntry)>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let mut reader = IndexedReaderBuilder::default()
        .build_from_path(path)
        .with_context(|| format!("Open dbSNP VCF {:?}", path))?;
    let header = reader.read_header()?;

    let mut results = Vec::new();
    let mut processed = 0usize;
    for target in targets {
        let contig = map_contig(&target.chrom);
        let start = target.pos.saturating_sub(5);
        let end = target.pos + 5;
        let region = format!("{}:{}-{}", contig, start.max(1), end);
        let region: Region = region.parse()?;
        let mut found = None;

        if let Ok(query) = reader.query(&header, &region) {
            for record in query.records() {
                let record = record?;
                if record_matches_rsid(&record, &target.rsid) {
                    let reference = record.reference_bases().to_string();
                    let alternates = record.alternate_bases().as_ref().to_string();
                    if !reference.is_empty() && alternates != "." {
                        found = Some(CacheEntry {
                            reference,
                            alternates,
                            chromosome: Some(target.chrom.clone()),
                            position: Some(target.pos),
                            source: Some("dbsnp".to_string()),
                        });
                    }
                    break;
                }
            }
        }

        if let Some(entry) = found {
            results.push((target.rsid.clone(), entry));
        }
        processed += 1;
        if processed.is_multiple_of(1000) || processed == targets.len() {
            eprintln!("dbSNP progress: {processed}/{}", targets.len());
        }
    }
    Ok(results)
}

fn record_matches_rsid(record: &vcf::Record, target: &str) -> bool {
    let ids_binding = record.ids();
    let ids = ids_binding.as_ref();
    if ids.is_empty() || ids == "." {
        return false;
    }
    ids.split(';')
        .any(|id| normalize_rsid(id) == normalize_rsid(target))
}

fn resolve_with_ensembl(
    targets: &[MissingRecord],
    threads: usize,
    retries: usize,
) -> Result<Vec<(String, CacheEntry)>> {
    let client = Arc::new(
        Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("Build HTTP client")?,
    );
    let pool = ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .build()
        .context("build ensembl thread pool")?;

    let processed = Arc::new(AtomicUsize::new(0));
    let total = targets.len();
    let results = pool.install(|| {
        targets
            .par_iter()
            .filter_map(|target| {
                let entry = fetch_ensembl_ref_alt(&client, &target.rsid, retries);
                let count = processed.fetch_add(1, Ordering::Relaxed) + 1;
                if count.is_multiple_of(50) || count == total {
                    eprintln!("Ensembl progress: {count}/{total}");
                }
                entry.map(|(reference, alternates)| {
                    (
                        target.rsid.clone(),
                        CacheEntry {
                            reference,
                            alternates,
                            chromosome: Some(target.chrom.clone()),
                            position: Some(target.pos),
                            source: Some("ensembl".to_string()),
                        },
                    )
                })
            })
            .collect::<Vec<_>>()
    });

    Ok(results)
}

fn fetch_ensembl_ref_alt(client: &Client, rsid: &str, retries: usize) -> Option<(String, String)> {
    let url = format!("https://rest.ensembl.org/variation/human/{rsid}?");
    for attempt in 0..=retries {
        let response = client
            .get(&url)
            .header("Content-Type", "application/json")
            .send();
        match response {
            Ok(resp) if resp.status().is_success() => {
                let json: Value = resp.json().ok()?;
                let mappings = json.get("mappings")?.as_array()?;
                for mapping in mappings {
                    if mapping.get("assembly_name")?.as_str()? == "GRCh38" {
                        let allele_string = mapping.get("allele_string")?.as_str()?;
                        if let Some((reference, alternates)) = parse_allele_string(allele_string) {
                            return Some((reference, alternates));
                        }
                    }
                }
                return None;
            }
            _ => {
                if attempt < retries {
                    thread::sleep(Duration::from_millis(250));
                    continue;
                }
                return None;
            }
        }
    }
    None
}

fn parse_allele_string(raw: &str) -> Option<(String, String)> {
    let parts = raw.split('/').collect::<Vec<_>>();
    if parts.len() < 2 {
        return None;
    }
    let reference = parts[0].to_string();
    let alternates = parts[1..].join(",");
    if reference.is_empty() || alternates.is_empty() {
        return None;
    }
    Some((reference, alternates))
}

fn map_contig(chrom: &str) -> String {
    let normalized = chrom
        .trim()
        .trim_start_matches("chr")
        .trim_start_matches("CHR");
    for (key, value) in REFSEQ_GRCH38_MAP {
        if key.eq_ignore_ascii_case(normalized) {
            return (*value).to_string();
        }
    }
    normalized.to_string()
}

fn import_local_cache(path: &Path, cache: &mut RsidCache) -> Result<()> {
    if !path.exists() {
        bail!("Local cache file not found: {:?}", path);
    }
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if ext == "json" || ext == "jsonl" {
        let mut imported = RsidCache::load(path)?;
        for (rsid, entry) in imported.entries.drain() {
            if let Some(entry) = entry {
                cache.insert(&rsid, entry);
            }
        }
        return Ok(());
    }

    let file = File::open(path).with_context(|| format!("Open {:?}", path))?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let parts = line.split('\t').collect::<Vec<_>>();
        if parts.len() < 3 {
            continue;
        }
        let rsid = normalize_rsid(parts[0]);
        let reference = parts[1].to_string();
        let alternates = parts[2].to_string();
        let chrom = parts.get(3).map(|v| v.to_string());
        let pos = parts.get(4).and_then(|v| v.parse::<i64>().ok());
        cache.insert(
            &rsid,
            CacheEntry {
                reference,
                alternates,
                chromosome: chrom,
                position: pos,
                source: Some("local".to_string()),
            },
        );
    }
    Ok(())
}
