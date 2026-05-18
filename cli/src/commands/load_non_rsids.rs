use anyhow::{Context, Result};
use csv::ReaderBuilder;
use serde::Deserialize;

use crate::stats::{ReferenceVariant, StatsStore};
use crate::LoadNonRsidsArgs;

#[derive(Debug, Deserialize)]
struct ResolvedRow {
    query_rsid: String,
    query_chrom: String,
    query_pos: String,
    ref_pos: String,
    #[serde(rename = "ref")]
    reference: String,
    alt: String,
    status: String,
    #[serde(default)]
    note: String,
    snp_name: String,
}

pub fn run_load_non_rsids(args: LoadNonRsidsArgs) -> Result<()> {
    if !args.lookup.exists() {
        anyhow::bail!("Lookup CSV not found: {:?}", args.lookup);
    }

    let store = StatsStore::connect(&args.sqlite)?;
    let mut reader = ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(&args.lookup)
        .with_context(|| format!("Read lookup CSV {:?}", args.lookup))?;

    let mut conn = store.open_connection()?;
    let tx = conn.transaction()?;

    let mut imported = 0usize;
    let mut skipped = 0usize;

    for row in reader.deserialize::<ResolvedRow>() {
        let row = row.with_context(|| "parse resolved row")?;
        if row.status.to_lowercase() != "exact" || row.snp_name.trim().is_empty() {
            skipped += 1;
            continue;
        }
        let pos = row
            .ref_pos
            .parse::<i64>()
            .or_else(|_| row.query_pos.parse::<i64>())
            .with_context(|| format!("parse position for {}", row.snp_name))?;
        let rsid_int = row
            .query_rsid
            .trim()
            .trim_start_matches("rs")
            .parse::<i64>()
            .with_context(|| format!("parse rsid {}", row.query_rsid))?;
        let reference = ReferenceVariant {
            rsid: rsid_int,
            chromosome: row.query_chrom,
            position: pos,
            reference: row.reference,
            alternates: row.alt,
        };
        let note = (!row.note.trim().is_empty()).then(|| row.note.clone());
        StatsStore::upsert_non_rsid_in_tx(
            &tx,
            row.snp_name.trim(),
            &reference,
            &args.source,
            note.as_deref(),
        )?;
        imported += 1;
    }

    tx.commit()?;
    println!(
        "🧬 Loaded {} non-rsid markers into {} ({} skipped, source={})",
        imported,
        args.sqlite.display(),
        skipped,
        args.source
    );
    Ok(())
}
