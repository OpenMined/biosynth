use anyhow::{Context, Result};
use csv::ReaderBuilder;
use serde::Deserialize;

use crate::stats::{ReferenceVariant, StatsStore};
use crate::ReferenceLoadArgs;

#[derive(Debug, Deserialize)]
struct LookupRow {
    query_rsid: String,
    query_chrom: String,
    query_pos: String,
    ref_pos: String,
    #[serde(rename = "ref")]
    reference: String,
    alt: String,
    status: String,
}

pub fn run_reference_load(args: ReferenceLoadArgs) -> Result<()> {
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

    for row in reader.deserialize::<LookupRow>() {
        let row = row.with_context(|| "parse lookup row")?;
        if row.status.to_lowercase() != "exact" {
            skipped += 1;
            continue;
        }
        let pos = row
            .ref_pos
            .parse::<i64>()
            .or_else(|_| row.query_pos.parse::<i64>())
            .with_context(|| format!("parse position for {}", row.query_rsid))?;
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
        StatsStore::upsert_reference_in_tx(&tx, &reference)?;
        imported += 1;
    }

    tx.commit()?;
    println!(
        "📚 Loaded {} reference rows into {} ({} skipped)",
        imported,
        args.sqlite.display(),
        skipped
    );
    Ok(())
}
