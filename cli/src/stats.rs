use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, Transaction};
use serde::Serialize;

use crate::genotype::{FileMetadata, ParseSummary, VariantRecord};

#[derive(Debug, Clone)]
pub struct ReferenceVariant {
    pub rsid: i64,
    pub chromosome: String,
    pub position: i64,
    pub reference: String,
    pub alternates: String,
}

#[derive(Debug, Clone)]
pub struct StatsStore {
    sqlite_path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct SummaryReport {
    pub files_processed: usize,
    pub total_variants: u64,
    pub skipped_rows: u64,
    pub unique_rsids: u64,
    pub formats_seen: Vec<CategoryCount>,
    pub builds_seen: Vec<CategoryCount>,
    pub sqlite_path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct CategoryCount {
    pub value: Option<String>,
    pub count: u64,
}

impl StatsStore {
    pub fn connect(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).with_context(|| format!("Create {:?}", parent))?;
            }
        }
        let conn =
            Connection::open(path).with_context(|| format!("Open database at {:?}", path))?;
        configure_connection(&conn)?;
        init_schema(&conn)?;
        Ok(Self {
            sqlite_path: path.to_path_buf(),
        })
    }

    pub fn open_connection(&self) -> Result<Connection> {
        let conn = Connection::open(&self.sqlite_path)
            .with_context(|| format!("Open database at {:?}", self.sqlite_path))?;
        configure_connection(&conn)?;
        Ok(conn)
    }

    pub fn has_file(&self, _path: &Path) -> Result<bool> {
        Ok(false)
    }

    pub fn record_variant_in_tx(
        _tx: &Transaction<'_>,
        _variant: &VariantRecord,
        _metadata: &FileMetadata,
    ) -> Result<()> {
        Ok(())
    }

    pub fn record_file(
        &self,
        _conn: &Connection,
        _metadata: &FileMetadata,
        _summary: &ParseSummary,
        _duration: Duration,
        _path: &Path,
    ) -> Result<()> {
        Ok(())
    }

    pub fn upsert_reference_in_tx(
        tx: &Transaction<'_>,
        reference: &ReferenceVariant,
    ) -> Result<()> {
        tx.execute(
            "INSERT INTO rsid_reference (rsid, chromosome, position, reference, alternates)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(rsid) DO UPDATE SET
                chromosome=excluded.chromosome,
                position=excluded.position,
                reference=excluded.reference,
                alternates=excluded.alternates",
            params![
                reference.rsid,
                reference.chromosome,
                reference.position,
                reference.reference,
                reference.alternates,
            ],
        )?;
        Ok(())
    }

    pub fn summary(&self) -> Result<SummaryReport> {
        let conn = self.open_connection()?;
        let unique_rsids: i64 = conn
            .query_row("SELECT COUNT(*) FROM rsid_reference", [], |row| row.get(0))
            .unwrap_or(0);

        let formats_seen = self.collect_category_counts(
            &conn,
            "SELECT f.name, COUNT(rr.rsid)
             FROM formats f
             LEFT JOIN rsid_reference rr ON rr.format_id = f.id
             GROUP BY f.id
             ORDER BY COUNT(rr.rsid) DESC",
        )?;
        let builds_seen = self.collect_category_counts(
            &conn,
            "SELECT genome_build, COUNT(*) FROM formats GROUP BY genome_build ORDER BY COUNT(*) DESC",
        )?;
        let total_variants = formats_seen.iter().map(|entry| entry.count).sum();

        Ok(SummaryReport {
            files_processed: 0,
            total_variants,
            skipped_rows: 0,
            unique_rsids: unique_rsids as u64,
            formats_seen,
            builds_seen,
            sqlite_path: self.sqlite_path.clone(),
        })
    }

    pub fn all_references(&self, limit: Option<usize>) -> Result<Vec<ReferenceVariant>> {
        let conn = self.open_connection()?;
        let mut base_query = String::from(
            "SELECT rsid, chromosome, position, reference, alternates
             FROM rsid_reference
             ORDER BY chromosome, position",
        );
        let mut stmt = if limit.is_some() {
            base_query.push_str(" LIMIT ?1");
            conn.prepare(&base_query)?
        } else {
            conn.prepare(&base_query)?
        };
        let mut rows = if let Some(limit) = limit {
            stmt.query([limit as i64])?
        } else {
            stmt.query([])?
        };
        let mut references = Vec::new();
        while let Some(row) = rows.next()? {
            references.push(ReferenceVariant {
                rsid: row.get(0)?,
                chromosome: row.get(1)?,
                position: row.get(2)?,
                reference: row.get(3)?,
                alternates: row.get(4)?,
            });
        }
        Ok(references)
    }

    fn collect_category_counts(
        &self,
        conn: &Connection,
        query: &str,
    ) -> Result<Vec<CategoryCount>> {
        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let value: Option<String> = row.get(0)?;
            let count: i64 = row.get(1)?;
            results.push(CategoryCount {
                value,
                count: count as u64,
            });
        }
        Ok(results)
    }
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS formats (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            genome_build TEXT
        );
        CREATE TABLE IF NOT EXISTS rsid_reference (
            rsid INTEGER PRIMARY KEY,
            format_id INTEGER NOT NULL DEFAULT 1,
            chromosome TEXT NOT NULL,
            position INTEGER NOT NULL,
            reference TEXT NOT NULL,
            alternates TEXT NOT NULL,
            FOREIGN KEY(format_id) REFERENCES formats(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_rsid_reference_format ON rsid_reference(format_id);
        "#,
    )?;
    seed_formats(conn)?;
    Ok(())
}

fn seed_formats(conn: &Connection) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO formats (id, name, genome_build) VALUES (?1, ?2, ?3)",
        params![1_i64, "dynamic_dna", "GRCh38"],
    )?;
    Ok(())
}

fn configure_connection(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}
