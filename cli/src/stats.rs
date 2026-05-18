use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags, Transaction};
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
    read_only: bool,
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
            read_only: false,
        })
    }

    pub fn connect_read_only(path: &Path) -> Result<Self> {
        let conn = open_read_only_connection(path)?;
        configure_connection_read_only(&conn)?;
        Ok(Self {
            sqlite_path: path.to_path_buf(),
            read_only: true,
        })
    }

    pub fn open_connection(&self) -> Result<Connection> {
        let conn = if self.read_only {
            open_read_only_connection(&self.sqlite_path)?
        } else {
            Connection::open(&self.sqlite_path)
                .with_context(|| format!("Open database at {:?}", self.sqlite_path))?
        };
        if self.read_only {
            configure_connection_read_only(&conn)?;
        } else {
            configure_connection(&conn)?;
        }
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

        let non_rsids: i64 = conn
            .query_row("SELECT COUNT(*) FROM grch38_non_rsids", [], |row| row.get(0))
            .unwrap_or(0);
        let formats_seen = vec![
            CategoryCount {
                value: Some("rsid_reference".to_string()),
                count: unique_rsids as u64,
            },
            CategoryCount {
                value: Some("grch38_non_rsids".to_string()),
                count: non_rsids as u64,
            },
        ];
        let total_variants = (unique_rsids + non_rsids) as u64;
        let builds_seen = vec![CategoryCount {
            value: Some(GENOME_BUILD.to_string()),
            count: total_variants,
        }];

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

    pub fn all_references_with_overrides(&self) -> Result<Vec<ReferenceVariant>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(
            "SELECT rsid, chromosome, position, reference, alternates
             FROM rsid_reference_user
             UNION ALL
             SELECT rsid, chromosome, position, reference, alternates
             FROM rsid_reference
             WHERE rsid NOT IN (SELECT rsid FROM rsid_reference_user)
             ORDER BY rsid",
        )?;
        let mut rows = stmt.query([])?;
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

    pub fn all_non_rsids(&self) -> Result<Vec<ReferenceVariant>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(
            "SELECT rsid, chromosome, position, reference, alternates
             FROM grch38_non_rsids",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(ReferenceVariant {
                rsid: row.get(0)?,
                chromosome: row.get(1)?,
                position: row.get(2)?,
                reference: row.get(3)?,
                alternates: row.get(4)?,
            });
        }
        Ok(out)
    }

    pub fn known_rsids(&self) -> Result<HashSet<i64>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(
            "SELECT rsid FROM rsid_reference
             UNION
             SELECT rsid FROM rsid_reference_user",
        )?;
        let mut rows = stmt.query([])?;
        let mut rsids = HashSet::new();
        while let Some(row) = rows.next()? {
            let rsid: i64 = row.get(0)?;
            rsids.insert(rsid);
        }
        Ok(rsids)
    }

    pub fn upsert_user_reference_in_tx(
        tx: &Transaction<'_>,
        reference: &ReferenceVariant,
        source: Option<&str>,
    ) -> Result<()> {
        tx.execute(
            "INSERT INTO rsid_reference_user (rsid, chromosome, position, reference, alternates, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(rsid) DO UPDATE SET
                chromosome=excluded.chromosome,
                position=excluded.position,
                reference=excluded.reference,
                alternates=excluded.alternates,
                source=excluded.source",
            params![
                reference.rsid,
                reference.chromosome,
                reference.position,
                reference.reference,
                reference.alternates,
                source,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_non_rsid_in_tx(
        tx: &Transaction<'_>,
        snp_name: &str,
        reference: &ReferenceVariant,
        source: &str,
        note: Option<&str>,
    ) -> Result<()> {
        tx.execute(
            "INSERT INTO grch38_non_rsids
                (snp_name, rsid, chromosome, position, reference, alternates, source, note)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(snp_name) DO UPDATE SET
                rsid=excluded.rsid,
                chromosome=excluded.chromosome,
                position=excluded.position,
                reference=excluded.reference,
                alternates=excluded.alternates,
                source=excluded.source,
                note=excluded.note",
            params![
                snp_name,
                reference.rsid,
                reference.chromosome,
                reference.position,
                reference.reference,
                reference.alternates,
                source,
                note,
            ],
        )?;
        Ok(())
    }

    /// Resolve a marker by genomic position. Used by the Illumina/Carigenetics
    /// path for probes that carry no rs id in the file: user overrides win,
    /// then the Ensembl-resolved non-rsid table, then the base reference.
    pub fn resolve_by_position(
        &self,
        chromosome: &str,
        position: i64,
    ) -> Result<Option<ReferenceVariant>> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare(
            "SELECT rsid, chromosome, position, reference, alternates FROM (
                 SELECT rsid, chromosome, position, reference, alternates, 0 AS pri
                   FROM rsid_reference_user WHERE chromosome=?1 AND position=?2
                 UNION ALL
                 SELECT rsid, chromosome, position, reference, alternates, 1 AS pri
                   FROM grch38_non_rsids WHERE chromosome=?1 AND position=?2
                 UNION ALL
                 SELECT rsid, chromosome, position, reference, alternates, 2 AS pri
                   FROM rsid_reference WHERE chromosome=?1 AND position=?2
             ) ORDER BY pri LIMIT 1",
        )?;
        let mut rows = stmt.query(params![chromosome, position])?;
        if let Some(row) = rows.next()? {
            Ok(Some(ReferenceVariant {
                rsid: row.get(0)?,
                chromosome: row.get(1)?,
                position: row.get(2)?,
                reference: row.get(3)?,
                alternates: row.get(4)?,
            }))
        } else {
            Ok(None)
        }
    }

}

pub const GENOME_BUILD: &str = "GRCh38";

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS rsid_reference (
            rsid INTEGER PRIMARY KEY,
            chromosome TEXT NOT NULL,
            position INTEGER NOT NULL,
            reference TEXT NOT NULL,
            alternates TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rsid_reference_user (
            rsid INTEGER PRIMARY KEY,
            chromosome TEXT NOT NULL,
            position INTEGER NOT NULL,
            reference TEXT NOT NULL,
            alternates TEXT NOT NULL,
            source TEXT
        );
        CREATE TABLE IF NOT EXISTS grch38_non_rsids (
            snp_name TEXT PRIMARY KEY,
            rsid INTEGER NOT NULL,
            chromosome TEXT NOT NULL,
            position INTEGER NOT NULL,
            reference TEXT NOT NULL,
            alternates TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'ensembl_grch38',
            note TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_rsid_reference_pos
            ON rsid_reference(chromosome, position);
        CREATE INDEX IF NOT EXISTS idx_grch38_non_rsids_pos
            ON grch38_non_rsids(chromosome, position);
        CREATE INDEX IF NOT EXISTS idx_grch38_non_rsids_rsid
            ON grch38_non_rsids(rsid);
        "#,
    )?;
    Ok(())
}

fn configure_connection(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}

fn configure_connection_read_only(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "query_only", "ON")?;
    Ok(())
}

fn open_read_only_connection(path: &Path) -> Result<Connection> {
    let uri = read_only_uri(path);
    Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("Open database at {:?} (read-only)", path))
}

fn read_only_uri(path: &Path) -> String {
    let raw = path.to_string_lossy();
    let escaped = raw.replace(' ', "%20");
    format!("file:{escaped}?immutable=1")
}
