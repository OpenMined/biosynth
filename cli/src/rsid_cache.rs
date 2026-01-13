use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub reference: String,
    pub alternates: String,
    pub chromosome: Option<String>,
    pub position: Option<i64>,
    pub source: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct RsidCache {
    pub entries: HashMap<String, Option<CacheEntry>>,
}

#[derive(Debug, Serialize)]
struct CacheEntryOut {
    #[serde(rename = "ref")]
    reference: String,
    alt: String,
    chrom: Option<String>,
    pos: Option<i64>,
    source: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CacheValue {
    Array(Vec<String>),
    Object(CacheEntryIn),
    Null,
}

#[derive(Debug, Deserialize)]
struct CacheEntryIn {
    #[serde(rename = "ref")]
    reference: Option<String>,
    alt: Option<String>,
    chrom: Option<String>,
    pos: Option<i64>,
    source: Option<String>,
}

impl RsidCache {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("Read cache file {:?}", path))?;
        let json: serde_json::Value =
            serde_json::from_str(&raw).context("Parse rsid cache JSON")?;

        let mut entries = HashMap::new();
        if let serde_json::Value::Object(map) = json {
            for (key, value) in map {
                let rsid = normalize_rsid(&key);
                let decoded =
                    serde_json::from_value::<CacheValue>(value).unwrap_or(CacheValue::Null);
                let entry = match decoded {
                    CacheValue::Null => None,
                    CacheValue::Array(values) => {
                        if values.len() >= 2 {
                            Some(CacheEntry {
                                reference: values[0].clone(),
                                alternates: values[1].clone(),
                                chromosome: None,
                                position: None,
                                source: None,
                            })
                        } else {
                            None
                        }
                    }
                    CacheValue::Object(obj) => {
                        let reference = obj.reference.unwrap_or_default();
                        let alternates = obj.alt.unwrap_or_default();
                        if reference.is_empty() || alternates.is_empty() {
                            None
                        } else {
                            Some(CacheEntry {
                                reference,
                                alternates,
                                chromosome: obj.chrom,
                                position: obj.pos,
                                source: obj.source,
                            })
                        }
                    }
                };
                entries.insert(rsid, entry);
            }
        }

        Ok(Self { entries })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| format!("Create {:?}", parent))?;
            }
        }
        let mut output = serde_json::Map::new();
        for (rsid, entry) in &self.entries {
            let value = match entry {
                Some(entry) => serde_json::to_value(CacheEntryOut {
                    reference: entry.reference.clone(),
                    alt: entry.alternates.clone(),
                    chrom: entry.chromosome.clone(),
                    pos: entry.position,
                    source: entry.source.clone(),
                })?,
                None => serde_json::Value::Null,
            };
            output.insert(rsid.clone(), value);
        }
        let payload = serde_json::Value::Object(output);
        let mut file = File::create(path).with_context(|| format!("Create {:?}", path))?;
        serde_json::to_writer_pretty(&mut file, &payload)?;
        file.write_all(b"\n")?;
        Ok(())
    }

    pub fn get(&self, rsid: &str) -> Option<&CacheEntry> {
        self.entries
            .get(&normalize_rsid(rsid))
            .and_then(|v| v.as_ref())
    }

    pub fn insert(&mut self, rsid: &str, entry: CacheEntry) {
        self.entries.insert(normalize_rsid(rsid), Some(entry));
    }

    pub fn mark_unresolved(&mut self, rsid: &str) {
        self.entries.insert(normalize_rsid(rsid), None);
    }
}

pub fn normalize_rsid(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed[..2].eq_ignore_ascii_case("rs") {
        format!("rs{}", &trimmed[2..])
    } else {
        format!("rs{}", trimmed)
    }
}

pub fn default_cache_path() -> std::path::PathBuf {
    std::path::PathBuf::from(".rsid_cache.json")
}
