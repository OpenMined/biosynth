use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use reqwest::header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED};
use serde::{Deserialize, Serialize};

const GITHUB_RAW_BASE: &str = "https://raw.githubusercontent.com/openmined/biosynth/main";
const DEFAULT_DB_PATH: &str = "data/genostats.sqlite";
const CACHE_SUBDIR: &str = "biosynth";
const DB_FILENAME: &str = "genostats.sqlite";

pub fn ensure_reference_db(custom_path: Option<&PathBuf>, force_download: bool) -> Result<PathBuf> {
    let baked_in_db_path = PathBuf::from("/app/data/genostats.sqlite");

    if let Some(path) = custom_path {
        if path.exists() {
            return Ok(path.clone());
        }
        if !path.is_absolute() && baked_in_db_path.exists() {
            return Ok(baked_in_db_path);
        }
    }

    let default_path = PathBuf::from(DEFAULT_DB_PATH);
    let use_default = custom_path
        .map(|path| *path == default_path)
        .unwrap_or(true);
    let target_path = if use_default {
        cache_db_path().unwrap_or_else(|| default_path.clone())
    } else {
        custom_path.cloned().unwrap_or(default_path)
    };

    if baked_in_db_path.exists() && use_default && !force_download {
        return Ok(baked_in_db_path);
    }

    if force_download || !target_path.exists() {
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("Create directory {:?}", parent))?;
        }
        println!("📥 Downloading reference database from GitHub...");
        let downloaded =
            download_file_with_cache("data/genostats.sqlite", &target_path, force_download)?;
        if downloaded {
            println!("✅ Downloaded to {:?}", target_path);
        } else {
            println!("✅ Reference database unchanged (cache hit).");
        }
        return Ok(target_path);
    }

    let _ = download_file_with_cache("data/genostats.sqlite", &target_path, false)?;
    Ok(target_path)
}

fn cache_db_path() -> Option<PathBuf> {
    let base = if let Ok(dir) = env::var("XDG_CACHE_HOME") {
        if dir.trim().is_empty() {
            None
        } else {
            Some(PathBuf::from(dir))
        }
    } else if let Ok(home) = env::var("HOME") {
        Some(PathBuf::from(home).join(".cache"))
    } else if let Ok(home) = env::var("USERPROFILE") {
        Some(PathBuf::from(home).join(".cache"))
    } else {
        None
    };

    base.map(|dir| dir.join(CACHE_SUBDIR).join(DB_FILENAME))
}

fn download_file_with_cache(
    remote_filename: &str,
    local_path: &Path,
    force_download: bool,
) -> Result<bool> {
    let url = format!("{}/{}", GITHUB_RAW_BASE, remote_filename);
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .context("Build HTTP client")?;

    let mut request = client.get(&url);
    if !force_download && local_path.exists() {
        if let Some(meta) = read_cache_metadata(local_path).ok().flatten() {
            if let Some(etag) = meta.etag {
                request = request.header(IF_NONE_MATCH, etag);
            }
            if let Some(last_modified) = meta.last_modified {
                request = request.header(IF_MODIFIED_SINCE, last_modified);
            }
        }
    }

    let response = request
        .send()
        .with_context(|| format!("Download from {}", url))?;

    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(false);
    }
    if !response.status().is_success() {
        anyhow::bail!("HTTP {} for {}", response.status(), url);
    }

    let headers = response.headers().clone();
    let bytes = response
        .bytes()
        .with_context(|| format!("Read response from {}", url))?;

    let mut file =
        fs::File::create(local_path).with_context(|| format!("Create {:?}", local_path))?;

    file.write_all(&bytes)
        .with_context(|| format!("Write to {:?}", local_path))?;
    let meta = CacheMetadata::from_headers(&headers);
    write_cache_metadata(local_path, meta)?;

    Ok(true)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheMetadata {
    etag: Option<String>,
    last_modified: Option<String>,
}

impl CacheMetadata {
    fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        let etag = headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string());
        let last_modified = headers
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string());
        Self {
            etag,
            last_modified,
        }
    }
}

fn read_cache_metadata(local_path: &Path) -> Result<Option<CacheMetadata>> {
    let meta_path = metadata_path(local_path);
    if !meta_path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&meta_path)
        .with_context(|| format!("Read cache metadata {:?}", meta_path))?;
    let parsed = serde_json::from_str(&raw)
        .with_context(|| format!("Parse cache metadata {:?}", meta_path))?;
    Ok(Some(parsed))
}

fn write_cache_metadata(local_path: &Path, metadata: CacheMetadata) -> Result<()> {
    let meta_path = metadata_path(local_path);
    let serialized = serde_json::to_string_pretty(&metadata)
        .with_context(|| format!("Serialize cache metadata {:?}", meta_path))?;
    fs::write(&meta_path, serialized)
        .with_context(|| format!("Write cache metadata {:?}", meta_path))?;
    Ok(())
}

fn metadata_path(local_path: &Path) -> PathBuf {
    let base = local_path.to_string_lossy();
    PathBuf::from(format!("{base}.meta.json"))
}
