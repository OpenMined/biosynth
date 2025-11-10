use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use reqwest::blocking::Client;

const GITHUB_RAW_BASE: &str = "https://raw.githubusercontent.com/openmined/biosynth/main";
const DATA_DIR: &str = "data";

pub fn ensure_reference_db(custom_path: Option<&PathBuf>) -> Result<PathBuf> {
    let data_dir = PathBuf::from(DATA_DIR);
    let data_db_path = data_dir.join("genostats.sqlite");

    if let Some(path) = custom_path {
        if path.exists() {
            return Ok(path.clone());
        }
    }

    if cfg!(debug_assertions) {
        if data_db_path.exists() {
            return Ok(data_db_path);
        }
        if let Some(path) = custom_path {
            return Ok(path.clone());
        }
        return Ok(PathBuf::from("genostats.sqlite"));
    }

    if !data_dir.exists() {
        fs::create_dir_all(&data_dir)
            .with_context(|| format!("Create data directory {:?}", data_dir))?;
    }

    if !data_db_path.exists() {
        println!("📥 Downloading reference database from GitHub...");
        download_file("data/genostats.sqlite", &data_db_path)?;
        println!("✅ Downloaded to {:?}", data_db_path);
    }

    Ok(data_db_path)
}

fn download_file(remote_filename: &str, local_path: &Path) -> Result<()> {
    let url = format!("{}/{}", GITHUB_RAW_BASE, remote_filename);
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .context("Build HTTP client")?;

    let response = client
        .get(&url)
        .send()
        .with_context(|| format!("Download from {}", url))?;

    if !response.status().is_success() {
        anyhow::bail!("HTTP {} for {}", response.status(), url);
    }

    let bytes = response
        .bytes()
        .with_context(|| format!("Read response from {}", url))?;

    let mut file =
        fs::File::create(local_path).with_context(|| format!("Create {:?}", local_path))?;

    file.write_all(&bytes)
        .with_context(|| format!("Write to {:?}", local_path))?;

    Ok(())
}
