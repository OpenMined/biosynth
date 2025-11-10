use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use walkdir::WalkDir;

pub fn collect_input_files(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    if inputs.is_empty() {
        bail!("Provide at least one --input path");
    }

    let mut files = Vec::new();
    for input in inputs {
        if input.is_file() {
            files.push(canonicalize_path(input)?);
            continue;
        }

        if input.is_dir() {
            for entry in WalkDir::new(input).into_iter().filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                let path = entry.path();
                if is_candidate_file(path) {
                    files.push(canonicalize_path(path)?);
                }
            }
            continue;
        }

        bail!("Input path {:?} is not a file or directory", input);
    }

    files.sort();
    files.dedup();
    Ok(files)
}

fn canonicalize_path<P: AsRef<Path>>(path: P) -> Result<PathBuf> {
    let path_ref = path.as_ref();
    match fs::canonicalize(path_ref) {
        Ok(resolved) => Ok(resolved),
        Err(_) => Ok(path_ref.to_path_buf()),
    }
}

fn is_candidate_file(path: &Path) -> bool {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext_lower = ext.to_lowercase();
        return matches!(ext_lower.as_str(), "txt" | "tsv" | "csv");
    }
    true
}
