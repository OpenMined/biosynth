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

/// Best-effort available memory in bytes, used to auto-size RAM budgets.
/// Honors container limits (cgroups v2/v1) first, then host memory
/// (`/proc/meminfo` on Linux, `sysctl hw.memsize` on macOS). None if unknown.
pub fn available_memory_bytes() -> Option<u64> {
    // cgroups v2 (container memory limit)
    if let Ok(value) = fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = value.trim();
        if trimmed != "max" {
            if let Ok(bytes) = trimmed.parse::<u64>() {
                if bytes > 0 {
                    return Some(bytes);
                }
            }
        }
    }
    // cgroups v1
    if let Ok(value) = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(bytes) = value.trim().parse::<u64>() {
            // v1 uses a huge sentinel when unlimited; ignore implausibly large values.
            if bytes > 0 && bytes < (1u64 << 62) {
                return Some(bytes);
            }
        }
    }
    // Linux host: MemAvailable
    if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if let Some(rest) = line.strip_prefix("MemAvailable:") {
                if let Some(kb) = rest.split_whitespace().next().and_then(|v| v.parse::<u64>().ok())
                {
                    return Some(kb * 1024);
                }
            }
        }
    }
    // macOS: total physical memory via sysctl
    if let Ok(output) = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
    {
        if let Ok(text) = String::from_utf8(output.stdout) {
            if let Ok(bytes) = text.trim().parse::<u64>() {
                if bytes > 0 {
                    return Some(bytes);
                }
            }
        }
    }
    None
}
