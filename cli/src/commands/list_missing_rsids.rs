use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::genotype_reader::{detect_delimiter, open_text_reader, RowOutcome, RowParser};
use crate::rsid_cache::normalize_rsid;
use crate::stats::StatsStore;
use crate::util::collect_input_files;
use crate::ListMissingRsidsArgs;

#[derive(Debug, Clone)]
struct MissingEntry {
    rsid: String,
    chrom: String,
    pos: i64,
    count: u64,
}

pub fn run_list_missing_rsids(args: ListMissingRsidsArgs) -> Result<()> {
    if args.inputs.is_empty() {
        bail!("Provide at least one --input path");
    }
    let inputs = collect_input_files(&args.inputs)?;
    if inputs.is_empty() {
        bail!("No genotype files discovered in the provided inputs");
    }

    let store = StatsStore::connect(&args.sqlite)?;
    let known = store.known_rsids()?;

    let mut missing: HashMap<String, MissingEntry> = HashMap::new();
    for path in &inputs {
        collect_missing_for_file(path, &known, &mut missing)?;
    }

    let mut entries = missing.into_values().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.rsid.cmp(&b.rsid));

    if let Some(parent) = args.missing_file.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| format!("Create {:?}", parent))?;
        }
    }

    match args.format.to_lowercase().as_str() {
        "tsv" => write_missing_tsv(&args.missing_file, &entries)?,
        "jsonl" => write_missing_jsonl(&args.missing_file, &entries)?,
        _ => bail!("Unsupported --format (use tsv or jsonl)"),
    }

    println!(
        "✅ Wrote {} missing rsids to {}",
        entries.len(),
        args.missing_file.display()
    );
    Ok(())
}

fn collect_missing_for_file(
    path: &Path,
    known: &std::collections::HashSet<i64>,
    missing: &mut HashMap<String, MissingEntry>,
) -> Result<()> {
    let mut reader = open_text_reader(path)?;
    let mut buffered_lines = Vec::new();
    let mut buffer = String::new();

    while buffered_lines.len() < 2048 {
        buffer.clear();
        let bytes = reader.read_line(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        buffered_lines.push(buffer.clone());
    }

    if buffered_lines.is_empty() {
        return Ok(());
    }

    let delimiter = detect_delimiter(&buffered_lines);
    let mut parser = RowParser::new(delimiter);

    for line in &buffered_lines {
        handle_row(line, &mut parser, known, missing)?;
    }

    buffer.clear();
    loop {
        buffer.clear();
        let bytes = reader.read_line(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        handle_row(&buffer, &mut parser, known, missing)?;
    }

    Ok(())
}

fn handle_row(
    line: &str,
    parser: &mut RowParser,
    known: &std::collections::HashSet<i64>,
    missing: &mut HashMap<String, MissingEntry>,
) -> Result<()> {
    match parser.consume_line(line)? {
        RowOutcome::Parsed(row) => {
            let rsid_norm = normalize_rsid(&row.rsid);
            let rsid_int = rsid_norm.trim_start_matches("rs").parse::<i64>();
            if let Ok(rsid_int) = rsid_int {
                if !known.contains(&rsid_int) {
                    let entry = missing.entry(rsid_norm.clone()).or_insert(MissingEntry {
                        rsid: rsid_norm.clone(),
                        chrom: row.chrom.clone(),
                        pos: row.pos,
                        count: 0,
                    });
                    entry.count += 1;
                }
            }
        }
        RowOutcome::Skipped | RowOutcome::Ignored => {}
    }
    Ok(())
}

fn write_missing_tsv(path: &PathBuf, entries: &[MissingEntry]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("Create {:?}", path))?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "rsid\tchrom\tpos\tcount")?;
    for entry in entries {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}",
            entry.rsid, entry.chrom, entry.pos, entry.count
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_missing_jsonl(path: &PathBuf, entries: &[MissingEntry]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("Create {:?}", path))?;
    let mut writer = BufWriter::new(file);
    for entry in entries {
        let payload = serde_json::json!({
            "rsid": entry.rsid,
            "chrom": entry.chrom,
            "pos": entry.pos,
            "count": entry.count,
        });
        serde_json::to_writer(&mut writer, &payload)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}
