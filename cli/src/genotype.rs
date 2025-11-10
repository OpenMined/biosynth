use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{bail, Context, Result};

const LOOKAHEAD_LINES: usize = 2048;
const COMMENT_PREFIXES: [&str; 2] = ["#", "//"];
const RSID_ALIASES: &[&str] = &["rsid", "name", "snp", "marker", "id"];
const CHROM_ALIASES: &[&str] = &["chromosome", "chr", "chrom"];
const POSITION_ALIASES: &[&str] = &[
    "position",
    "pos",
    "coordinate",
    "basepairposition",
    "basepair",
];
const GENOTYPE_ALIASES: &[&str] = &[
    "genotype",
    "gt",
    "result",
    "results",
    "result1",
    "call",
    "calls",
    "yourcode",
    "code",
    "genotypevalue",
    "variation",
];
const ALLELE1_ALIASES: &[&str] = &["allele1", "allelea", "allele_a", "allele1top"];
const ALLELE2_ALIASES: &[&str] = &["allele2", "alleleb", "allele_b", "allele2top"];

#[derive(Debug, Clone)]
pub struct FileMetadata {}

#[derive(Debug, Clone)]
pub struct VariantRecord {
    pub _rsid: String,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ParseSummary {
    pub variant_count: usize,
    pub skipped_rows: usize,
}

#[derive(Debug, Clone)]
pub struct ParsedFile {
    pub metadata: FileMetadata,
    pub summary: ParseSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeOutcome {
    Parsed,
    Skipped,
    Ignored,
}

pub fn process_file<F>(path: &Path, mut on_variant: F) -> Result<ParsedFile>
where
    F: FnMut(&VariantRecord, &FileMetadata) -> Result<()>,
{
    let file = File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
    let mut reader = BufReader::new(file);
    let mut buffered_lines: Vec<String> = Vec::new();
    let mut buffer = String::new();

    while buffered_lines.len() < LOOKAHEAD_LINES {
        buffer.clear();
        let bytes = reader.read_line(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        buffered_lines.push(buffer.clone());
    }

    if buffered_lines.is_empty() {
        bail!("File {:?} is empty", path);
    }

    let metadata = detect_metadata(&buffered_lines, path);
    let metadata_for_handler = metadata.clone();
    let delimiter = detect_delimiter(&buffered_lines);
    let mut parser = LineParser::new(delimiter);
    let mut summary = ParseSummary::default();
    let mut handler = |record: &VariantRecord| on_variant(record, &metadata_for_handler);

    // Process buffered lines first.
    for line in &buffered_lines {
        match parser.consume_line(line, &mut handler)? {
            ConsumeOutcome::Parsed => summary.variant_count += 1,
            ConsumeOutcome::Skipped => summary.skipped_rows += 1,
            ConsumeOutcome::Ignored => {}
        }
    }

    // Process the rest of the file from the buffered reader.
    buffer.clear();
    loop {
        buffer.clear();
        let bytes = reader.read_line(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        match parser.consume_line(&buffer, &mut handler)? {
            ConsumeOutcome::Parsed => summary.variant_count += 1,
            ConsumeOutcome::Skipped => summary.skipped_rows += 1,
            ConsumeOutcome::Ignored => {}
        }
    }

    Ok(ParsedFile { metadata, summary })
}

fn detect_metadata(_lines: &[String], _path: &Path) -> FileMetadata {
    FileMetadata {}
}

#[derive(Debug, Clone, Copy)]
enum Delimiter {
    Tab,
    Comma,
    Space,
}

fn detect_delimiter(lines: &[String]) -> Delimiter {
    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty()
            || COMMENT_PREFIXES
                .iter()
                .any(|prefix| trimmed.starts_with(prefix))
        {
            continue;
        }
        if line.contains('\t') {
            return Delimiter::Tab;
        }
        if line.contains(',') {
            return Delimiter::Comma;
        }
        let whitespace_fields = trimmed.split_whitespace().collect::<Vec<_>>();
        if whitespace_fields.len() > 1 {
            return Delimiter::Space;
        }
    }
    Delimiter::Tab
}

struct LineParser {
    delimiter: Delimiter,
    header: Option<Vec<String>>,
    comment_header: Option<Vec<String>>,
    alias_map: HashMap<&'static str, BTreeSet<&'static str>>,
}

impl LineParser {
    fn new(delimiter: Delimiter) -> Self {
        let mut alias_map: HashMap<&'static str, BTreeSet<&'static str>> = HashMap::new();
        alias_map.insert("rsid", RSID_ALIASES.iter().cloned().collect());
        alias_map.insert("chromosome", CHROM_ALIASES.iter().cloned().collect());
        alias_map.insert("position", POSITION_ALIASES.iter().cloned().collect());
        alias_map.insert("genotype", GENOTYPE_ALIASES.iter().cloned().collect());
        alias_map.insert("allele1", ALLELE1_ALIASES.iter().cloned().collect());
        alias_map.insert("allele2", ALLELE2_ALIASES.iter().cloned().collect());
        Self {
            delimiter,
            header: None,
            comment_header: None,
            alias_map,
        }
    }

    fn consume_line<F>(&mut self, line: &str, handler: &mut F) -> Result<ConsumeOutcome>
    where
        F: FnMut(&VariantRecord) -> Result<()>,
    {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(ConsumeOutcome::Ignored);
        }

        if let Some(prefix) = COMMENT_PREFIXES
            .iter()
            .find(|prefix| trimmed.starts_with(**prefix))
        {
            let candidate = trimmed.trim_start_matches(prefix).trim();
            if candidate.is_empty() {
                return Ok(ConsumeOutcome::Ignored);
            }
            let fields = self.parse_fields(candidate);
            if self.looks_like_header(&fields) {
                self.comment_header = Some(fields);
            }
            return Ok(ConsumeOutcome::Ignored);
        }

        let fields = self.parse_fields(line);
        if fields.is_empty() {
            return Ok(ConsumeOutcome::Ignored);
        }

        if self.header.is_none() {
            if self.looks_like_header(&fields) {
                self.header = Some(fields);
                return Ok(ConsumeOutcome::Ignored);
            }

            if let Some(header) = self.comment_header.take() {
                self.header = Some(header);
            } else {
                let default_header = self.default_header(fields.len());
                self.header = Some(default_header);
            }
        }

        let header = self.header.as_ref().expect("header must be set");
        let mut row_map: HashMap<String, String> = HashMap::new();
        for (idx, value) in fields.into_iter().enumerate() {
            if idx >= header.len() {
                continue;
            }
            row_map.insert(normalize_name(&header[idx]), strip_inline_comment(&value));
        }

        let rsid = self.lookup(&row_map, "rsid");
        let chromosome = self.lookup(&row_map, "chromosome");
        let position = self.lookup(&row_map, "position");
        let genotype_value = self.lookup(&row_map, "genotype");

        let rsid = match rsid {
            Some(value) if !value.is_empty() => value,
            _ => return Ok(ConsumeOutcome::Skipped),
        };

        if chromosome.is_none() || chromosome.as_ref().is_none_or(|v| v.is_empty()) {
            return Ok(ConsumeOutcome::Skipped);
        }

        if position.and_then(|v| v.parse::<i64>().ok()).is_none() {
            return Ok(ConsumeOutcome::Skipped);
        }

        if genotype_value.is_none() {
            let allele1 = self.lookup(&row_map, "allele1").unwrap_or_default();
            let allele2 = self.lookup(&row_map, "allele2").unwrap_or_default();
            if allele1.is_empty() && allele2.is_empty() {
                return Ok(ConsumeOutcome::Skipped);
            }
        }

        let record = VariantRecord { _rsid: rsid };

        handler(&record)?;
        Ok(ConsumeOutcome::Parsed)
    }

    fn lookup(&self, row_map: &HashMap<String, String>, key: &str) -> Option<String> {
        let aliases = self.alias_map.get(key)?;
        for alias in aliases {
            let normalized_key = normalize_name(alias);
            if let Some(value) = row_map.get(&normalized_key) {
                if !value.is_empty() {
                    return Some(value.clone());
                }
            }
        }
        None
    }

    fn parse_fields(&self, line: &str) -> Vec<String> {
        match self.delimiter {
            Delimiter::Tab => line
                .split('\t')
                .map(|field| field.trim().to_string())
                .collect(),
            Delimiter::Space => line
                .split_whitespace()
                .map(|field| field.trim().to_string())
                .collect(),
            Delimiter::Comma => split_csv_line(line),
        }
    }

    fn looks_like_header(&self, fields: &[String]) -> bool {
        if fields.is_empty() {
            return false;
        }
        let first = normalize_name(&fields[0]);
        self.alias_map
            .get("rsid")
            .map(|aliases| aliases.contains(first.as_str()))
            .unwrap_or(false)
    }

    fn default_header(&self, field_count: usize) -> Vec<String> {
        let base = vec!["rsid", "chromosome", "position", "genotype"];
        if field_count <= base.len() {
            base[..field_count].iter().map(|s| s.to_string()).collect()
        } else {
            let mut header = base.into_iter().map(|s| s.to_string()).collect::<Vec<_>>();
            for idx in 0..(field_count - header.len()) {
                header.push(format!("extra_{}", idx));
            }
            header
        }
    }
}

fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                fields.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    fields.push(current.trim().to_string());

    fields
}

fn normalize_name(name: &str) -> String {
    name.chars()
        .filter(|c| !matches!(c, ' ' | '\t' | '-' | '_'))
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn strip_inline_comment(value: &str) -> String {
    let mut trimmed = value.trim();
    if let Some(idx) = trimmed.find('#') {
        trimmed = &trimmed[..idx];
    }
    if let Some(idx) = trimmed.find("//") {
        trimmed = &trimmed[..idx];
    }
    trimmed.trim().to_string()
}
