use std::collections::{BTreeSet, HashMap};

use anyhow::Result;

const COMMENT_PREFIXES: [&str; 2] = ["#", "//"];

const RSID_ALIASES: &[&str] = &["rsid", "name", "snp", "marker", "id", "snpid"];
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
    "call",
    "calls",
    "yourcode",
    "code",
    "genotypevalue",
    "variation",
];
const ALLELE1_ALIASES: &[&str] = &["allele1", "allelea", "allele_a", "allele1top"];
const ALLELE2_ALIASES: &[&str] = &["allele2", "alleleb", "allele_b", "allele2top"];
const GS_ALIASES: &[&str] = &["gs", "gscore", "genotypescore", "score"];
const BAF_ALIASES: &[&str] = &["baf", "b_allele_freq", "ballelefrequency"];
const LRR_ALIASES: &[&str] = &["lrr", "logrratio", "logr"];

#[derive(Debug, Clone)]
pub struct GenotypeRow {
    pub rsid: String,
    pub chrom: String,
    pub pos: i64,
    pub genotype: Option<String>,
    pub gs: Option<String>,
    pub baf: Option<String>,
    pub lrr: Option<String>,
}

pub enum RowOutcome {
    Parsed(GenotypeRow),
    Skipped,
    Ignored,
}

#[derive(Debug, Clone, Copy)]
pub enum Delimiter {
    Tab,
    Comma,
    Space,
}

pub struct RowParser {
    delimiter: Delimiter,
    header: Option<Vec<String>>,
    comment_header: Option<Vec<String>>,
    alias_map: HashMap<&'static str, BTreeSet<&'static str>>,
    /// Illumina GSGT "Final Report": saw `[Header]`, still before `[Data]`.
    awaiting_data: bool,
    /// This file is in the Illumina GSGT Final Report (Carigenetics) format.
    carigenetics: bool,
}

/// Extract an `rs\d+` id from an Illumina SNP Name
/// (`BOT-rs1135675`, `rs111647200_ilmndup1`, ...). None if there is no rs id.
fn extract_rs(name: &str) -> Option<String> {
    let bytes = name.as_bytes();
    let mut i = 0;
    while i + 2 < bytes.len() {
        let r = bytes[i];
        if (r == b'r' || r == b'R')
            && (bytes[i + 1] == b's' || bytes[i + 1] == b'S')
            && bytes[i + 2].is_ascii_digit()
        {
            let start = i + 2;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            return Some(format!("rs{}", &name[start..end]));
        }
        i += 1;
    }
    None
}

impl RowParser {
    pub fn new(delimiter: Delimiter) -> Self {
        let mut alias_map: HashMap<&'static str, BTreeSet<&'static str>> = HashMap::new();
        alias_map.insert("rsid", RSID_ALIASES.iter().cloned().collect());
        alias_map.insert("chromosome", CHROM_ALIASES.iter().cloned().collect());
        alias_map.insert("position", POSITION_ALIASES.iter().cloned().collect());
        alias_map.insert("genotype", GENOTYPE_ALIASES.iter().cloned().collect());
        alias_map.insert("allele1", ALLELE1_ALIASES.iter().cloned().collect());
        alias_map.insert("allele2", ALLELE2_ALIASES.iter().cloned().collect());
        alias_map.insert("gs", GS_ALIASES.iter().cloned().collect());
        alias_map.insert("baf", BAF_ALIASES.iter().cloned().collect());
        alias_map.insert("lrr", LRR_ALIASES.iter().cloned().collect());
        Self {
            delimiter,
            header: None,
            comment_header: None,
            alias_map,
            awaiting_data: false,
            carigenetics: false,
        }
    }

    pub fn consume_line(&mut self, line: &str) -> Result<RowOutcome> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(RowOutcome::Ignored);
        }

        let trimmed = strip_bom(trimmed);

        // Illumina GSGT Final Report sections.
        if trimmed.eq_ignore_ascii_case("[Header]") {
            self.carigenetics = true;
            self.awaiting_data = true;
            return Ok(RowOutcome::Ignored);
        }
        if trimmed.eq_ignore_ascii_case("[Data]") {
            self.awaiting_data = false;
            return Ok(RowOutcome::Ignored);
        }
        if self.awaiting_data {
            // metadata line between [Header] and [Data]
            return Ok(RowOutcome::Ignored);
        }
        // First line after [Data] is the column header.
        if self.carigenetics && self.header.is_none() {
            self.header = Some(self.parse_fields(trimmed));
            return Ok(RowOutcome::Ignored);
        }

        if let Some(prefix) = COMMENT_PREFIXES
            .iter()
            .find(|prefix| trimmed.starts_with(**prefix))
        {
            let candidate = trimmed.trim_start_matches(prefix).trim();
            if candidate.is_empty() {
                return Ok(RowOutcome::Ignored);
            }
            let fields = self.parse_fields(candidate);
            if self.looks_like_header(&fields) {
                self.comment_header = Some(fields);
            }
            return Ok(RowOutcome::Ignored);
        }

        let fields = self.parse_fields(strip_bom(line));
        if fields.is_empty() {
            return Ok(RowOutcome::Ignored);
        }

        if self.header.is_none() {
            if self.looks_like_header(&fields) {
                self.header = Some(fields);
                return Ok(RowOutcome::Ignored);
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

        if self.carigenetics {
            // SNP Name -> rsid via rs\d+ extraction; non-rs probes keep an
            // empty rsid and resolve downstream by (chrom,pos).
            let snp_name = row_map
                .get("snpname")
                .map(|s| s.as_str())
                .unwrap_or_default();
            let rsid = extract_rs(snp_name).unwrap_or_default();
            let chrom = match row_map.get("chr").or_else(|| row_map.get("chromosome")) {
                Some(v) if !v.is_empty() && v != "0" => v.clone(),
                _ => return Ok(RowOutcome::Skipped),
            };
            let pos = match row_map
                .get("position")
                .and_then(|v| v.parse::<i64>().ok())
            {
                Some(p) if p > 0 => p,
                _ => return Ok(RowOutcome::Skipped),
            };
            let a1 = row_map.get("allele1plus").map(|s| s.as_str()).unwrap_or("");
            let a2 = row_map.get("allele2plus").map(|s| s.as_str()).unwrap_or("");
            let genotype = if a1.is_empty() && a2.is_empty() {
                None
            } else if a1 == "-" || a2 == "-" {
                Some("--".to_string())
            } else {
                Some(format!("{}{}", a1, a2))
            };
            return Ok(RowOutcome::Parsed(GenotypeRow {
                rsid,
                chrom,
                pos,
                genotype,
                gs: None,
                baf: None,
                lrr: None,
            }));
        }

        let rsid = match self.lookup(&row_map, "rsid") {
            Some(value) if !value.is_empty() => value,
            _ => return Ok(RowOutcome::Skipped),
        };
        let chrom = match self.lookup(&row_map, "chromosome") {
            Some(value) if !value.is_empty() => value,
            _ => return Ok(RowOutcome::Skipped),
        };
        let pos = match self
            .lookup(&row_map, "position")
            .and_then(|value| value.parse::<i64>().ok())
        {
            Some(pos) => pos,
            None => return Ok(RowOutcome::Skipped),
        };

        let genotype = self.lookup(&row_map, "genotype").filter(|v| !v.is_empty());
        let allele1 = self.lookup(&row_map, "allele1").filter(|v| !v.is_empty());
        let allele2 = self.lookup(&row_map, "allele2").filter(|v| !v.is_empty());
        let combined_genotype = match (genotype, allele1, allele2) {
            (Some(gt), _, _) => Some(gt),
            (None, Some(a1), Some(a2)) => Some(format!("{}{}", a1, a2)),
            (None, Some(a1), None) => Some(a1),
            (None, None, Some(a2)) => Some(a2),
            (None, None, None) => None,
        };

        Ok(RowOutcome::Parsed(GenotypeRow {
            rsid,
            chrom,
            pos,
            genotype: combined_genotype,
            gs: self.lookup(&row_map, "gs"),
            baf: self.lookup(&row_map, "baf"),
            lrr: self.lookup(&row_map, "lrr"),
        }))
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
        let base = vec![
            "rsid",
            "chromosome",
            "position",
            "genotype",
            "gs",
            "baf",
            "lrr",
        ];
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

pub fn detect_delimiter(lines: &[String]) -> Delimiter {
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

fn strip_bom(value: &str) -> &str {
    value.strip_prefix('\u{feff}').unwrap_or(value)
}
