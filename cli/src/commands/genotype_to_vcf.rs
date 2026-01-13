use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use crate::download::ensure_reference_db;
use crate::stats::{ReferenceVariant, StatsStore};
use crate::GenotypeToVcfArgs;

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

pub fn run_genotype_to_vcf(args: GenotypeToVcfArgs) -> Result<()> {
    if !args.input.exists() {
        bail!("Input genotype file not found: {:?}", args.input);
    }

    let sqlite_path = ensure_reference_db(Some(&args.sqlite))?;
    let store = StatsStore::connect(&sqlite_path)?;
    let reference_map = load_reference_map(&store)?;

    let output_paths = resolve_output_paths(&args)?;
    if let Some(parent) = output_paths.write_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Create output directory {:?}", parent))?;
        }
    }

    let input = File::open(&args.input).with_context(|| format!("Open {:?}", args.input))?;
    let mut reader = BufReader::new(input);
    let mut buffered_lines = Vec::new();
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
        bail!("Input file {:?} is empty", args.input);
    }

    let delimiter = detect_delimiter(&buffered_lines);
    let mut parser = RowParser::new(delimiter);

    let sample_name = args
        .sample
        .clone()
        .unwrap_or_else(|| default_sample_name(&args.input));
    let missing_log_path = args
        .missing_log
        .clone()
        .unwrap_or_else(|| default_log_path(&output_paths.write_path));

    let mut stats = ConversionStats::default();
    let mut parsed_rows: Vec<GenotypeRow> = Vec::new();

    for line in &buffered_lines {
        collect_row(line, &mut parser, &mut parsed_rows, &mut stats)?;
    }

    buffer.clear();
    loop {
        buffer.clear();
        let bytes = reader.read_line(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        collect_row(&buffer, &mut parser, &mut parsed_rows, &mut stats)?;
    }

    let thread_count = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("build genotype-to-vcf thread pool")?;

    let results = pool.install(|| {
        parsed_rows
            .par_iter()
            .map(|row| convert_row(&reference_map, row, args.include_metrics))
            .collect::<Vec<RowResult>>()
    });

    let contigs = collect_contigs(&parsed_rows);
    let mut writer = BufWriter::new(
        File::create(&output_paths.write_path)
            .with_context(|| format!("Create {:?}", output_paths.write_path))?,
    );
    write_vcf_header(&mut writer, &sample_name, &contigs, args.include_metrics)?;

    let mut missing_logger = MissingLogger::new(Some(&missing_log_path))?;
    for result in results {
        stats.add(&result.stats);
        for message in result.logs {
            missing_logger.log(&message)?;
        }
        if let Some(line) = result.line {
            writer.write_all(line.as_bytes())?;
        }
    }

    writer.flush()?;
    missing_logger.flush()?;

    if output_paths.gzip {
        gzip_output(&output_paths.write_path)?;
    }

    println!(
        "✅ Wrote {} VCF rows to {}",
        stats.written_rows,
        output_paths.final_path.display()
    );
    println!(
        "📝 Missing/invalid rows logged to {}",
        missing_log_path.display()
    );
    if stats.skipped_rows > 0
        || stats.invalid_rsid > 0
        || stats.missing_reference > 0
        || stats.missing_genotype > 0
        || stats.unknown_allele > 0
    {
        eprintln!(
            "⚠️ Skipped rows: {}, invalid rsids: {}, missing refs: {}, missing genotypes: {}, unknown alleles: {}",
            stats.skipped_rows,
            stats.invalid_rsid,
            stats.missing_reference,
            stats.missing_genotype,
            stats.unknown_allele
        );
    }

    Ok(())
}

fn collect_row(
    line: &str,
    parser: &mut RowParser,
    rows: &mut Vec<GenotypeRow>,
    stats: &mut ConversionStats,
) -> Result<()> {
    match parser.consume_line(line)? {
        RowOutcome::Parsed(row) => {
            stats.parsed_rows += 1;
            rows.push(row);
        }
        RowOutcome::Skipped => stats.skipped_rows += 1,
        RowOutcome::Ignored => {}
    }
    Ok(())
}

fn convert_row(
    reference_map: &HashMap<i64, ReferenceVariant>,
    row: &GenotypeRow,
    include_metrics: bool,
) -> RowResult {
    let mut result = RowResult::default();
    let rsid_label = row.rsid.trim();
    if rsid_label.is_empty() {
        result.stats.invalid_rsid += 1;
        result.logs.push("missing_rsid".to_string());
        return result;
    }

    let rsid_value = strip_rsid_prefix(rsid_label);
    let rsid_int: i64 = match rsid_value.parse() {
        Ok(value) => value,
        Err(_) => {
            result.stats.invalid_rsid += 1;
            result.logs.push(format!("invalid_rsid: {rsid_label}"));
            return result;
        }
    };

    let reference = match reference_map.get(&rsid_int) {
        Some(reference) => reference,
        None => {
            result.stats.missing_reference += 1;
            result.logs.push(format!("missing_reference: {rsid_label}"));
            return result;
        }
    };

    let ref_allele = normalize_sequence(&reference.reference);
    if ref_allele.is_empty() {
        result.stats.missing_reference += 1;
        result.logs.push(format!("empty_reference: {rsid_label}"));
        return result;
    }

    let mut alt_list = parse_alternates(&reference.alternates);

    let alleles = row.genotype.as_deref().and_then(parse_genotype_alleles);
    if let Some((a1, a2)) = alleles.as_ref() {
        append_observed_alt(&mut alt_list, &ref_allele, a1);
        append_observed_alt(&mut alt_list, &ref_allele, a2);
    }

    let gt = match alleles {
        Some((a1, a2)) => {
            let kind = classify_variant(&ref_allele, &alt_list);
            let idx1 = map_allele_index(&a1, &ref_allele, &alt_list, kind);
            let idx2 = map_allele_index(&a2, &ref_allele, &alt_list, kind);
            match (idx1, idx2) {
                (Some(i1), Some(i2)) => format!("{}/{}", i1.min(i2), i1.max(i2)),
                _ => {
                    result.stats.unknown_allele += 1;
                    result
                        .logs
                        .push(format!("unknown_allele: {rsid_label} {a1}/{a2}"));
                    "./.".to_string()
                }
            }
        }
        None => {
            result.stats.missing_genotype += 1;
            result.logs.push(format!("missing_genotype: {rsid_label}"));
            "./.".to_string()
        }
    };

    let alt_field = if alt_list.is_empty() {
        ".".to_string()
    } else {
        alt_list.join(",")
    };

    let (format_field, sample_field) = if include_metrics {
        let gs = row.gs.clone().unwrap_or_else(|| ".".to_string());
        let baf = row.baf.clone().unwrap_or_else(|| ".".to_string());
        let lrr = row.lrr.clone().unwrap_or_else(|| ".".to_string());
        (
            "GT:GS:BAF:LRR".to_string(),
            format!("{gt}:{gs}:{baf}:{lrr}"),
        )
    } else {
        ("GT".to_string(), gt.clone())
    };

    result.stats.written_rows += 1;
    result.line = Some(format!(
        "{}\t{}\t{}\t{}\t{}\t.\tPASS\t.\t{}\t{}\n",
        row.chrom, row.pos, rsid_label, ref_allele, alt_field, format_field, sample_field
    ));
    result
}

fn write_vcf_header(
    writer: &mut BufWriter<File>,
    sample_name: &str,
    contigs: &[String],
    include_metrics: bool,
) -> Result<()> {
    writeln!(writer, "##fileformat=VCFv4.2")?;
    writeln!(writer, "##source=bvs genotype-to-vcf")?;
    for contig in contigs {
        writeln!(writer, "##contig=<ID={}>", contig)?;
    }
    writeln!(
        writer,
        "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">"
    )?;
    if include_metrics {
        writeln!(
            writer,
            "##FORMAT=<ID=GS,Number=1,Type=Float,Description=\"Genotype score\">"
        )?;
        writeln!(
            writer,
            "##FORMAT=<ID=BAF,Number=1,Type=Float,Description=\"B-allele frequency\">"
        )?;
        writeln!(
            writer,
            "##FORMAT=<ID=LRR,Number=1,Type=Float,Description=\"Log R ratio\">"
        )?;
    }
    writeln!(
        writer,
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t{}",
        sample_name
    )?;
    Ok(())
}

fn resolve_output_paths(args: &GenotypeToVcfArgs) -> Result<OutputPaths> {
    let output = match &args.output {
        Some(path) => path.clone(),
        None => default_output_path(&args.input, args.gzip),
    };

    let output_is_gz = output
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext == "gz")
        .unwrap_or(false);
    let gzip = args.gzip || output_is_gz;

    if args.gzip && args.output.is_some() && !output_is_gz {
        bail!("--gzip requires --output to end with .gz");
    }

    let write_path = if gzip {
        output.with_extension("")
    } else {
        output.clone()
    };

    Ok(OutputPaths {
        write_path: write_path.clone(),
        final_path: if gzip { output } else { write_path },
        gzip,
    })
}

fn default_output_path(input: &Path, gzip: bool) -> PathBuf {
    let base = input.with_extension("vcf");
    if gzip {
        base.with_extension("vcf.gz")
    } else {
        base
    }
}

fn default_sample_name(input: &Path) -> String {
    input
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(|stem| stem.replace(' ', "_"))
        .filter(|stem| !stem.is_empty())
        .unwrap_or_else(|| "SAMPLE".to_string())
}

fn default_log_path(output_path: &Path) -> PathBuf {
    output_path.with_extension("vcf.log")
}

fn gzip_output(path: &Path) -> Result<()> {
    let status = Command::new("gzip")
        .arg("-f")
        .arg(path)
        .status()
        .with_context(|| "Failed to execute gzip")?;
    if !status.success() {
        bail!("gzip exited with status {}", status);
    }
    Ok(())
}

struct OutputPaths {
    write_path: PathBuf,
    final_path: PathBuf,
    gzip: bool,
}

#[derive(Default, Clone, Copy)]
struct ConversionStats {
    parsed_rows: usize,
    written_rows: usize,
    skipped_rows: usize,
    invalid_rsid: usize,
    missing_reference: usize,
    missing_genotype: usize,
    unknown_allele: usize,
}

impl ConversionStats {
    fn add(&mut self, other: &ConversionStats) {
        self.parsed_rows += other.parsed_rows;
        self.written_rows += other.written_rows;
        self.skipped_rows += other.skipped_rows;
        self.invalid_rsid += other.invalid_rsid;
        self.missing_reference += other.missing_reference;
        self.missing_genotype += other.missing_genotype;
        self.unknown_allele += other.unknown_allele;
    }
}

#[derive(Default)]
struct RowResult {
    line: Option<String>,
    stats: ConversionStats,
    logs: Vec<String>,
}

struct MissingLogger {
    writer: Box<dyn Write>,
}

impl MissingLogger {
    fn new(path: Option<&PathBuf>) -> Result<Self> {
        let writer: Box<dyn Write> = match path {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)
                            .with_context(|| format!("Create {:?}", parent))?;
                    }
                }
                Box::new(BufWriter::new(
                    File::create(path).with_context(|| format!("Create {:?}", path))?,
                ))
            }
            None => Box::new(std::io::stderr()),
        };
        Ok(Self { writer })
    }

    fn log(&mut self, message: &str) -> Result<()> {
        writeln!(self.writer, "{message}")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("flush missing log")
    }
}

#[derive(Debug, Clone)]
struct GenotypeRow {
    rsid: String,
    chrom: String,
    pos: i64,
    genotype: Option<String>,
    gs: Option<String>,
    baf: Option<String>,
    lrr: Option<String>,
}

enum RowOutcome {
    Parsed(GenotypeRow),
    Skipped,
    Ignored,
}

#[derive(Debug, Clone, Copy)]
enum Delimiter {
    Tab,
    Comma,
    Space,
}

struct RowParser {
    delimiter: Delimiter,
    header: Option<Vec<String>>,
    comment_header: Option<Vec<String>>,
    alias_map: HashMap<&'static str, BTreeSet<&'static str>>,
}

impl RowParser {
    fn new(delimiter: Delimiter) -> Self {
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
        }
    }

    fn consume_line(&mut self, line: &str) -> Result<RowOutcome> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
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

        let fields = self.parse_fields(line);
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

fn parse_genotype_alleles(raw: &str) -> Option<(String, String)> {
    if is_missing_genotype(raw) {
        return None;
    }
    let trimmed = raw.trim();
    if trimmed.contains('/') || trimmed.contains('|') {
        let parts: Vec<&str> = trimmed.split(['/', '|']).collect();
        if parts.len() == 2 {
            let a1 = normalize_allele(parts[0])?;
            let a2 = normalize_allele(parts[1])?;
            return Some((a1, a2));
        }
    }

    let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
    let mut chars = compact.chars();
    let first = chars.next()?;
    let second = chars.next().unwrap_or(first);
    let a1 = normalize_allele(&first.to_string())?;
    let a2 = normalize_allele(&second.to_string())?;
    Some((a1, a2))
}

fn normalize_allele(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_ascii_uppercase();
    if upper == "I" || upper == "D" {
        return Some(upper);
    }
    if upper.chars().all(|c| matches!(c, 'A' | 'C' | 'G' | 'T')) {
        return Some(upper);
    }
    None
}

fn is_missing_genotype(raw: &str) -> bool {
    let trimmed = raw.trim().to_ascii_uppercase();
    if trimmed.is_empty() {
        return true;
    }
    if matches!(trimmed.as_str(), "." | "./." | ".|." | "NA" | "N/A") {
        return true;
    }
    trimmed
        .chars()
        .all(|c| matches!(c, 'N' | '-' | '0' | '.' | '/' | '|'))
}

fn normalize_sequence(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn parse_alternates(alternates: &str) -> Vec<String> {
    let mut results = Vec::new();
    for alt in alternates.split(',') {
        let alt = normalize_sequence(alt);
        if alt.is_empty() || alt == "." {
            continue;
        }
        if !results.iter().any(|existing| existing == &alt) {
            results.push(alt);
        }
    }
    results
}

fn append_observed_alt(alts: &mut Vec<String>, reference: &str, allele: &str) {
    if allele == "I" || allele == "D" {
        return;
    }
    if allele == reference {
        return;
    }
    if !alts.iter().any(|alt| alt == allele) {
        alts.push(allele.to_string());
    }
}

#[derive(Clone, Copy, Debug)]
enum VariantKind {
    Snp,
    Mnv,
    Insertion,
    Deletion,
    Mixed,
    Unknown,
}

fn classify_variant(reference: &str, alts: &[String]) -> VariantKind {
    if alts.is_empty() {
        return VariantKind::Unknown;
    }
    let ref_len = reference.len();
    let mut longer = false;
    let mut shorter = false;
    let mut equal = false;
    for alt in alts {
        match alt.len().cmp(&ref_len) {
            std::cmp::Ordering::Greater => longer = true,
            std::cmp::Ordering::Less => shorter = true,
            std::cmp::Ordering::Equal => equal = true,
        }
    }
    let flags = longer as u8 + shorter as u8 + equal as u8;
    if flags > 1 {
        return VariantKind::Mixed;
    }
    if longer {
        return VariantKind::Insertion;
    }
    if shorter {
        return VariantKind::Deletion;
    }
    if ref_len == 1 {
        VariantKind::Snp
    } else {
        VariantKind::Mnv
    }
}

fn map_allele_index(
    allele: &str,
    reference: &str,
    alts: &[String],
    kind: VariantKind,
) -> Option<usize> {
    if allele == reference {
        return Some(0);
    }
    if allele == "I" || allele == "D" {
        return map_indel_symbol(allele, reference, alts, kind);
    }
    alts.iter().position(|alt| alt == allele).map(|idx| idx + 1)
}

fn map_indel_symbol(
    symbol: &str,
    reference: &str,
    alts: &[String],
    kind: VariantKind,
) -> Option<usize> {
    let ref_len = reference.len();
    match kind {
        VariantKind::Insertion => {
            if symbol == "D" {
                return Some(0);
            }
            alts.iter()
                .position(|alt| alt.len() > ref_len)
                .map(|idx| idx + 1)
        }
        VariantKind::Deletion => {
            if symbol == "I" {
                return Some(0);
            }
            alts.iter()
                .position(|alt| alt.len() < ref_len)
                .map(|idx| idx + 1)
        }
        _ => None,
    }
}

fn strip_rsid_prefix(value: &str) -> &str {
    match value.get(0..2) {
        Some(prefix) if prefix.eq_ignore_ascii_case("rs") => &value[2..],
        _ => value,
    }
}

fn load_reference_map(store: &StatsStore) -> Result<HashMap<i64, ReferenceVariant>> {
    let references = store.all_references(None)?;
    let mut map = HashMap::with_capacity(references.len());
    for reference in references {
        map.insert(reference.rsid, reference);
    }
    Ok(map)
}

fn collect_contigs(rows: &[GenotypeRow]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for row in rows {
        let contig = row.chrom.trim();
        if !contig.is_empty() {
            set.insert(contig.to_string());
        }
    }
    let mut contigs: Vec<String> = set.into_iter().collect();
    contigs.sort_by(|a, b| compare_contigs(a, b));
    contigs
}

fn compare_contigs(a: &str, b: &str) -> std::cmp::Ordering {
    let a_key = contig_sort_key(a);
    let b_key = contig_sort_key(b);
    a_key
        .0
        .cmp(&b_key.0)
        .then_with(|| a_key.1.cmp(&b_key.1))
        .then_with(|| a.cmp(b))
}

fn contig_sort_key(value: &str) -> (u8, u32) {
    let trimmed = value.trim();
    let normalized = trimmed
        .strip_prefix("chr")
        .or_else(|| trimmed.strip_prefix("CHR"));
    let core = normalized.unwrap_or(trimmed);
    if let Ok(num) = core.parse::<u32>() {
        return (0, num);
    }
    match core.to_ascii_uppercase().as_str() {
        "X" => (1, 23),
        "Y" => (1, 24),
        "MT" | "M" => (1, 25),
        _ => (2, 0),
    }
}
