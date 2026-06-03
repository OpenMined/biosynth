use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use crate::download::ensure_reference_db;
use crate::genotype_reader::{detect_delimiter, open_text_reader, RowOutcome, RowParser};
use crate::rsid_cache::{default_cache_path, normalize_rsid, RsidCache};
use crate::stats::{ReferenceVariant, StatsStore};
use crate::util::{collect_input_files, genotype_file_stem};
use crate::GenotypeToVcfArgs;

const LOOKAHEAD_LINES: usize = 2048;
const DEFAULT_GQ: &str = "10";
const DEFAULT_DP: &str = "10";
const MISSING_GENOTYPES: [&str; 4] = ["--", "NN", "00", ".."];
const CONTIG_HEADERS: [&str; 25] = [
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17",
    "18", "19", "20", "21", "22", "X", "Y", "MT",
];

pub fn run_genotype_to_vcf(args: GenotypeToVcfArgs) -> Result<()> {
    if args.inputs.is_empty() {
        bail!("Provide at least one --input path");
    }
    let inputs = collect_input_files(&args.inputs)?;
    if inputs.is_empty() {
        bail!("No genotype files discovered in the provided inputs");
    }
    if inputs.len() > 1 && args.output.is_some() {
        bail!("--output can only be used with a single input file");
    }
    if inputs.len() > 1 && args.sample.is_some() {
        bail!("--sample can only be used with a single input file");
    }
    if inputs.len() > 1 && args.missing_log.is_some() {
        bail!("--missing-log can only be used with a single input file");
    }

    let sqlite_path = resolve_sqlite_path(&args)?;
    let store = if read_only_db_requested() {
        StatsStore::connect_read_only(&sqlite_path)?
    } else {
        StatsStore::connect(&sqlite_path)?
    };
    let reference_map = load_reference_map(&store)?;
    let position_map = load_position_map(&store)?;

    let cache_path = args.cache.clone().unwrap_or_else(default_cache_path);
    let cache = if cache_path.exists() {
        RsidCache::load(&cache_path)?
    } else {
        RsidCache::default()
    };

    for input in inputs {
        let output_paths = resolve_output_paths(&args, &input)?;
        if let Some(parent) = output_paths.write_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Create output directory {:?}", parent))?;
            }
        }

        let sample_name = args
            .sample
            .clone()
            .unwrap_or_else(|| default_sample_name(&input));
        let missing_log_path = args
            .missing_log
            .clone()
            .unwrap_or_else(|| default_log_path(&output_paths.write_path));

        let stats = convert_file(
            &input,
            &output_paths,
            &reference_map,
            &position_map,
            &cache,
            &sample_name,
            &missing_log_path,
            args.include_metrics,
        )?;

        println!(
            "✅ Wrote {} VCF rows to {} (unresolved {})",
            stats.written_rows,
            output_paths.final_path.display(),
            stats.unresolved_rows
        );
        println!(
            "📝 Missing/invalid rows logged to {}",
            missing_log_path.display()
        );
    }

    Ok(())
}

fn read_only_db_requested() -> bool {
    match std::env::var("BVS_READ_ONLY_DB") {
        Ok(value) => matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn resolve_sqlite_path(args: &GenotypeToVcfArgs) -> Result<PathBuf> {
    if read_only_db_requested() {
        let baked_in = PathBuf::from("/app/data/genostats.sqlite");
        if baked_in.exists() {
            return Ok(baked_in);
        }
    }
    ensure_reference_db(Some(&args.sqlite), args.force_download)
}

#[allow(clippy::too_many_arguments)]
fn convert_file(
    input: &Path,
    output_paths: &OutputPaths,
    reference_map: &HashMap<i64, ReferenceVariant>,
    position_map: &HashMap<(String, i64), ReferenceVariant>,
    cache: &RsidCache,
    sample_name: &str,
    missing_log_path: &Path,
    include_metrics: bool,
) -> Result<ConversionStats> {
    let mut reader = open_text_reader(input)?;
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
        bail!("Input file {:?} is empty", input);
    }

    let delimiter = detect_delimiter(&buffered_lines);
    let mut parser = RowParser::new(delimiter);

    let mut stats = ConversionStats::default();
    let mut rows = Vec::new();
    for line in &buffered_lines {
        collect_row(line, &mut parser, &mut rows, &mut stats)?;
    }

    buffer.clear();
    loop {
        buffer.clear();
        let bytes = reader.read_line(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        collect_row(&buffer, &mut parser, &mut rows, &mut stats)?;
    }

    let thread_count = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("build genotype-to-vcf thread pool")?;

    let results = pool.install(|| {
        rows.par_iter()
            .map(|row| convert_row(reference_map, position_map, cache, row, include_metrics))
            .collect::<Vec<RowResult>>()
    });

    let mut writer = BufWriter::new(
        File::create(&output_paths.write_path)
            .with_context(|| format!("Create {:?}", output_paths.write_path))?,
    );
    write_vcf_header(&mut writer, sample_name, include_metrics)?;

    let mut missing_logger = MissingLogger::new(Some(missing_log_path))?;

    // First pass: accumulate stats/logs; gather emittable rows in order.
    #[derive(Clone)]
    struct Emit {
        key: String,
        rsid: String,
        gt: String,
        line: String,
    }
    let mut emits: Vec<Emit> = Vec::with_capacity(results.len());
    for result in results {
        stats.add(&result.stats);
        for message in result.logs {
            missing_logger.log(&message)?;
        }
        if let Some(line) = result.line {
            // Merge identity = same locus AND same variant id. Real
            // duplicate probes (BOT/BOT2/_ilmndup) share chrom+pos+rsid;
            // co-located SNP/indel differ by rsid; placeholder rsids
            // (".") at distinct positions differ by locus.
            let (chrom, pos) = result.locus.clone().unwrap_or_default();
            emits.push(Emit {
                key: format!("{}\t{}\t{}", chrom, pos, result.rsid),
                rsid: result.rsid,
                gt: result.gt.unwrap_or_else(|| "./.".to_string()),
                line,
            });
        }
    }

    // Duplicate-probe merge: collapse all rows sharing an rsid
    // (Illumina BOT-/BOT2-/_ilmndup replicates) into one VCF row.
    //   drop no-calls; remaining concordant -> use it;
    //   remaining disagree -> no-call + conflict log.
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<Emit>> = HashMap::new();
    for e in emits {
        if !groups.contains_key(&e.key) {
            order.push(e.key.clone());
        }
        groups.entry(e.key.clone()).or_default().push(e);
    }

    let mut written = 0usize;
    for key in order {
        let members = groups.remove(&key).expect("group present");
        if members.len() == 1 {
            writer.write_all(members[0].line.as_bytes())?;
            written += 1;
            continue;
        }
        let calls: Vec<&Emit> = members.iter().filter(|m| m.gt != "./.").collect();
        let distinct: std::collections::BTreeSet<&str> =
            calls.iter().map(|m| m.gt.as_str()).collect();
        if calls.is_empty() {
            // all no-call: emit one no-call row
            writer.write_all(members[0].line.as_bytes())?;
        } else if distinct.len() == 1 {
            // concordant (incl. call + dropped no-calls): use the called row
            writer.write_all(calls[0].line.as_bytes())?;
        } else {
            // genuine disagreement: emit no-call, log conflict
            writer.write_all(force_nocall(&members[0].line).as_bytes())?;
            let gts: Vec<&str> = calls.iter().map(|m| m.gt.as_str()).collect();
            missing_logger.log(&format!(
                "dup_conflict: {} gts=[{}] ({} probes)",
                members[0].rsid,
                gts.join(","),
                members.len()
            ))?;
        }
        written += 1;
    }
    // written_rows now reflects merged loci, not raw probe rows.
    stats.written_rows = written;

    writer.flush()?;
    missing_logger.flush()?;

    if output_paths.gzip {
        gzip_output(&output_paths.write_path)?;
    }

    Ok(stats)
}

fn collect_row(
    line: &str,
    parser: &mut RowParser,
    rows: &mut Vec<crate::genotype_reader::GenotypeRow>,
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
    position_map: &HashMap<(String, i64), ReferenceVariant>,
    cache: &RsidCache,
    row: &crate::genotype_reader::GenotypeRow,
    include_metrics: bool,
) -> RowResult {
    let mut result = RowResult::default();
    let mut rsid_label = row.rsid.trim().to_string();

    // Resolve ref/alt by rsid first; for Illumina non-rsid probes (empty or
    // unresolvable rsid) fall back to a (chrom,pos) lookup against the
    // grch38_non_rsids table.
    let mut resolved = if rsid_label.is_empty() {
        None
    } else {
        resolve_reference(&rsid_label, reference_map, cache)
    };
    if resolved.is_none() {
        if let Some(pv) = position_map.get(&(row.chrom.clone(), row.pos)) {
            let alternates = parse_alternates(&pv.alternates);
            if !pv.reference.is_empty() && !alternates.is_empty() {
                if rsid_label.is_empty() {
                    rsid_label = format!("rs{}", pv.rsid);
                }
                resolved = Some(ResolvedReference {
                    reference: normalize_sequence(&pv.reference),
                    alternates,
                });
            }
        }
    }
    if rsid_label.is_empty() && resolved.is_none() {
        result.stats.invalid_rsid += 1;
        result.logs.push("missing_rsid".to_string());
        return result;
    }
    let rsid_label = rsid_label.as_str();

    let mut unresolved = false;
    let mut reference = "N".to_string();
    let mut alternates: Vec<String> = Vec::new();

    if let Some(resolved_ref) = resolved.as_ref() {
        reference = resolved_ref.reference.clone();
        alternates = resolved_ref.alternates.clone();
    } else {
        unresolved = true;
        result.stats.missing_reference += 1;
        result.logs.push(format!("missing_reference: {rsid_label}"));
    }

    let (gt_call, gt_unresolved, used_indel) = if resolved.is_some() {
        build_genotype_call(row.genotype.as_deref(), &reference, &alternates)
    } else {
        ("./.".to_string(), true, false)
    };
    if gt_unresolved {
        unresolved = true;
        if resolved.is_some() {
            result.stats.missing_genotype += 1;
            result.logs.push(format!("missing_genotype: {rsid_label}"));
        }
    } else if used_indel {
        result.logs.push(format!(
            "indel_inferred: {rsid_label} {} -> {} (ref={}, alt={})",
            row.genotype.clone().unwrap_or_default(),
            gt_call,
            reference,
            alternates.join(",")
        ));
    }

    let alt_field = if alternates.is_empty() {
        ".".to_string()
    } else {
        alternates.join(",")
    };

    let mut info_fields = vec![
        format!("GQ={DEFAULT_GQ}"),
        format!("DP={DEFAULT_DP}"),
        format!("FORCE_RSID={}", rsid_label),
    ];
    if unresolved {
        info_fields.push("UNRESOLVED".to_string());
        result.stats.unresolved_rows += 1;
    }
    let info_field = info_fields.join(";");

    let (format_field, sample_field) = if include_metrics {
        let gs = row.gs.clone().unwrap_or_else(|| ".".to_string());
        let baf = row.baf.clone().unwrap_or_else(|| ".".to_string());
        let lrr = row.lrr.clone().unwrap_or_else(|| ".".to_string());
        (
            "GT:GQ:DP:GS:BAF:LRR".to_string(),
            format!("{gt_call}:{DEFAULT_GQ}:{DEFAULT_DP}:{gs}:{baf}:{lrr}"),
        )
    } else {
        (
            "GT:GQ:DP".to_string(),
            format!("{gt_call}:{DEFAULT_GQ}:{DEFAULT_DP}"),
        )
    };

    result.stats.written_rows += 1;
    result.rsid = rsid_label.to_string();
    result.locus = Some((row.chrom.clone(), row.pos));
    result.gt = Some(gt_call.clone());
    result.line = Some(format!(
        "{}\t{}\t{}\t{}\t{}\t.\tPASS\t{}\t{}\t{}\n",
        row.chrom,
        row.pos,
        rsid_label,
        reference,
        alt_field,
        info_field,
        format_field,
        sample_field
    ));
    result
}

/// Rewrite a VCF data line to a no-call genotype and flag INFO=CONFLICT.
/// Columns: 0 CHROM 1 POS 2 ID 3 REF 4 ALT 5 QUAL 6 FILTER 7 INFO 8 FORMAT 9 SAMPLE
fn force_nocall(line: &str) -> String {
    let trimmed = line.strip_suffix('\n').unwrap_or(line);
    let mut cols: Vec<String> = trimmed.split('\t').map(|s| s.to_string()).collect();
    if cols.len() >= 10 {
        if !cols[7].split(';').any(|f| f == "CONFLICT") {
            cols[7].push_str(";CONFLICT");
        }
        let mut parts = cols[9].split(':');
        let rest: Vec<&str> = parts.by_ref().skip(1).collect();
        cols[9] = if rest.is_empty() {
            "./.".to_string()
        } else {
            format!("./.:{}", rest.join(":"))
        };
    }
    let mut out = cols.join("\t");
    out.push('\n');
    out
}

fn resolve_reference(
    rsid: &str,
    reference_map: &HashMap<i64, ReferenceVariant>,
    cache: &RsidCache,
) -> Option<ResolvedReference> {
    let rsid_norm = normalize_rsid(rsid);
    let rsid_int = rsid_norm.trim_start_matches("rs").parse::<i64>().ok()?;
    if let Some(reference) = reference_map.get(&rsid_int) {
        let alternates = parse_alternates(&reference.alternates);
        if !reference.reference.is_empty() && !alternates.is_empty() {
            return Some(ResolvedReference {
                reference: normalize_sequence(&reference.reference),
                alternates,
            });
        }
    }

    if let Some(entry) = cache.get(&rsid_norm) {
        let alternates = parse_alternates(&entry.alternates);
        if !entry.reference.is_empty() && !alternates.is_empty() {
            return Some(ResolvedReference {
                reference: normalize_sequence(&entry.reference),
                alternates,
            });
        }
    }

    None
}

fn write_vcf_header(
    writer: &mut BufWriter<File>,
    sample_name: &str,
    include_metrics: bool,
) -> Result<()> {
    writeln!(writer, "##fileformat=VCFv4.2")?;
    writeln!(writer, "##source=DynamicDNA")?;
    writeln!(writer, "##reference=hg38")?;
    writeln!(
        writer,
        "##INFO=<ID=GQ,Number=1,Type=Integer,Description=\"Genotype Quality\">"
    )?;
    writeln!(
        writer,
        "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Read Depth\">"
    )?;
    writeln!(
        writer,
        "##INFO=<ID=FORCE_RSID,Number=1,Type=String,Description=\"Forced RSID\">"
    )?;
    writeln!(
        writer,
        "##INFO=<ID=UNRESOLVED,Number=0,Type=Flag,Description=\"Non-ACGT or missing genotype\">"
    )?;
    writeln!(
        writer,
        "##FILTER=<ID=PASS,Description=\"All filters passed\">"
    )?;
    writeln!(
        writer,
        "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">"
    )?;
    writeln!(
        writer,
        "##FORMAT=<ID=GQ,Number=1,Type=Integer,Description=\"Genotype Quality\">"
    )?;
    writeln!(
        writer,
        "##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Read Depth\">"
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
    for contig in CONTIG_HEADERS {
        writeln!(writer, "##contig=<ID={}>", contig)?;
    }
    writeln!(
        writer,
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t{}",
        sample_name
    )?;
    Ok(())
}

fn resolve_output_paths(args: &GenotypeToVcfArgs, input: &Path) -> Result<OutputPaths> {
    let output = if let Some(path) = &args.output {
        path.clone()
    } else {
        default_output_path(input, args.outdir.as_ref(), args.gzip)
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

fn default_output_path(input: &Path, outdir: Option<&PathBuf>, gzip: bool) -> PathBuf {
    let base_name = genotype_file_stem(input).unwrap_or_else(|| "output".to_string());
    let filename = if gzip {
        format!("{base_name}.vcf.gz")
    } else {
        format!("{base_name}.vcf")
    };
    if let Some(outdir) = outdir {
        outdir.join(filename)
    } else {
        input.with_file_name(filename)
    }
}

fn default_log_path(output_path: &Path) -> PathBuf {
    if output_path.extension().and_then(|ext| ext.to_str()) == Some("gz") {
        let without_gz = output_path.with_extension("");
        without_gz.with_extension("vcf.log")
    } else {
        output_path.with_extension("vcf.log")
    }
}

fn default_sample_name(input: &Path) -> String {
    let base = input
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("SAMPLE");
    if let Some((prefix, _)) = base.split_once('_') {
        if !prefix.is_empty() {
            return prefix.to_string();
        }
    }
    genotype_file_stem(input)
        .map(|stem| stem.replace(' ', "_"))
        .filter(|stem| !stem.is_empty())
        .unwrap_or_else(|| "SAMPLE".to_string())
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

#[derive(Clone)]
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
    unresolved_rows: usize,
}

impl ConversionStats {
    fn add(&mut self, other: &ConversionStats) {
        self.parsed_rows += other.parsed_rows;
        self.written_rows += other.written_rows;
        self.skipped_rows += other.skipped_rows;
        self.invalid_rsid += other.invalid_rsid;
        self.missing_reference += other.missing_reference;
        self.missing_genotype += other.missing_genotype;
        self.unresolved_rows += other.unresolved_rows;
    }
}

#[derive(Default)]
struct RowResult {
    line: Option<String>,
    stats: ConversionStats,
    logs: Vec<String>,
    /// Locus identity for duplicate-probe merging (Illumina BOT/BOT2/ilmndup).
    rsid: String,
    locus: Option<(String, i64)>,
    /// The GT call ("0/0", "0/1", "1/1", "./.") when a line was produced.
    gt: Option<String>,
}

struct MissingLogger {
    writer: Box<dyn Write>,
}

impl MissingLogger {
    fn new(path: Option<&Path>) -> Result<Self> {
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

struct ResolvedReference {
    reference: String,
    alternates: Vec<String>,
}

fn build_genotype_call(
    genotype: Option<&str>,
    reference: &str,
    alternates: &[String],
) -> (String, bool, bool) {
    let Some(genotype) = genotype else {
        return ("./.".to_string(), true, false);
    };
    let trimmed = genotype.trim().to_uppercase();
    if trimmed.is_empty() || MISSING_GENOTYPES.contains(&trimmed.as_str()) {
        return ("./.".to_string(), true, false);
    }

    let is_indel = trimmed.contains('D')
        || trimmed.contains('I')
        || alternates.iter().any(|alt| alt.len() != reference.len());
    if is_indel {
        if let Some((a1, a2)) = map_indel_genotype(&trimmed, reference, alternates) {
            if let Some(gt) = map_to_gt(&a1, &a2, reference, alternates) {
                return (gt, false, true);
            }
        }
    }

    let (a1, a2) = match split_genotype(&trimmed) {
        Some(tokens) => tokens,
        None => return ("./.".to_string(), true, false),
    };
    match map_to_gt(&a1, &a2, reference, alternates) {
        Some(gt) => (gt, false, false),
        None => ("./.".to_string(), true, false),
    }
}

fn split_genotype(genotype: &str) -> Option<(String, String)> {
    if genotype.contains('/') || genotype.contains('|') {
        let parts: Vec<&str> = genotype.split(['/', '|']).collect();
        if parts.len() >= 2 {
            return Some((parts[0].to_string(), parts[1].to_string()));
        }
    }

    let compact: String = genotype.chars().filter(|c| !c.is_whitespace()).collect();
    if compact.is_empty() {
        return None;
    }
    if compact.len() == 1 {
        return Some((compact.clone(), compact));
    }
    if compact.len() == 2 {
        let mut chars = compact.chars();
        let a1 = chars.next()?.to_string();
        let a2 = chars.next()?.to_string();
        return Some((a1, a2));
    }
    let mid = compact.len() / 2;
    Some((compact[..mid].to_string(), compact[mid..].to_string()))
}

fn map_to_gt(
    allele1: &str,
    allele2: &str,
    reference: &str,
    alternates: &[String],
) -> Option<String> {
    let idx1 = allele_index(allele1, reference, alternates)?;
    let idx2 = allele_index(allele2, reference, alternates)?;
    let min = idx1.min(idx2);
    let max = idx1.max(idx2);
    Some(format!("{}/{}", min, max))
}

fn allele_index(allele: &str, reference: &str, alternates: &[String]) -> Option<usize> {
    if allele == reference {
        return Some(0);
    }
    alternates
        .iter()
        .position(|alt| alt == allele)
        .map(|idx| idx + 1)
}

fn map_indel_genotype(
    genotype: &str,
    reference: &str,
    alternates: &[String],
) -> Option<(String, String)> {
    if !genotype.contains('D') && !genotype.contains('I') {
        return None;
    }
    let mut candidates = Vec::with_capacity(alternates.len() + 1);
    candidates.push(reference.to_string());
    candidates.extend_from_slice(alternates);
    candidates.sort_by_key(|value| value.len());
    let allele_d = candidates.first()?.clone();
    let allele_i = candidates.last()?.clone();
    if allele_d.len() == allele_i.len() {
        return None;
    }

    let mut mapped = Vec::new();
    for ch in genotype.chars() {
        match ch {
            'D' => mapped.push(allele_d.clone()),
            'I' => mapped.push(allele_i.clone()),
            _ => return None,
        }
    }
    if mapped.len() >= 2 {
        return Some((mapped[0].clone(), mapped[1].clone()));
    }
    None
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

fn normalize_sequence(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn load_reference_map(store: &StatsStore) -> Result<HashMap<i64, ReferenceVariant>> {
    let references = store.all_references_with_overrides()?;
    let mut map = HashMap::with_capacity(references.len());
    for reference in references {
        map.insert(reference.rsid, reference);
    }
    Ok(map)
}

fn load_position_map(store: &StatsStore) -> Result<HashMap<(String, i64), ReferenceVariant>> {
    let non_rsids = store.all_non_rsids()?;
    let mut map = HashMap::with_capacity(non_rsids.len());
    for variant in non_rsids {
        map.insert((variant.chromosome.clone(), variant.position), variant);
    }
    Ok(map)
}
