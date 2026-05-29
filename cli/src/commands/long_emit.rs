use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};

use crate::download::ensure_reference_db;
use crate::genotype_reader::{detect_delimiter, RowOutcome, RowParser};
use crate::long_rows::{
    is_snp, locus_key, normalize_sequence, parse_alternates, parse_genotype_dosages,
    parse_vcf_gt_dosages, vcf_gt_from_sample, vcf_sample_name, LongRow, LongRowWriter,
};
use crate::rsid_cache::normalize_rsid;
use crate::stats::{ReferenceVariant, StatsStore};
use crate::EmitLongArgs;
use rusqlite::OptionalExtension;

const LOOKAHEAD_LINES: usize = 2048;

pub fn run_long_emit(args: EmitLongArgs) -> Result<()> {
    let overall_start = Instant::now();
    let has_vcf = args.vcf.is_some();
    let has_inputs = !args.inputs.is_empty();
    if has_vcf == has_inputs {
        bail!("Provide either --vcf or --input (genotype files), but not both");
    }

    if has_vcf {
        let vcf_path = args.vcf.as_ref().expect("vcf path");
        let output_path = resolve_output_path(vcf_path, args.output.as_ref())?;
        let stats = emit_from_vcf(vcf_path, &output_path, args.participant.as_ref())?;
        eprintln!(
            "✅ emit-long (vcf): {} variants, {} rows, {} missing-gt",
            stats.variants_seen, stats.rows_emitted, stats.missing_gt
        );
        eprintln!(
            "⏱️  emit-long: elapsed {:.2}s",
            overall_start.elapsed().as_secs_f64()
        );
        println!("✅ Emitted long rows to {}", output_path.display());
        return Ok(());
    }

    if args.inputs.len() > 1 && args.output.is_some() {
        bail!("--output can only be used with a single --input file");
    }
    if args.inputs.len() > 1 && args.participant.is_some() {
        bail!("--participant can only be used with a single --input file");
    }
    if args.inputs.len() > 1 && args.missing_ref_log.is_some() {
        bail!("--missing-ref-log can only be used with a single --input file");
    }

    let sqlite_path = ensure_reference_db(Some(&args.sqlite), args.force_download)?;
    let store = StatsStore::connect_read_only(&sqlite_path)?;
    let mut resolver = ReferenceResolver::new(&store)?;

    let mut successful_files = 0usize;
    for input in &args.inputs {
        let start = Instant::now();
        let output_path = resolve_output_path(input, args.output.as_ref())?;
        let participant = args
            .participant
            .clone()
            .unwrap_or_else(|| default_participant(input));
        let mut missing_logger = MissingRefLogger::new(args.missing_ref_log.as_deref())?;
        let stats = match emit_from_genotype(
            input,
            &output_path,
            &participant,
            &mut resolver,
            &mut missing_logger,
        ) {
            Ok(stats) => stats,
            Err(err) if args.inputs.len() > 1 => {
                eprintln!(
                    "WARNING: skipping file {}: emit-long failed: {err:#}",
                    input.display()
                );
                let _ = std::fs::remove_file(&output_path);
                continue;
            }
            Err(err) => return Err(err),
        };
        successful_files += 1;
        eprintln!(
            "✅ emit-long (genotype): {} parsed, {} rows, {} missing-ref, {} missing-gt",
            stats.rows_parsed, stats.rows_emitted, stats.missing_reference, stats.missing_gt
        );
        eprintln!(
            "⏱️  emit-long: {} took {:.2}s",
            input.display(),
            start.elapsed().as_secs_f64()
        );
        println!(
            "✅ Emitted long rows to {} (participant {})",
            output_path.display(),
            participant
        );
    }
    if successful_files == 0 {
        bail!("No input files produced usable long rows");
    }
    eprintln!(
        "⏱️  emit-long: total elapsed {:.2}s",
        overall_start.elapsed().as_secs_f64()
    );
    Ok(())
}

struct EmitStats {
    variants_seen: u64,
    rows_emitted: u64,
    missing_gt: u64,
    rows_parsed: u64,
    missing_reference: u64,
}

fn emit_from_vcf(
    input: &Path,
    output: &Path,
    participant_override: Option<&String>,
) -> Result<EmitStats> {
    let reader: Box<dyn Read> = if is_gz(input) {
        let file = File::open(input).with_context(|| format!("Open {:?}", input))?;
        Box::new(noodles::bgzf::io::Reader::new(file))
    } else {
        Box::new(File::open(input).with_context(|| format!("Open {:?}", input))?)
    };
    let mut buf = BufReader::new(reader);
    let mut line = String::new();
    let mut participant = participant_override
        .cloned()
        .unwrap_or_else(|| default_participant(input));
    let mut saw_header = false;

    let mut writer = LongRowWriter::new(BufWriter::new(
        File::create(output).with_context(|| format!("Create {:?}", output))?,
    ));

    let mut stats = EmitStats {
        variants_seen: 0,
        rows_emitted: 0,
        missing_gt: 0,
        rows_parsed: 0,
        missing_reference: 0,
    };

    loop {
        line.clear();
        let bytes = buf.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        if line.starts_with("##") {
            continue;
        }
        if line.starts_with("#CHROM") {
            saw_header = true;
            if participant_override.is_none() {
                if let Some(sample) = vcf_sample_name(&line) {
                    participant = sample;
                }
            }
            continue;
        }
        if !saw_header {
            continue;
        }

        let parts: Vec<&str> = line.trim_end().split('\t').collect();
        if parts.len() < 10 {
            continue;
        }
        let chrom = parts[0];
        let pos = parts[1].parse::<i64>().unwrap_or(0);
        let rsid = parts[2];
        let reference = normalize_sequence(parts[3]);
        let alts = parse_alternates(parts[4]);
        if pos == 0 || !is_snp(&reference, &alts) {
            continue;
        }
        stats.variants_seen += 1;
        let sample = parts[9];
        let gt = vcf_gt_from_sample(sample).unwrap_or(".");
        let dosages = parse_vcf_gt_dosages(gt, alts.len());
        let rsid_val = if rsid == "." { "" } else { rsid };

        if let Some(dosages) = dosages {
            for (alt, dosage) in alts.iter().zip(dosages.iter()) {
                writer.write_row(&LongRow {
                    locus_key: locus_key(chrom, pos, &reference, alt),
                    rsid: rsid_val.to_string(),
                    participant_id: participant.clone(),
                    dosage: *dosage,
                })?;
                stats.rows_emitted += 1;
            }
        } else {
            stats.missing_gt += 1;
            for alt in &alts {
                writer.write_row(&LongRow {
                    locus_key: locus_key(chrom, pos, &reference, alt),
                    rsid: rsid_val.to_string(),
                    participant_id: participant.clone(),
                    dosage: -1,
                })?;
                stats.rows_emitted += 1;
            }
        }
    }

    writer.flush()?;
    Ok(stats)
}

fn emit_from_genotype(
    input: &Path,
    output: &Path,
    participant: &str,
    resolver: &mut ReferenceResolver,
    missing_logger: &mut MissingRefLogger,
) -> Result<EmitStats> {
    let input_file = File::open(input).with_context(|| format!("Open {:?}", input))?;
    let mut reader = BufReader::new(input_file);
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
        bail!("Input file {:?} is empty", input);
    }

    let delimiter = detect_delimiter(&buffered_lines);
    let mut parser = RowParser::new(delimiter);
    let mut stats = EmitStats {
        variants_seen: 0,
        rows_emitted: 0,
        missing_gt: 0,
        rows_parsed: 0,
        missing_reference: 0,
    };
    let mut pending_rows = Vec::new();

    let mut line_number: u64 = 0;
    let mut ctx = GenotypeEmitContext {
        parser: &mut parser,
        participant,
        resolver,
        stats: &mut stats,
        missing_logger,
        input,
        pending_rows: &mut pending_rows,
    };

    for line in &buffered_lines {
        line_number += 1;
        consume_genotype_line(&mut ctx, line, line_number)?;
    }

    buffer.clear();
    loop {
        buffer.clear();
        let bytes = reader.read_line(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        line_number += 1;
        consume_genotype_line(&mut ctx, &buffer, line_number)?;
    }

    let mut writer = LongRowWriter::new(BufWriter::new(
        File::create(output).with_context(|| format!("Create {:?}", output))?,
    ));
    write_merged_long_rows(&mut writer, pending_rows, &mut stats)?;
    writer.flush()?;
    Ok(stats)
}

struct GenotypeEmitContext<'a> {
    parser: &'a mut RowParser,
    participant: &'a str,
    resolver: &'a mut ReferenceResolver,
    stats: &'a mut EmitStats,
    missing_logger: &'a mut MissingRefLogger,
    input: &'a Path,
    pending_rows: &'a mut Vec<PendingLongRow>,
}

#[derive(Clone)]
struct PendingLongRow {
    key: String,
    row: LongRow,
}

fn consume_genotype_line(
    ctx: &mut GenotypeEmitContext<'_>,
    line: &str,
    line_number: u64,
) -> Result<()> {
    let outcome = match ctx.parser.consume_line(line) {
        Ok(outcome) => outcome,
        Err(err) => {
            ctx.missing_logger
                .log_issue(ctx.input, line_number, "parse_error", "", line)?;
            eprintln!(
                "WARNING: {}:{}: parse_error: {err:#}",
                ctx.input.display(),
                line_number
            );
            return Ok(());
        }
    };

    match outcome {
        RowOutcome::Parsed(row) => {
            ctx.stats.rows_parsed += 1;
            let mut rsid_label = clean_rsid_label(&row.rsid);
            let mut reference = if rsid_label.is_empty() {
                None
            } else {
                match ctx.resolver.resolve_rsid(&rsid_label) {
                    Ok(reference) => reference,
                    Err(err) => {
                        ctx.missing_logger.log_issue(
                            ctx.input,
                            line_number,
                            "rsid_resolve_error",
                            &rsid_label,
                            line,
                        )?;
                        eprintln!(
                            "WARNING: {}:{}: rsid_resolve_error {}: {err:#}",
                            ctx.input.display(),
                            line_number,
                            rsid_label
                        );
                        None
                    }
                }
            };
            if reference.is_none() {
                match ctx.resolver.resolve_position(&row.chrom, row.pos) {
                    Ok(Some(position_reference)) => {
                        if rsid_label.is_empty() {
                            rsid_label = format!("rs{}", position_reference.rsid);
                        }
                        reference = Some(position_reference);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        ctx.missing_logger.log_issue(
                            ctx.input,
                            line_number,
                            "position_resolve_error",
                            &format!("{}:{}", row.chrom, row.pos),
                            line,
                        )?;
                        eprintln!(
                            "WARNING: {}:{}: position_resolve_error {}:{}: {err:#}",
                            ctx.input.display(),
                            line_number,
                            row.chrom,
                            row.pos
                        );
                    }
                }
            }
            let reference = match reference {
                Some(reference) => reference,
                None => {
                    ctx.stats.missing_reference += 1;
                    ctx.missing_logger.log_issue(
                        ctx.input,
                        line_number,
                        "missing_ref",
                        &rsid_label,
                        line,
                    )?;
                    return Ok(());
                }
            };
            let reference_base = normalize_sequence(&reference.reference);
            let alternates = parse_alternates(&reference.alternates);
            if !is_snp(&reference_base, &alternates) {
                ctx.missing_logger.log_issue(
                    ctx.input,
                    line_number,
                    "non_snp_reference",
                    &rsid_label,
                    line,
                )?;
                return Ok(());
            }
            let dosages =
                parse_genotype_dosages(row.genotype.as_deref(), &reference_base, &alternates);
            if let Some(dosages) = dosages {
                for (alt, dosage) in alternates.iter().zip(dosages.iter()) {
                    push_pending_row(ctx, &reference, &reference_base, alt, &rsid_label, *dosage);
                }
            } else {
                ctx.stats.missing_gt += 1;
                for alt in &alternates {
                    push_pending_row(ctx, &reference, &reference_base, alt, &rsid_label, -1);
                }
            }
        }
        RowOutcome::Skipped => {
            ctx.missing_logger.log_issue(
                ctx.input,
                line_number,
                "skipped_unusable_row",
                "",
                line,
            )?;
        }
        RowOutcome::Ignored => {}
    }
    Ok(())
}

fn push_pending_row(
    ctx: &mut GenotypeEmitContext<'_>,
    reference: &ReferenceVariant,
    reference_base: &str,
    alt: &str,
    rsid_label: &str,
    dosage: i8,
) {
    let row = LongRow {
        locus_key: locus_key(
            &reference.chromosome,
            reference.position,
            reference_base,
            alt,
        ),
        rsid: rsid_label.to_string(),
        participant_id: ctx.participant.to_string(),
        dosage,
    };
    let key = format!("{}\t{}\t{}", row.locus_key, row.rsid, row.participant_id);
    ctx.pending_rows.push(PendingLongRow { key, row });
}

fn write_merged_long_rows(
    writer: &mut LongRowWriter<BufWriter<File>>,
    pending_rows: Vec<PendingLongRow>,
    stats: &mut EmitStats,
) -> Result<()> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<LongRow>> = HashMap::new();
    for pending in pending_rows {
        if !groups.contains_key(&pending.key) {
            order.push(pending.key.clone());
        }
        groups.entry(pending.key).or_default().push(pending.row);
    }

    for key in order {
        let members = groups.remove(&key).expect("group present");
        let selected = select_duplicate_probe_row(&members);
        writer.write_row(&selected)?;
        stats.rows_emitted += 1;
    }
    Ok(())
}

fn select_duplicate_probe_row(rows: &[LongRow]) -> LongRow {
    let calls: Vec<&LongRow> = rows.iter().filter(|row| row.dosage >= 0).collect();
    if calls.is_empty() {
        return rows[0].clone();
    }
    let distinct: BTreeSet<i8> = calls.iter().map(|row| row.dosage).collect();
    if distinct.len() == 1 {
        return calls[0].clone();
    }
    let mut conflict = rows[0].clone();
    conflict.dosage = -1;
    conflict
}

fn clean_rsid_label(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() || matches!(value, "." | "-") {
        return String::new();
    }
    normalize_rsid(value)
}

struct MissingRefLogger {
    writer: Option<BufWriter<File>>,
}

impl MissingRefLogger {
    fn new(path: Option<&Path>) -> Result<Self> {
        let writer = if let Some(path) = path {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("Create {:?}", parent))?;
                }
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("Open {:?}", path))?;
            Some(BufWriter::new(file))
        } else {
            None
        };
        Ok(Self { writer })
    }

    fn log_issue(
        &mut self,
        input: &Path,
        line_number: u64,
        code: &str,
        label: &str,
        raw_line: &str,
    ) -> Result<()> {
        let trimmed = raw_line.trim_end();
        if let Some(writer) = self.writer.as_mut() {
            writeln!(
                writer,
                "{}\t{}\t{}\t{}\t{}",
                input.display(),
                line_number,
                label,
                code,
                trimmed
            )?;
        } else if !matches!(
            code,
            "parse_error" | "rsid_resolve_error" | "position_resolve_error"
        ) {
            eprintln!(
                "WARNING: {}:{}: {} {}",
                input.display(),
                line_number,
                code,
                label
            );
        }
        Ok(())
    }
}

fn resolve_output_path(input: &Path, output: Option<&PathBuf>) -> Result<PathBuf> {
    if let Some(out) = output {
        return Ok(out.clone());
    }
    let stem = input
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let mut out = input.with_file_name(format!("{stem}.bvlr"));
    if let Some(parent) = input.parent() {
        if parent.as_os_str().is_empty() {
            out = PathBuf::from(format!("{stem}.bvlr"));
        }
    }
    Ok(out)
}

fn default_participant(input: &Path) -> String {
    input
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(|stem| stem.replace(' ', "_"))
        .filter(|stem| !stem.is_empty())
        .unwrap_or_else(|| "SAMPLE".to_string())
}

fn is_gz(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some("gz")
}

struct ReferenceResolver {
    conn: rusqlite::Connection,
    rsid_cache: HashMap<i64, Option<ReferenceVariant>>,
    position_cache: HashMap<(String, i64), Option<ReferenceVariant>>,
}

impl ReferenceResolver {
    fn new(store: &StatsStore) -> Result<Self> {
        let conn = store.open_connection()?;
        Ok(Self {
            conn,
            rsid_cache: HashMap::new(),
            position_cache: HashMap::new(),
        })
    }

    fn resolve_rsid(&mut self, rsid: &str) -> Result<Option<ReferenceVariant>> {
        let rsid_norm = normalize_rsid(rsid);
        let rsid_int = match rsid_norm.trim_start_matches("rs").parse::<i64>() {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        if let Some(cached) = self.rsid_cache.get(&rsid_int) {
            return Ok(cached.clone());
        }

        let mut row = self
            .conn
            .prepare_cached(
                "SELECT rsid, chromosome, position, reference, alternates
                 FROM rsid_reference_user WHERE rsid = ?1",
            )?
            .query_row([rsid_int], |row| {
                Ok(ReferenceVariant {
                    rsid: row.get(0)?,
                    chromosome: row.get(1)?,
                    position: row.get(2)?,
                    reference: row.get(3)?,
                    alternates: row.get(4)?,
                })
            })
            .optional()?;

        if row.is_none() {
            row = self
                .conn
                .prepare_cached(
                    "SELECT rsid, chromosome, position, reference, alternates
                     FROM rsid_reference WHERE rsid = ?1",
                )?
                .query_row([rsid_int], |row| {
                    Ok(ReferenceVariant {
                        rsid: row.get(0)?,
                        chromosome: row.get(1)?,
                        position: row.get(2)?,
                        reference: row.get(3)?,
                        alternates: row.get(4)?,
                    })
                })
                .optional()?;
        }

        self.rsid_cache.insert(rsid_int, row.clone());
        Ok(row)
    }

    fn resolve_position(&mut self, chrom: &str, pos: i64) -> Result<Option<ReferenceVariant>> {
        if pos <= 0 {
            return Ok(None);
        }
        let chrom = normalize_chrom(chrom);
        let key = (chrom, pos);
        if let Some(cached) = self.position_cache.get(&key) {
            return Ok(cached.clone());
        }

        let mut stmt = self.conn.prepare_cached(
            "SELECT rsid, chromosome, position, reference, alternates
             FROM grch38_non_rsids
             WHERE chromosome = ?1 AND position = ?2
             ORDER BY rsid",
        )?;
        let mut rows = stmt.query((&key.0, key.1))?;
        let mut resolved: Option<ReferenceVariant> = None;
        let mut ambiguous = false;
        while let Some(row) = rows.next()? {
            let candidate = ReferenceVariant {
                rsid: row.get(0)?,
                chromosome: row.get(1)?,
                position: row.get(2)?,
                reference: row.get(3)?,
                alternates: row.get(4)?,
            };
            if let Some(existing) = resolved.as_ref() {
                if existing.rsid != candidate.rsid
                    || existing.reference != candidate.reference
                    || existing.alternates != candidate.alternates
                {
                    ambiguous = true;
                    break;
                }
            } else {
                resolved = Some(candidate);
            }
        }
        let row = if ambiguous { None } else { resolved };

        self.position_cache.insert(key, row.clone());
        Ok(row)
    }
}

fn normalize_chrom(value: &str) -> String {
    let chrom = value.trim();
    chrom
        .strip_prefix("chr")
        .or_else(|| chrom.strip_prefix("CHR"))
        .unwrap_or(chrom)
        .to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::long_aggregate::run_long_aggregate;
    use crate::AggregateLongArgs;
    use std::fs;
    use std::io::Read;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn row(dosage: i8) -> LongRow {
        LongRow {
            locus_key: "1-100-A-G".to_string(),
            rsid: "rs1".to_string(),
            participant_id: "P1".to_string(),
            dosage,
        }
    }

    fn test_resolver() -> ReferenceResolver {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE rsid_reference_user (
                rsid INTEGER PRIMARY KEY,
                chromosome TEXT NOT NULL,
                position INTEGER NOT NULL,
                reference TEXT NOT NULL,
                alternates TEXT NOT NULL
            );
            CREATE TABLE rsid_reference (
                rsid INTEGER PRIMARY KEY,
                chromosome TEXT NOT NULL,
                position INTEGER NOT NULL,
                reference TEXT NOT NULL,
                alternates TEXT NOT NULL
            );
            CREATE TABLE grch38_non_rsids (
                snp_name TEXT PRIMARY KEY,
                rsid INTEGER NOT NULL,
                chromosome TEXT NOT NULL,
                position INTEGER NOT NULL,
                reference TEXT NOT NULL,
                alternates TEXT NOT NULL,
                source TEXT NOT NULL,
                note TEXT
            );
            INSERT INTO grch38_non_rsids
                (snp_name, rsid, chromosome, position, reference, alternates, source)
            VALUES
                ('CNV_1', 123, '2', 200, 'A', 'G', 'test'),
                ('AMBIG_1', 124, '3', 300, 'A', 'G', 'test'),
                ('AMBIG_2', 125, '3', 300, 'A', 'T', 'test');",
        )
        .unwrap();
        ReferenceResolver {
            conn,
            rsid_cache: HashMap::new(),
            position_cache: HashMap::new(),
        }
    }

    fn full_test_resolver() -> ReferenceResolver {
        let resolver = test_resolver();
        resolver
            .conn
            .execute_batch(
                "INSERT INTO rsid_reference
                    (rsid, chromosome, position, reference, alternates)
                VALUES
                    (1, '1', 100, 'A', 'G'),
                    (2, '1', 200, 'C', 'T'),
                    (3, '2', 300, 'A', 'C');
                INSERT INTO grch38_non_rsids
                    (snp_name, rsid, chromosome, position, reference, alternates, source)
                VALUES
                    ('NONRS_CNV_A', 3, '2', 300, 'A', 'C', 'test');",
            )
            .unwrap();
        resolver
    }

    fn unique_test_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("bvs-{label}-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_text(path: &Path, contents: &str) {
        fs::write(path, contents).unwrap();
    }

    fn emit_test_file(
        resolver: &mut ReferenceResolver,
        input: &Path,
        output: &Path,
        participant: &str,
        warning_log: &Path,
    ) {
        let mut logger = MissingRefLogger::new(Some(warning_log)).unwrap();
        let stats = emit_from_genotype(input, output, participant, resolver, &mut logger).unwrap();
        assert!(
            stats.rows_emitted > 0,
            "expected emitted rows for {}",
            participant
        );
    }

    #[test]
    fn duplicate_probe_merge_prefers_concordant_call_over_no_call() {
        let selected = select_duplicate_probe_row(&[row(-1), row(1), row(1)]);
        assert_eq!(selected.dosage, 1);
    }

    #[test]
    fn duplicate_probe_merge_conflict_becomes_no_call() {
        let selected = select_duplicate_probe_row(&[row(0), row(2)]);
        assert_eq!(selected.dosage, -1);
    }

    #[test]
    fn resolver_uses_position_fallback_for_non_rsid_illumina_probes() {
        let mut resolver = test_resolver();
        let reference = resolver.resolve_position("chr2", 200).unwrap().unwrap();
        assert_eq!(reference.rsid, 123);
        assert_eq!(reference.reference, "A");
        assert_eq!(reference.alternates, "G");
    }

    #[test]
    fn resolver_does_not_choose_ambiguous_position_fallback() {
        let mut resolver = test_resolver();
        assert!(resolver.resolve_position("3", 300).unwrap().is_none());
    }

    #[test]
    fn emit_long_then_aggregate_counts_ddna_and_noisy_illumina_rsids() {
        let tmp = unique_test_dir("emit-aggregate");
        let warn = tmp.join("warnings.tsv");
        let ddna1 = tmp.join("ddna1.txt");
        let ddna2 = tmp.join("ddna2.txt");
        let illumina_sectionless = tmp.join("illumina_sectionless.txt");
        let illumina_sections = tmp.join("illumina_sections.txt");

        write_text(
            &ddna1,
            "rs1\t1\t100\tAG\t.\t.\t.\nrs2\t1\t200\tTT\t.\t.\t.\nrs404\t1\t404\tAA\t.\t.\t.\n",
        );
        write_text(
            &ddna2,
            "rs1\tchr1\t100\tAA\t.\t.\t.\nrs2\t1\t200\tCT\t.\t.\t.\n",
        );
        write_text(
            &illumina_sectionless,
            "SNP Name\tSNP\tChr\tPosition\tAllele1 - Plus\tAllele2 - Plus\n\
             BOT-rs1\t[A/G]\tchr1\t100\tG\tG\n\
             rs1_ilmndup1\t[A/G]\t1\t100\t-\t-\n\
             malformed-short-row\n\
             NONRS_CNV_A\t[A/C]\tchr2\t300\tA\tC\n\
             UNMAPPED_VENDOR_PROBE\t.\t0\t0\tA\tA\n",
        );
        write_text(
            &illumina_sections,
            "[Header]\nGSGT Version\t2.0.5\n[Data]\n\
             SNP Name\tSNP\tChr\tPosition\tAllele1 - Plus\tAllele2 - Plus\n\
             rs1_ilmndup2\t[A/G]\t1\t100\tA\tG\n\
             BOT2-rs2\t[C/T]\t1\t200\tC\tC\n\
             rs2_ilmndup_conflict\t[C/T]\t1\t200\tT\tT\n\
             NONRS_CNV_A\t[A/C]\t2\t300\tC\tC\n",
        );

        let mut resolver = full_test_resolver();
        let inputs = [
            (&ddna1, tmp.join("P_DDNA1.bvlr"), "P_DDNA1"),
            (&ddna2, tmp.join("P_DDNA2.bvlr"), "P_DDNA2"),
            (&illumina_sectionless, tmp.join("P_ILL1.bvlr"), "P_ILL1"),
            (&illumina_sections, tmp.join("P_ILL2.bvlr"), "P_ILL2"),
        ];
        for (input, output, participant) in &inputs {
            emit_test_file(&mut resolver, input, output, participant, &warn);
        }

        let args = AggregateLongArgs {
            inputs: inputs.iter().map(|(_, output, _)| output.clone()).collect(),
            input_list: None,
            input_glob: None,
            matrix_tsv: Some(tmp.join("matrix.tsv")),
            allele_freq_tsv: tmp.join("allele.tsv"),
            tmp_dir: Some(tmp.join("tmp")),
            chunk_records: 2,
            threads: 1,
            max_ram_percent: 80,
        };
        run_long_aggregate(args).unwrap();

        let mut matrix = String::new();
        File::open(tmp.join("matrix.tsv"))
            .unwrap()
            .read_to_string(&mut matrix)
            .unwrap();
        assert!(matrix.contains("locus_key\trsid\tP_DDNA1\tP_DDNA2\tP_ILL1\tP_ILL2"));
        assert!(matrix.contains("1-100-A-G\trs1\t1\t0\t2\t1"));
        assert!(matrix.contains("1-200-C-T\trs2\t2\t1\t-1\t-1"));
        assert!(matrix.contains("2-300-A-C\trs3\t-1\t-1\t1\t2"));

        let mut allele = String::new();
        File::open(tmp.join("allele.tsv"))
            .unwrap()
            .read_to_string(&mut allele)
            .unwrap();
        assert!(allele.contains("1-100-A-G\t4\t8\t1\t2\t0.500000\trs1"));
        assert!(allele.contains("1-200-C-T\t3\t4\t1\t1\t0.750000\trs2"));
        assert!(allele.contains("2-300-A-C\t3\t4\t1\t1\t0.750000\trs3"));

        let warnings = fs::read_to_string(&warn).unwrap();
        assert!(warnings.contains("missing_ref"));
        assert!(warnings.contains("skipped_unusable_row"));
    }
}
