use std::collections::HashMap;
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

    for input in &args.inputs {
        let start = Instant::now();
        let output_path = resolve_output_path(input, args.output.as_ref())?;
        let participant = args
            .participant
            .clone()
            .unwrap_or_else(|| default_participant(input));
        let mut missing_logger = MissingRefLogger::new(args.missing_ref_log.as_deref())?;
        let stats = emit_from_genotype(
            input,
            &output_path,
            &participant,
            &mut resolver,
            &mut missing_logger,
        )?;
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

    let mut line_number: u64 = 0;
    let mut ctx = GenotypeEmitContext {
        parser: &mut parser,
        writer: &mut writer,
        participant,
        resolver,
        stats: &mut stats,
        missing_logger,
        input,
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

    writer.flush()?;
    Ok(stats)
}

struct GenotypeEmitContext<'a> {
    parser: &'a mut RowParser,
    writer: &'a mut LongRowWriter<BufWriter<File>>,
    participant: &'a str,
    resolver: &'a mut ReferenceResolver,
    stats: &'a mut EmitStats,
    missing_logger: &'a mut MissingRefLogger,
    input: &'a Path,
}

fn consume_genotype_line(
    ctx: &mut GenotypeEmitContext<'_>,
    line: &str,
    line_number: u64,
) -> Result<()> {
    match ctx.parser.consume_line(line)? {
        RowOutcome::Parsed(row) => {
            ctx.stats.rows_parsed += 1;
            let rsid_label = row.rsid.trim();
            if rsid_label.is_empty() {
                return Ok(());
            }
            let reference = match ctx.resolver.resolve(rsid_label)? {
                Some(reference) => reference,
                None => {
                    ctx.stats.missing_reference += 1;
                    ctx.missing_logger
                        .log(ctx.input, line_number, rsid_label, line)?;
                    return Ok(());
                }
            };
            let reference_base = normalize_sequence(&reference.reference);
            let alternates = parse_alternates(&reference.alternates);
            if !is_snp(&reference_base, &alternates) {
                return Ok(());
            }
            let dosages =
                parse_genotype_dosages(row.genotype.as_deref(), &reference_base, &alternates);
            if let Some(dosages) = dosages {
                for (alt, dosage) in alternates.iter().zip(dosages.iter()) {
                    ctx.writer.write_row(&LongRow {
                        locus_key: locus_key(
                            &reference.chromosome,
                            reference.position,
                            &reference_base,
                            alt,
                        ),
                        rsid: rsid_label.to_string(),
                        participant_id: ctx.participant.to_string(),
                        dosage: *dosage,
                    })?;
                    ctx.stats.rows_emitted += 1;
                }
            } else {
                ctx.stats.missing_gt += 1;
                for alt in &alternates {
                    ctx.writer.write_row(&LongRow {
                        locus_key: locus_key(
                            &reference.chromosome,
                            reference.position,
                            &reference_base,
                            alt,
                        ),
                        rsid: rsid_label.to_string(),
                        participant_id: ctx.participant.to_string(),
                        dosage: -1,
                    })?;
                    ctx.stats.rows_emitted += 1;
                }
            }
        }
        RowOutcome::Skipped | RowOutcome::Ignored => {}
    }
    Ok(())
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

    fn log(&mut self, input: &Path, line_number: u64, rsid: &str, raw_line: &str) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            let trimmed = raw_line.trim_end();
            writeln!(
                writer,
                "{}\t{}\t{}\tmissing_ref\t{}",
                input.display(),
                line_number,
                rsid,
                trimmed
            )?;
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
    cache: HashMap<i64, Option<ReferenceVariant>>,
}

impl ReferenceResolver {
    fn new(store: &StatsStore) -> Result<Self> {
        let conn = store.open_connection()?;
        Ok(Self {
            conn,
            cache: HashMap::new(),
        })
    }

    fn resolve(&mut self, rsid: &str) -> Result<Option<ReferenceVariant>> {
        let rsid_norm = normalize_rsid(rsid);
        let rsid_int = match rsid_norm.trim_start_matches("rs").parse::<i64>() {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        if let Some(cached) = self.cache.get(&rsid_int) {
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

        self.cache.insert(rsid_int, row.clone());
        Ok(row)
    }
}
