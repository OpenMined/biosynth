use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

use anyhow::{Context, Result};

use crate::long_rows::LongRowReader;
use crate::DumpLongArgs;

pub fn run_long_dump(args: DumpLongArgs) -> Result<()> {
    let start = Instant::now();
    let input = File::open(&args.input).with_context(|| format!("Open {:?}", args.input))?;
    let mut reader = LongRowReader::new(input);
    let mut writer = BufWriter::new(
        File::create(&args.output).with_context(|| format!("Create {:?}", args.output))?,
    );

    writeln!(writer, "locus_key\trsid\tparticipant_id\tdosage")?;
    let mut rows: u64 = 0;
    while let Some(row) = reader.read_row()? {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}",
            row.locus_key, row.rsid, row.participant_id, row.dosage
        )?;
        rows += 1;
        if rows.is_multiple_of(5_000_000) {
            eprintln!("🔎 dump-long: wrote {} rows", rows);
        }
    }
    writer.flush().context("flush long dump")?;
    eprintln!("✅ dump-long: wrote {} rows", rows);
    eprintln!(
        "⏱️  dump-long: elapsed {:.2}s",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
