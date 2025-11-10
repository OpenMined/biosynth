use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgAction, Args, Parser, Subcommand};

mod download;
mod genotype;
mod stats;
mod util;

use crate::commands::allele_report::run_allele_report;
use crate::commands::genostats::run_genostats;
use crate::commands::reference_load::run_reference_load;
use crate::commands::synthetic::run_synthetic;

mod commands {
    pub mod allele_report;
    pub mod genostats;
    pub mod reference_load;
    pub mod synthetic;
}

#[derive(Parser)]
#[command(name = "bvs", version, about = "Synthetic Data Toolkit for BioVault", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Analyze genotype files and persist aggregated statistics.
    Genostats(GenostatsArgs),
    /// Export an HTML report of observed alleles per rsid.
    AlleleReport(AlleleReportArgs),
    /// Load reference allele lookup data into SQLite.
    ReferenceLoad(ReferenceLoadArgs),
    /// Generate a reference genotype file from stored data.
    Synthetic(SyntheticArgs),
}

#[derive(Args, Clone)]
pub struct GenostatsArgs {
    /// Input file or directory paths to process. Directories are scanned recursively.
    #[arg(short = 'i', long = "input")]
    pub inputs: Vec<PathBuf>,
    /// Path to the SQLite database used to store aggregated stats.
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Optional JSON file to dump a summary report.
    #[arg(long)]
    pub summary_json: Option<PathBuf>,
    /// Limit the number of files processed (useful for testing).
    #[arg(long)]
    pub max_files: Option<usize>,
    /// Skip files already recorded in the SQLite database.
    #[arg(long, action = ArgAction::SetTrue)]
    pub skip_recorded_files: bool,
    /// Number of worker threads to use when parsing files.
    #[arg(long, default_value = "16")]
    pub threads: usize,
}

#[derive(Args, Clone)]
pub struct AlleleReportArgs {
    /// Path to the SQLite database created by `bvs genostats` (uses data/genostats.sqlite in production).
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Output path for the generated HTML report.
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Args, Clone)]
pub struct ReferenceLoadArgs {
    /// Path to the SQLite database created by `bvs genostats`.
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// CSV produced by `scripts/extract_reference_variants.py`.
    #[arg(long)]
    pub lookup: PathBuf,
}

#[derive(Args, Clone)]
pub struct SyntheticArgs {
    /// Path to the SQLite database containing rsid_reference data (uses data/genostats.sqlite in production).
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Output file to write
    #[arg(long)]
    pub output: PathBuf,
    /// Probability of substituting a random ALT allele instead of the reference.
    #[arg(long, default_value = "0.01")]
    pub alt_frequency: f64,
    /// Optional RNG seed for reproducible output.
    #[arg(long)]
    pub seed: Option<u64>,
    /// Limit the number of rows emitted (defaults to all).
    #[arg(long)]
    pub limit: Option<usize>,
    /// Number of files to generate in parallel.
    #[arg(long, default_value = "1")]
    pub count: usize,
    /// Override number of worker threads for synthetic generation.
    #[arg(long)]
    pub threads: Option<usize>,
    /// Optional JSON file describing overlay variants to force/include.
    #[arg(long = "variants-file")]
    pub variants_file: Option<PathBuf>,
    /// Inline JSON describing overlay variants (use instead of --variants-file).
    #[arg(long = "variants-json")]
    pub variants_json: Option<String>,
    /// Minimum random participant ID (inclusive) when using {id} placeholder.
    #[arg(long, default_value_t = 100000)]
    pub id_min: u32,
    /// Maximum random participant ID (inclusive) when using {id} placeholder.
    #[arg(long, default_value_t = 999999)]
    pub id_max: u32,
    /// Year used for random date placeholders.
    #[arg(long, default_value_t = 2025)]
    pub date_year: i32,
    /// Minimum random month (1-12) for date placeholders.
    #[arg(long, default_value_t = 1)]
    pub month_min: u32,
    /// Maximum random month (1-12) for date placeholders.
    #[arg(long, default_value_t = 12)]
    pub month_max: u32,
    /// Minimum random day (1-28) for date placeholders.
    #[arg(long, default_value_t = 1)]
    pub day_min: u32,
    /// Maximum random day (1-28) for date placeholders.
    #[arg(long, default_value_t = 28)]
    pub day_max: u32,
    /// Date format string used for {date} placeholder (chrono format).
    #[arg(long, default_value = "%m-%d-%Y")]
    pub date_format: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Genostats(args) => run_genostats(args),
        Commands::AlleleReport(args) => run_allele_report(args),
        Commands::ReferenceLoad(args) => run_reference_load(args),
        Commands::Synthetic(args) => run_synthetic(args),
    }
}
