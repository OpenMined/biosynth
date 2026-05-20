use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

mod download;
mod genotype;
mod genotype_reader;
mod long_rows;
mod rsid_cache;
mod stats;
mod util;

use crate::commands::allele_report::run_allele_report;
use crate::commands::genostats::run_genostats;
use crate::commands::genotype_to_vcf::run_genotype_to_vcf;
use crate::commands::list_missing_cache::run_list_missing_cache;
use crate::commands::list_missing_rsids::run_list_missing_rsids;
use crate::commands::load_non_rsids::run_load_non_rsids;
use crate::commands::long_aggregate::run_long_aggregate;
use crate::commands::long_dump::run_long_dump;
use crate::commands::long_emit::run_long_emit;
use crate::commands::reference_load::run_reference_load;
use crate::commands::resolve_rsids::run_resolve_rsids;
use crate::commands::sync_rsid_cache::run_sync_rsid_cache;
use crate::commands::synthetic::run_synthetic;
use crate::commands::update::run_update;

mod commands {
    pub mod allele_report;
    pub mod genostats;
    pub mod genotype_to_vcf;
    pub mod list_missing_cache;
    pub mod list_missing_rsids;
    pub mod load_non_rsids;
    pub mod long_aggregate;
    pub mod long_dump;
    pub mod long_emit;
    pub mod reference_load;
    pub mod resolve_rsids;
    pub mod sync_rsid_cache;
    pub mod synthetic;
    pub mod update;
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
    /// Convert a genotype file into a single-sample VCF.
    GenotypeToVcf(GenotypeToVcfArgs),
    /// Emit compact long-row records from a VCF or genotype file.
    EmitLong(EmitLongArgs),
    /// Aggregate long-row records into matrix and allele frequency TSVs.
    AggregateLong(AggregateLongArgs),
    /// Convert a .bvlr file into TSV for debugging.
    DumpLong(DumpLongArgs),
    /// List rsids missing from the database based on a cache file.
    ListMissingCache(ListMissingCacheArgs),
    /// List rsids missing from the reference database.
    ListMissingRsids(ListMissingRsidsArgs),
    /// Resolve rsids from a missing list into a cache file.
    ResolveRsids(ResolveRsidsArgs),
    /// Load reference allele lookup data into SQLite.
    ReferenceLoad(ReferenceLoadArgs),
    /// Load resolved non-rsid markers into the grch38_non_rsids table.
    LoadNonRsids(LoadNonRsidsArgs),
    /// Sync rsid cache entries into the SQLite user override table.
    SyncRsidCache(SyncRsidCacheArgs),
    /// Generate a reference genotype file from stored data.
    Synthetic(SyntheticArgs),
    /// Update the bvs binary from the latest GitHub release.
    Update(UpdateArgs),
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
    /// Force re-download of the reference database.
    #[arg(long, action = ArgAction::SetTrue)]
    pub force_download: bool,
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
pub struct LoadNonRsidsArgs {
    /// Path to the SQLite database.
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Resolved CSV (…nonrsids.resolved.dedup.csv) to load.
    #[arg(long)]
    pub lookup: PathBuf,
    /// Source tag stored with each row.
    #[arg(long, default_value = "ensembl_grch38")]
    pub source: String,
}

#[derive(Args, Clone)]
pub struct GenotypeToVcfArgs {
    /// Input genotype file (tab/space/comma-delimited).
    #[arg(short = 'i', long = "input")]
    pub inputs: Vec<PathBuf>,
    /// Path to the SQLite database containing rsid_reference data.
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Force re-download of the reference database.
    #[arg(long, action = ArgAction::SetTrue)]
    pub force_download: bool,
    /// Output VCF path (single input only; defaults to <input>.vcf or <input>.vcf.gz).
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Output directory for multi-file conversions (defaults to each input directory).
    #[arg(long)]
    pub outdir: Option<PathBuf>,
    /// Sample name to use in the VCF header (defaults to input filename stem).
    #[arg(long)]
    pub sample: Option<String>,
    /// Optional log file for missing or invalid rows (defaults to stderr).
    #[arg(long)]
    pub missing_log: Option<PathBuf>,
    /// Gzip the output VCF (requires output path to end with .gz).
    #[arg(long, action = ArgAction::SetTrue)]
    pub gzip: bool,
    /// Include GS/BAF/LRR columns as FORMAT fields when present.
    #[arg(long, action = ArgAction::SetTrue)]
    pub include_metrics: bool,
    /// Optional rsid cache file for fallback lookups.
    #[arg(long)]
    pub cache: Option<PathBuf>,
}

#[derive(Args, Clone)]
pub struct EmitLongArgs {
    /// Input genotype file(s) (tab/space/comma-delimited).
    #[arg(short = 'i', long = "input")]
    pub inputs: Vec<PathBuf>,
    /// Input VCF file (plain or gz). Mutually exclusive with --input.
    #[arg(long)]
    pub vcf: Option<PathBuf>,
    /// Path to the SQLite database containing rsid_reference data.
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Force re-download of the reference database.
    #[arg(long, action = ArgAction::SetTrue)]
    pub force_download: bool,
    /// Output file (defaults to <input>.bvlr or <vcf>.bvlr).
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Participant id (defaults to filename stem or VCF sample name if present).
    #[arg(long)]
    pub participant: Option<String>,
    /// Optional log file for missing reference rows (appends).
    #[arg(long)]
    pub missing_ref_log: Option<PathBuf>,
}

#[derive(Args, Clone)]
pub struct AggregateLongArgs {
    /// Input files or directories containing .bvlr files.
    #[arg(short = 'i', long = "input")]
    pub inputs: Vec<PathBuf>,
    /// File containing input paths (one per line, # comments allowed).
    #[arg(long)]
    pub input_list: Option<PathBuf>,
    /// Glob pattern for input files (e.g. "*.bvlr").
    #[arg(long)]
    pub input_glob: Option<String>,
    /// Output matrix TSV path (omit to skip matrix generation).
    #[arg(long)]
    pub matrix_tsv: Option<PathBuf>,
    /// Output allele frequency TSV path.
    #[arg(long)]
    pub allele_freq_tsv: PathBuf,
    /// Optional temp directory (defaults to system temp).
    #[arg(long)]
    pub tmp_dir: Option<PathBuf>,
    /// Number of records per chunk during external sort (0 = auto).
    #[arg(long, default_value = "0")]
    pub chunk_records: usize,
    /// Max RAM percent to use when auto-sizing chunk records.
    #[arg(long, default_value = "80")]
    pub max_ram_percent: u8,
    /// Number of worker threads to use (0 = auto/all cores).
    #[arg(long, default_value = "0")]
    pub threads: usize,
}

#[derive(Args, Clone)]
pub struct DumpLongArgs {
    /// Input .bvlr file to dump.
    #[arg(long)]
    pub input: PathBuf,
    /// Output TSV path.
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Args, Clone)]
pub struct ListMissingRsidsArgs {
    /// Input file or directory paths to scan.
    #[arg(short = 'i', long = "input")]
    pub inputs: Vec<PathBuf>,
    /// Path to the SQLite database containing rsid_reference data.
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Output file for missing rsids (tsv or jsonl).
    #[arg(long)]
    pub missing_file: PathBuf,
    /// Output format: tsv or jsonl.
    #[arg(long, default_value = "tsv")]
    pub format: String,
}

#[derive(Args, Clone)]
pub struct ListMissingCacheArgs {
    /// Cache file containing rsid -> ref/alt mappings.
    #[arg(long)]
    pub cache: Option<PathBuf>,
    /// Path to the SQLite database containing rsid_reference data.
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Output file for missing rsids (one per line).
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Args, Clone)]
pub struct ResolveRsidsArgs {
    /// Missing rsids file generated by list-missing-rsids.
    #[arg(long)]
    pub missing_file: PathBuf,
    /// Cache file to read/write rsid resolutions.
    #[arg(long)]
    pub cache: Option<PathBuf>,
    /// Local dbSNP VCF (.vcf.gz) to resolve rsids offline.
    #[arg(long)]
    pub dbsnp: Option<PathBuf>,
    /// Allow Ensembl API fallback lookups.
    #[arg(long, action = ArgAction::SetTrue)]
    pub web: bool,
    /// Only use existing cache entries (no dbSNP or web lookups).
    #[arg(long, action = ArgAction::SetTrue)]
    pub cache_only: bool,
    /// Maximum threads for web lookups.
    #[arg(long, default_value_t = 8)]
    pub threads: usize,
    /// Retry count for web lookups.
    #[arg(long, default_value_t = 5)]
    pub retry: usize,
    /// Optional local file with rsid/ref/alt overrides (tsv or json).
    #[arg(long)]
    pub local_file: Option<PathBuf>,
}

#[derive(Args, Clone)]
pub struct SyncRsidCacheArgs {
    /// Cache file to sync into SQLite.
    #[arg(long)]
    pub cache: PathBuf,
    /// Path to the SQLite database containing rsid_reference data.
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Optional missing file to supply chrom/pos for cache entries.
    #[arg(long)]
    pub missing_file: Option<PathBuf>,
    /// Apply changes to SQLite (default: dry run).
    #[arg(long, action = ArgAction::SetTrue)]
    pub apply: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyntheticFormat {
    /// Dynamic DNA (DDNA) plus-strand genotype text (default).
    Ddna,
    /// Illumina GenomeStudio GSGT Final Report.
    Illumina,
}

#[derive(Args, Clone)]
pub struct SyntheticArgs {
    /// Output file format.
    #[arg(long = "format", value_enum, default_value_t = SyntheticFormat::Ddna)]
    pub format: SyntheticFormat,
    /// Path to the SQLite database containing rsid_reference data (uses data/genostats.sqlite in production).
    #[arg(long, default_value = "data/genostats.sqlite")]
    pub sqlite: PathBuf,
    /// Force re-download of the reference database.
    #[arg(long, action = ArgAction::SetTrue)]
    pub force_download: bool,
    /// Output file to write
    #[arg(long)]
    pub output: PathBuf,
    /// ALT allele frequency used to sample diploid genotypes under Hardy-Weinberg equilibrium.
    #[arg(long, default_value = "0.01")]
    pub alt_frequency: f64,
    /// Probability of emitting a no-call genotype ("--") for a row.
    #[arg(long, default_value = "0.0")]
    pub no_call_frequency: f64,
    /// Constrain each rsID to one cohort-level biallelic allele pair.
    #[arg(long, action = ArgAction::SetTrue)]
    pub biallelic: bool,
    /// Genotype token used for no-call rows (e.g. "--" or ".").
    #[arg(long, default_value = "--")]
    pub no_call_token: String,
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

#[derive(Args, Clone)]
pub struct UpdateArgs {
    /// Specific version tag to install (e.g. v0.1.9). Defaults to latest.
    #[arg(long)]
    pub version: Option<String>,
    /// Installation directory (defaults to ~/.local/bin).
    #[arg(long)]
    pub install_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Genostats(args) => run_genostats(args),
        Commands::AlleleReport(args) => run_allele_report(args),
        Commands::GenotypeToVcf(args) => run_genotype_to_vcf(args),
        Commands::EmitLong(args) => run_long_emit(args),
        Commands::AggregateLong(args) => run_long_aggregate(args),
        Commands::DumpLong(args) => run_long_dump(args),
        Commands::ListMissingCache(args) => run_list_missing_cache(args),
        Commands::ListMissingRsids(args) => run_list_missing_rsids(args),
        Commands::ResolveRsids(args) => run_resolve_rsids(args),
        Commands::ReferenceLoad(args) => run_reference_load(args),
        Commands::LoadNonRsids(args) => run_load_non_rsids(args),
        Commands::SyncRsidCache(args) => run_sync_rsid_cache(args),
        Commands::Synthetic(args) => run_synthetic(args),
        Commands::Update(args) => run_update(args),
    }
}
