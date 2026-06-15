use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::long_rows::{locus_key, normalize_sequence, LongRow};

const BED_MAGIC: [u8; 3] = [0x6c, 0x1b, 0x01];

#[derive(Debug, Clone)]
pub struct PlinkSample {
    pub fid: String,
    pub iid: String,
}

#[derive(Debug, Clone)]
pub struct PlinkVariant {
    pub chrom: String,
    pub id: String,
    pub pos: i64,
    pub a1: String,
    pub a2: String,
}

#[derive(Debug, Clone)]
pub struct PlinkInfo {
    pub samples: usize,
    pub variants: usize,
    pub bed_bytes: u64,
    pub expected_bed_bytes: u64,
    pub bytes_per_variant: usize,
    pub snp_variants: usize,
}

#[derive(Debug, Default, Clone)]
pub struct PlinkScanStats {
    pub variants_seen: u64,
    pub variants_emitted: u64,
    pub rows_emitted: u64,
    pub missing_calls: u64,
    pub skipped_non_snp: u64,
    pub skipped_bad_position: u64,
}

pub fn plink_paths(prefix: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let with_ext = |ext: &str| {
        let mut path = prefix.as_os_str().to_owned();
        path.push(ext);
        PathBuf::from(path)
    };
    (with_ext(".bed"), with_ext(".bim"), with_ext(".fam"))
}

pub fn read_fam(path: &Path) -> Result<Vec<PlinkSample>> {
    let file = File::open(path).with_context(|| format!("Open PLINK FAM {:?}", path))?;
    let reader = BufReader::new(file);
    let mut samples = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let fields: Vec<&str> = trimmed.split_whitespace().collect();
        if fields.len() < 2 {
            bail!(
                "Invalid FAM row {} in {:?}: expected at least 2 columns",
                idx + 1,
                path
            );
        }
        samples.push(PlinkSample {
            fid: fields[0].to_string(),
            iid: fields[1].to_string(),
        });
    }
    Ok(samples)
}

pub fn read_bim(path: &Path) -> Result<Vec<PlinkVariant>> {
    let file = File::open(path).with_context(|| format!("Open PLINK BIM {:?}", path))?;
    let reader = BufReader::new(file);
    let mut variants = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let fields: Vec<&str> = trimmed.split_whitespace().collect();
        if fields.len() < 6 {
            bail!(
                "Invalid BIM row {} in {:?}: expected 6 columns",
                idx + 1,
                path
            );
        }
        variants.push(PlinkVariant {
            chrom: fields[0].to_string(),
            id: fields[1].to_string(),
            pos: fields[3].parse::<i64>().with_context(|| {
                format!("Invalid BIM position on row {} in {:?}", idx + 1, path)
            })?,
            a1: fields[4].to_string(),
            a2: fields[5].to_string(),
        });
    }
    Ok(variants)
}

pub fn inspect_plink_prefix(prefix: &Path) -> Result<PlinkInfo> {
    let (bed_path, bim_path, fam_path) = plink_paths(prefix);
    let samples = read_fam(&fam_path)?;
    let variants = read_bim(&bim_path)?;
    let bytes_per_variant = samples.len().div_ceil(4);
    let expected_bed_bytes = 3 + (variants.len() * bytes_per_variant) as u64;
    let bed_bytes = std::fs::metadata(&bed_path)
        .with_context(|| format!("Stat PLINK BED {:?}", bed_path))?
        .len();
    validate_bed_header_and_size(&bed_path, expected_bed_bytes)?;
    let snp_variants = variants
        .iter()
        .filter(|v| valid_snp_alleles(&v.a2, &v.a1) && v.pos > 0)
        .count();
    Ok(PlinkInfo {
        samples: samples.len(),
        variants: variants.len(),
        bed_bytes,
        expected_bed_bytes,
        bytes_per_variant,
        snp_variants,
    })
}

pub fn variant_locus_key(variant: &PlinkVariant) -> Option<String> {
    if variant.pos <= 0 || !valid_snp_alleles(&variant.a2, &variant.a1) {
        return None;
    }
    let reference = normalize_sequence(&variant.a2);
    let alternate = normalize_sequence(&variant.a1);
    Some(locus_key(
        &variant.chrom,
        variant.pos,
        &reference,
        &alternate,
    ))
}

pub fn stream_plink_long_rows<F>(prefix: &Path, mut on_row: F) -> Result<PlinkScanStats>
where
    F: FnMut(LongRow) -> Result<()>,
{
    let (bed_path, bim_path, fam_path) = plink_paths(prefix);
    let samples = read_fam(&fam_path)?;
    let variants = read_bim(&bim_path)?;
    if samples.is_empty() {
        bail!("PLINK FAM has no samples: {:?}", fam_path);
    }
    let bytes_per_variant = samples.len().div_ceil(4);
    let expected_bed_bytes = 3 + (variants.len() * bytes_per_variant) as u64;
    validate_bed_header_and_size(&bed_path, expected_bed_bytes)?;

    let mut bed =
        File::open(&bed_path).with_context(|| format!("Open PLINK BED {:?}", bed_path))?;
    let mut header = [0u8; 3];
    bed.read_exact(&mut header)?;

    let mut stats = PlinkScanStats::default();
    let mut row_bytes = vec![0u8; bytes_per_variant];
    for variant in variants {
        bed.read_exact(&mut row_bytes)
            .with_context(|| format!("Read BED genotypes for {}", variant.id))?;
        stats.variants_seen += 1;

        let locus = match variant_locus_key(&variant) {
            Some(locus) => locus,
            None if variant.pos <= 0 => {
                stats.skipped_bad_position += 1;
                continue;
            }
            None => {
                stats.skipped_non_snp += 1;
                continue;
            }
        };

        let alternate = normalize_sequence(&variant.a1);
        if alternate.is_empty() {
            continue;
        };
        stats.variants_emitted += 1;

        for (sample_idx, sample) in samples.iter().enumerate() {
            let two_bit = (row_bytes[sample_idx / 4] >> ((sample_idx % 4) * 2)) & 0b11;
            let dosage = match two_bit {
                0b00 => 2,
                0b10 => 1,
                0b11 => 0,
                0b01 => {
                    stats.missing_calls += 1;
                    -1
                }
                _ => unreachable!(),
            };
            on_row(LongRow {
                locus_key: locus.clone(),
                rsid: clean_plink_id(&variant.id),
                participant_id: sample_id(sample),
                dosage,
            })?;
            stats.rows_emitted += 1;
        }
    }
    Ok(stats)
}

fn validate_bed_header_and_size(path: &Path, expected_bed_bytes: u64) -> Result<()> {
    let mut file = File::open(path).with_context(|| format!("Open PLINK BED {:?}", path))?;
    let mut header = [0u8; 3];
    file.read_exact(&mut header)
        .with_context(|| format!("Read PLINK BED header {:?}", path))?;
    if header != BED_MAGIC {
        bail!(
            "Unsupported PLINK BED {:?}: expected SNP-major magic 6c 1b 01, found {:02x} {:02x} {:02x}",
            path,
            header[0],
            header[1],
            header[2]
        );
    }
    let actual = std::fs::metadata(path)?.len();
    if actual != expected_bed_bytes {
        bail!(
            "PLINK BED size mismatch for {:?}: expected {} bytes from BIM/FAM dimensions, found {}",
            path,
            expected_bed_bytes,
            actual
        );
    }
    Ok(())
}

fn valid_snp_alleles(reference: &str, alternate: &str) -> bool {
    let reference = normalize_sequence(reference);
    let alternate = normalize_sequence(alternate);
    is_base(&reference) && is_base(&alternate) && reference != alternate
}

fn is_base(value: &str) -> bool {
    matches!(value, "A" | "C" | "G" | "T")
}

pub fn clean_plink_id(id: &str) -> String {
    if id == "." {
        String::new()
    } else {
        id.to_string()
    }
}

fn sample_id(sample: &PlinkSample) -> String {
    if sample.fid == "0" || sample.fid == sample.iid {
        sample.iid.clone()
    } else {
        format!("{}:{}", sample.fid, sample.iid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("bvs-plink-{label}-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn streams_bed_dosages_as_long_rows() {
        let dir = unique_test_dir("stream");
        let prefix = dir.join("test");
        fs::write(
            prefix.with_extension("fam"),
            "F1 S1 0 0 0 -9\nF1 S2 0 0 0 -9\nF1 S3 0 0 0 -9\n",
        )
        .unwrap();
        fs::write(prefix.with_extension("bim"), "1 rs1 0 100 G A\n").unwrap();
        let mut bed = File::create(prefix.with_extension("bed")).unwrap();
        bed.write_all(&[0x6c, 0x1b, 0x01]).unwrap();
        // S1: homozygous A1 (G) => 2, S2: hetero => 1, S3: homozygous A2 (A) => 0.
        bed.write_all(&[0b00 | (0b10 << 2) | (0b11 << 4)]).unwrap();

        let mut rows = Vec::new();
        let stats = stream_plink_long_rows(&prefix, |row| {
            rows.push(row);
            Ok(())
        })
        .unwrap();

        assert_eq!(stats.variants_seen, 1);
        assert_eq!(stats.rows_emitted, 3);
        assert_eq!(rows[0].locus_key, "1-100-A-G");
        assert_eq!(rows[0].participant_id, "F1:S1");
        assert_eq!(rows[0].dosage, 2);
        assert_eq!(rows[1].dosage, 1);
        assert_eq!(rows[2].dosage, 0);
    }
}
