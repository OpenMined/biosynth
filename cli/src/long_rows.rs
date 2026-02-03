use std::io::{self, Read, Write};

use anyhow::{bail, Context, Result};

const MAGIC: &[u8; 4] = b"BVLR";
const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LongRow {
    pub locus_key: String,
    pub rsid: String,
    pub participant_id: String,
    pub dosage: i8,
}

pub struct LongRowWriter<W: Write> {
    writer: W,
    header_written: bool,
}

impl<W: Write> LongRowWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            header_written: false,
        }
    }

    pub fn write_header(&mut self) -> Result<()> {
        if self.header_written {
            return Ok(());
        }
        self.writer.write_all(MAGIC)?;
        self.writer.write_all(&[VERSION])?;
        self.header_written = true;
        Ok(())
    }

    pub fn write_row(&mut self, row: &LongRow) -> Result<()> {
        self.write_header()?;
        write_string(&mut self.writer, &row.locus_key)?;
        write_string(&mut self.writer, &row.rsid)?;
        write_string(&mut self.writer, &row.participant_id)?;
        self.writer.write_all(&[row.dosage as u8])?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("flush long-row writer")
    }
}

pub struct LongRowReader<R: Read> {
    reader: R,
    header_read: bool,
}

impl<R: Read> LongRowReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            header_read: false,
        }
    }

    pub fn read_row(&mut self) -> Result<Option<LongRow>> {
        if !self.header_read {
            self.read_header()?;
        }

        let locus_key = match read_string(&mut self.reader)? {
            Some(value) => value,
            None => return Ok(None),
        };
        let rsid = read_string_required(&mut self.reader, "rsid")?;
        let participant_id = read_string_required(&mut self.reader, "participant_id")?;
        let dosage = read_i8(&mut self.reader)?;
        Ok(Some(LongRow {
            locus_key,
            rsid,
            participant_id,
            dosage,
        }))
    }

    fn read_header(&mut self) -> Result<()> {
        let mut magic = [0u8; 4];
        self.reader
            .read_exact(&mut magic)
            .context("read long-row header")?;
        if &magic != MAGIC {
            bail!("Invalid long-row file header");
        }
        let mut version = [0u8; 1];
        self.reader.read_exact(&mut version)?;
        if version[0] != VERSION {
            bail!("Unsupported long-row version: {}", version[0]);
        }
        self.header_read = true;
        Ok(())
    }
}

fn write_string<W: Write>(writer: &mut W, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let len = bytes.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(bytes)?;
    Ok(())
}

fn read_string<R: Read>(reader: &mut R) -> Result<Option<String>> {
    let mut len_bytes = [0u8; 4];
    match reader.read_exact(&mut len_bytes) {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err).context("read string length"),
    }
    let len = u32::from_le_bytes(len_bytes) as usize;
    let mut buffer = vec![0u8; len];
    reader.read_exact(&mut buffer)?;
    let value = String::from_utf8(buffer).context("decode long-row string")?;
    Ok(Some(value))
}

fn read_string_required<R: Read>(reader: &mut R, label: &str) -> Result<String> {
    read_string(reader)?.ok_or_else(|| anyhow::anyhow!("Unexpected EOF reading {label}"))
}

fn read_i8<R: Read>(reader: &mut R) -> Result<i8> {
    let mut buffer = [0u8; 1];
    reader.read_exact(&mut buffer)?;
    Ok(buffer[0] as i8)
}

pub fn is_snp(reference: &str, alts: &[String]) -> bool {
    reference.len() == 1 && alts.iter().all(|alt| alt.len() == 1)
}

pub fn parse_vcf_gt_dosages(gt: &str, alt_count: usize) -> Option<Vec<i8>> {
    if gt == "." || gt == "./." || gt == ".|." {
        return None;
    }
    let cleaned = gt.replace('|', "/");
    let parts: Vec<&str> = cleaned.split('/').collect();
    if parts.len() != 2 || parts.iter().any(|part| part == &".") {
        return None;
    }
    let a = parts[0].parse::<usize>().ok()?;
    let b = parts[1].parse::<usize>().ok()?;
    if a > alt_count || b > alt_count {
        return None;
    }
    let mut counts = vec![0i8; alt_count];
    if a > 0 {
        counts[a - 1] += 1;
    }
    if b > 0 {
        counts[b - 1] += 1;
    }
    Some(counts)
}

pub fn parse_genotype_dosages(
    genotype: Option<&str>,
    reference: &str,
    alternates: &[String],
) -> Option<Vec<i8>> {
    let genotype = genotype?.trim().to_uppercase();
    if genotype.is_empty() || matches!(genotype.as_str(), "--" | "NN" | "00" | ".." | ".") {
        return None;
    }

    let alleles = split_genotype_tokens(&genotype)?;
    let idx1 = allele_index(&alleles.0, reference, alternates)?;
    let idx2 = allele_index(&alleles.1, reference, alternates)?;
    let mut counts = vec![0i8; alternates.len()];
    if idx1 > 0 {
        counts[idx1 - 1] += 1;
    }
    if idx2 > 0 {
        counts[idx2 - 1] += 1;
    }
    Some(counts)
}

fn split_genotype_tokens(genotype: &str) -> Option<(String, String)> {
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

fn allele_index(allele: &str, reference: &str, alternates: &[String]) -> Option<usize> {
    if let Ok(idx) = allele.parse::<usize>() {
        if idx <= alternates.len() {
            return Some(idx);
        }
    }
    if allele == reference {
        return Some(0);
    }
    alternates
        .iter()
        .position(|alt| alt == allele)
        .map(|idx| idx + 1)
}

pub fn vcf_sample_name(header_line: &str) -> Option<String> {
    let parts: Vec<&str> = header_line.trim_end().split('\t').collect();
    if parts.len() >= 10 {
        Some(parts[9].to_string())
    } else {
        None
    }
}

pub fn normalize_sequence(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

pub fn locus_key(chrom: &str, pos: i64, reference: &str, alternate: &str) -> String {
    format!("{chrom}-{pos}-{reference}-{alternate}")
}

pub fn parse_alternates(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(normalize_sequence)
        .filter(|alt| !alt.is_empty() && alt != ".")
        .collect()
}

pub fn vcf_gt_from_sample(sample: &str) -> Option<&str> {
    sample.split(':').next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn long_row_roundtrip() {
        let mut buf = Vec::new();
        {
            let mut writer = LongRowWriter::new(&mut buf);
            writer
                .write_row(&LongRow {
                    locus_key: "1-100-A-G".to_string(),
                    rsid: "rs1".to_string(),
                    participant_id: "P1".to_string(),
                    dosage: 2,
                })
                .unwrap();
            writer.flush().unwrap();
        }
        let mut reader = LongRowReader::new(Cursor::new(buf));
        let row = reader.read_row().unwrap().unwrap();
        assert_eq!(
            row,
            LongRow {
                locus_key: "1-100-A-G".to_string(),
                rsid: "rs1".to_string(),
                participant_id: "P1".to_string(),
                dosage: 2,
            }
        );
        assert!(reader.read_row().unwrap().is_none());
    }

    #[test]
    fn parse_vcf_gt() {
        let dosages = parse_vcf_gt_dosages("1/2", 2).unwrap();
        assert_eq!(dosages, vec![1, 1]);
        let dosages = parse_vcf_gt_dosages("2/2", 2).unwrap();
        assert_eq!(dosages, vec![0, 2]);
        assert!(parse_vcf_gt_dosages("./.", 2).is_none());
    }

    #[test]
    fn parse_genotype() {
        let alts = vec!["C".to_string(), "G".to_string()];
        let dosages = parse_genotype_dosages(Some("A/G"), "A", &alts).unwrap();
        assert_eq!(dosages, vec![0, 1]);
        let dosages = parse_genotype_dosages(Some("CC"), "A", &alts).unwrap();
        assert_eq!(dosages, vec![2, 0]);
        assert!(parse_genotype_dosages(Some("--"), "A", &alts).is_none());
    }
}
