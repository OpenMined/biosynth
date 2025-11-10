#!/usr/bin/env python3
"""
Lookup rsid reference alleles for a genotype file against a GRCh38 dbSNP VCF.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pysam

CONTIG_MAP: Dict[str, str] = {
    "1": "NC_000001.11",
    "2": "NC_000002.12",
    "3": "NC_000003.12",
    "4": "NC_000004.12",
    "5": "NC_000005.10",
    "6": "NC_000006.12",
    "7": "NC_000007.14",
    "8": "NC_000008.11",
    "9": "NC_000009.12",
    "10": "NC_000010.11",
    "11": "NC_000011.10",
    "12": "NC_000012.12",
    "13": "NC_000013.11",
    "14": "NC_000014.9",
    "15": "NC_000015.10",
    "16": "NC_000016.10",
    "17": "NC_000017.11",
    "18": "NC_000018.10",
    "19": "NC_000019.10",
    "20": "NC_000020.11",
    "21": "NC_000021.9",
    "22": "NC_000022.11",
    "X": "NC_000023.11",
    "Y": "NC_000024.10",
    "M": "NC_012920.1",
    "MT": "NC_012920.1",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lookup rsid reference alleles from a GRCh38/dbSNP VCF."
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Genotype file with rsid, chromosome, position columns",
    )
    parser.add_argument(
        "--vcf",
        type=Path,
        default=Path("notebooks/data/GCF_000001405.40.gz"),
        help="bgzip-compressed dbSNP VCF (with .tbi index)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Where to write exact-match rows",
    )
    parser.add_argument(
        "--missing-output",
        type=Path,
        help="Where to write rows with no exact match",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N non-comment rows (for quick tests)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=0,
        help="If non-zero, search ±window bp when exact position misses",
    )
    return parser.parse_args()


def iter_genotype_rows(
    path: Path, limit: Optional[int]
) -> Iterator[Tuple[str, str, int]]:
    count = 0
    with path.open() as handle:
        for raw in handle:
            if raw.startswith("#"):
                continue
            parts = raw.strip().split("\t")
            if len(parts) < 3:
                continue
            rsid, chrom, pos = parts[0], parts[1], parts[2]
            try:
                pos_int = int(pos)
            except ValueError:
                continue
            yield rsid, chrom, pos_int
            count += 1
            if limit and count >= limit:
                break


def normalize_chrom(value: str) -> Optional[str]:
    clean = value.strip().upper()
    if clean.startswith("CHR"):
        clean = clean[3:]
    return CONTIG_MAP.get(clean)


def record_matches(rec: pysam.VariantRecord, target: str) -> bool:
    ids: List[str] = []
    if rec.id and rec.id != ".":
        ids.extend(part.strip() for part in rec.id.split(";") if part.strip())
    rs_info = rec.info.get("RS")
    if rs_info:
        if isinstance(rs_info, (list, tuple)):
            ids.extend(f"rs{int(v)}" for v in rs_info)
        else:
            ids.append(f"rs{int(rs_info)}")
    return target in ids


def fetch_variant(
    vf: pysam.VariantFile, contig: str, pos: int, rsid: str, window: int
) -> Tuple[Optional[pysam.VariantRecord], str]:
    if contig not in vf.header.contigs:
        return None, "missing_contig"

    def search(start: int, end: int) -> Optional[pysam.VariantRecord]:
        try:
            for rec in vf.fetch(contig, max(0, start), end):
                if record_matches(rec, rsid):
                    return rec
        except ValueError:
            return None
        return None

    rec = search(pos - 1, pos)
    if rec:
        return rec, "exact"
    if window > 0:
        rec = search(pos - 1 - window, pos + window)
        if rec:
            return rec, "window"
    return None, "not_found"


def build_row(
    query: Tuple[str, str, int],
    rec: Optional[pysam.VariantRecord],
    status: str,
) -> Dict[str, str]:
    rsid, chrom, pos = query
    output = {
        "query_rsid": rsid,
        "query_chrom": chrom,
        "query_pos": str(pos),
        "ref_contig": "",
        "ref_pos": "",
        "ref": "",
        "alt": "",
        "filters": "",
        "status": status,
    }
    if rec:
        output.update(
            {
                "ref_contig": rec.contig,
                "ref_pos": str(rec.pos),
                "ref": rec.ref or "",
                "alt": ",".join(rec.alts or []),
                "filters": ";".join(rec.filter.keys())
                if rec.filter is not None
                else "PASS",
            }
        )
    return output


def main() -> None:
    args = parse_args()

    if not args.input.exists():
        raise SystemExit(f"Missing genotype file: {args.input}")
    if not args.vcf.exists():
        raise SystemExit(f"Missing VCF file: {args.vcf}")

    vf = pysam.VariantFile(args.vcf)
    rows: Iterable[Tuple[str, str, int]] = iter_genotype_rows(args.input, args.limit)

    exact_path: Path = args.output
    missing_path: Path = args.missing_output
    exact_path.parent.mkdir(parents=True, exist_ok=True)
    missing_path.parent.mkdir(parents=True, exist_ok=True)

    exact_writer = None
    missing_writer = None
    exact_count = 0
    missing_count = 0

    with (
        exact_path.open("w", newline="") as exact_handle,
        missing_path.open("w", newline="") as missing_handle,
    ):
        for row in rows:
            rsid, chrom, pos = row
            contig = normalize_chrom(chrom)
            rec = None
            status = "no_contig"
            if contig:
                rec, status = fetch_variant(vf, contig, pos, rsid, args.window)
            csv_row = build_row(row, rec, status)
            if status == "exact":
                if exact_writer is None:
                    exact_writer = csv.DictWriter(
                        exact_handle, fieldnames=list(csv_row.keys())
                    )
                    exact_writer.writeheader()
                exact_writer.writerow(csv_row)
                exact_count += 1
            else:
                if missing_writer is None:
                    missing_writer = csv.DictWriter(
                        missing_handle, fieldnames=list(csv_row.keys())
                    )
                    missing_writer.writeheader()
                missing_writer.writerow(csv_row)
                missing_count += 1

    print(
        f"Wrote {exact_count} exact rows to {exact_path} "
        f"and {missing_count} non-exact rows to {missing_path}"
    )


if __name__ == "__main__":
    main()
