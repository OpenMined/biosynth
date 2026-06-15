#!/usr/bin/env python3
"""Export BVS allele-frequency TSVs for 1KG PLINK2 super-populations.

This reads PLINK2 .pvar/.pvar.zst INFO fields (AC_POP, AN_POP, AC_Het_POP,
AC_Hom_POP), optionally filters by a tracked loci TSV, and writes one standard
BVS allele-frequency TSV per super-population.
"""

from __future__ import annotations

import argparse
import csv
import subprocess
from pathlib import Path


CHROM_ORDER = [str(i) for i in range(1, 23)] + ["X", "Y", "M", "contigs"]
DEFAULT_POPS = ["AFR", "AMR", "EAS", "EUR", "SAS"]


def pvar_name(chrom: str) -> str:
    return f"chr{chrom}_hg38_rs.pvar.zst" if chrom != "contigs" else "contigs_hg38_rs.pvar.zst"


def discover_pvars(data_dir: Path) -> list[Path]:
    paths = []
    for chrom in CHROM_ORDER:
        path = data_dir / pvar_name(chrom)
        if path.exists():
            paths.append(path)
    return paths


def open_zstd_text(path: Path):
    proc = subprocess.Popen(
        ["zstd", "-dcf", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    return proc, proc.stdout


def parse_info(info: str) -> dict[str, str]:
    values = {}
    if not info or info == ".":
        return values
    for item in info.split(";"):
        if "=" in item:
            key, value = item.split("=", 1)
            values[key] = value
    return values


def info_int(info: dict[str, str], key: str, alt_idx: int) -> int | None:
    value = info.get(key)
    if not value or value == ".":
        return None
    parts = value.split(",")
    piece = parts[alt_idx] if alt_idx < len(parts) else parts[0]
    if not piece or piece == ".":
        return None
    try:
        return int(piece)
    except ValueError:
        return None


def is_snv(ref: str, alt: str) -> bool:
    return (
        len(ref) == 1
        and len(alt) == 1
        and ref in {"A", "C", "G", "T"}
        and alt in {"A", "C", "G", "T"}
    )


def locus_key(chrom: str, pos: str, ref: str, alt: str) -> str:
    return f"{chrom}-{pos}-{ref}-{alt}"


def load_loci_filter(path: Path) -> set[str]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        fieldnames = reader.fieldnames or []
        norm = {name.lower().lstrip("\ufeff"): name for name in fieldnames}
        keys = set()
        if "locus_key" in norm:
            col = norm["locus_key"]
            for row in reader:
                key = (row.get(col) or "").strip()
                if key:
                    keys.add(key)
        else:
            required = ["chrom", "pos", "ref", "alt"]
            missing = [name for name in required if name not in norm]
            if missing:
                raise SystemExit(
                    f"{path} must contain locus_key or chrom/pos/ref/alt columns; missing {missing}"
                )
            for row in reader:
                chrom = (row.get(norm["chrom"]) or "").strip()
                pos = (row.get(norm["pos"]) or "").strip()
                ref = (row.get(norm["ref"]) or "").strip()
                alt = (row.get(norm["alt"]) or "").strip()
                if chrom and pos and ref and alt:
                    keys.add(locus_key(chrom, pos, ref, alt))
        if not keys:
            raise SystemExit(f"No loci loaded from {path}")
        return keys


def init_writers(out_dir: Path, pops: list[str]):
    out_dir.mkdir(parents=True, exist_ok=True)
    handles = {}
    writers = {}
    for pop in pops:
        path = out_dir / f"1kg_grch38_{pop}.allele_freq.tsv"
        handle = path.open("w", newline="")
        handles[pop] = handle
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "locus_key",
                "allele_count",
                "allele_number",
                "num_homo",
                "num_hetero",
                "allele_freq",
                "rsid",
            ]
        )
        writers[pop] = writer
    return handles, writers


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--loci-filter", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--pop", action="append", default=None)
    parser.add_argument("--include-non-snv", action="store_true")
    args = parser.parse_args()

    pops = args.pop or DEFAULT_POPS
    loci_filter = load_loci_filter(args.loci_filter)
    print(f"loci_filter\t{len(loci_filter)}", flush=True)

    pvars = discover_pvars(args.data_dir)
    if not pvars:
        raise SystemExit(f"No pvar shards found in {args.data_dir}")

    handles, writers = init_writers(args.out_dir, pops)
    variants_seen = 0
    emitted = 0
    try:
        for path in pvars:
            print(f"pvar\t{path}", flush=True)
            proc, stream = open_zstd_text(path)
            header = None
            for raw in stream:
                line = raw.rstrip("\n")
                if not line or line.startswith("##"):
                    continue
                if line.startswith("#"):
                    header = line.lstrip("#").split("\t")
                    continue
                if header is None:
                    raise RuntimeError(f"No #CHROM header found in {path}")
                row = dict(zip(header, line.split("\t")))
                variants_seen += 1
                chrom = row.get("CHROM", "")
                pos = row.get("POS", "")
                rsid = "" if row.get("ID", "") == "." else row.get("ID", "")
                ref = row.get("REF", "")
                alts = [alt for alt in row.get("ALT", "").split(",") if alt and alt != "."]
                info = parse_info(row.get("INFO", "."))
                for idx, alt in enumerate(alts):
                    if not args.include_non_snv and not is_snv(ref, alt):
                        continue
                    key = locus_key(chrom, pos, ref, alt)
                    if key not in loci_filter:
                        continue
                    for pop in pops:
                        ac = info_int(info, f"AC_{pop}", idx)
                        an = info_int(info, f"AN_{pop}", 0)
                        if ac is None or an is None:
                            continue
                        num_hetero = info_int(info, f"AC_Het_{pop}", idx) or 0
                        hom_alt_alleles = info_int(info, f"AC_Hom_{pop}", idx) or 0
                        af = ac / an if an else 0.0
                        writers[pop].writerow(
                            [key, ac, an, hom_alt_alleles // 2, num_hetero, f"{af:.6f}", rsid]
                        )
                    emitted += 1
            stderr = proc.stderr.read() if proc.stderr else ""
            ret = proc.wait()
            if ret != 0:
                raise RuntimeError(f"zstd failed for {path}: {stderr.strip()}")
    finally:
        for handle in handles.values():
            handle.close()

    print(f"variants_seen\t{variants_seen}")
    print(f"tracked_loci_emitted\t{emitted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
