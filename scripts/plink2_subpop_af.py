#!/usr/bin/env python3
"""Compute BVS allele-frequency TSVs per PSAM population from PLINK2 PGEN shards.

This decodes hard calls from .pgen/.pgen.zst with pgenlib, groups samples by the
PSAM Population column, and writes one standard BVS allele-frequency TSV per
population. The current implementation emits biallelic SNVs only.
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import numpy as np
import pgenlib


CHROM_ORDER = [str(i) for i in range(1, 23)] + ["X", "Y", "M", "contigs"]


def pvar_name(chrom: str) -> str:
    return f"chr{chrom}_hg38_rs.pvar.zst" if chrom != "contigs" else "contigs_hg38_rs.pvar.zst"


def pgen_name(chrom: str) -> str:
    return f"chr{chrom}_hg38.pgen.zst" if chrom != "contigs" else "contigs_hg38.pgen.zst"


def discover_shards(data_dir: Path, chroms: set[str] | None) -> list[tuple[str, Path, Path]]:
    shards = []
    for chrom in CHROM_ORDER:
        if chroms and chrom not in chroms:
            continue
        pgen = data_dir / pgen_name(chrom)
        pvar = data_dir / pvar_name(chrom)
        if pgen.exists() and pvar.exists():
            shards.append((chrom, pgen, pvar))
    return shards


def load_population_indices(psam: Path) -> tuple[dict[str, np.ndarray], dict[str, int]]:
    groups: dict[str, list[int]] = defaultdict(list)
    with psam.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        reader.fieldnames = ["IID" if f == "#IID" else f for f in (reader.fieldnames or [])]
        for idx, row in enumerate(reader):
            pop = row.get("Population", "").strip()
            if pop:
                groups[pop].append(idx)
    arrays = {pop: np.asarray(indices, dtype=np.int32) for pop, indices in sorted(groups.items())}
    counts = {pop: len(indices) for pop, indices in groups.items()}
    return arrays, counts


def decompress_zst(source: Path, dest: Path) -> None:
    with dest.open("wb") as out:
        subprocess.run(["zstd", "-dcf", str(source)], stdout=out, check=True)


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


def iter_pvar_rows(path: Path):
    proc, stream = open_zstd_text(path)
    header = None
    try:
        for raw in stream:
            line = raw.rstrip("\n")
            if not line or line.startswith("##"):
                continue
            if line.startswith("#"):
                header = line.lstrip("#").split("\t")
                continue
            if header is None:
                raise RuntimeError(f"No #CHROM header found in {path}")
            yield dict(zip(header, line.split("\t")))
    finally:
        stderr = proc.stderr.read() if proc.stderr else ""
        ret = proc.wait()
        if ret != 0:
            raise RuntimeError(f"zstd failed for {path}: {stderr.strip()}")


def is_biallelic_snv(row: dict[str, str]) -> bool:
    ref = row.get("REF", "")
    alt = row.get("ALT", "")
    return (
        len(ref) == 1
        and len(alt) == 1
        and ref in {"A", "C", "G", "T"}
        and alt in {"A", "C", "G", "T"}
    )


def locus_key(row: dict[str, str]) -> str:
    return f"{row['CHROM']}-{row['POS']}-{row['REF']}-{row['ALT']}"


def clean_rsid(value: str) -> str:
    return "" if value == "." else value


def init_writers(out_dir: Path, populations: list[str]):
    out_dir.mkdir(parents=True, exist_ok=True)
    writers = {}
    handles = {}
    for pop in populations:
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
    return writers, handles


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--pythonpath", default="/tmp/bvs_pgenlib")
    parser.add_argument("--chrom", action="append", help="Chromosome to process; repeatable.")
    parser.add_argument("--tmp-dir", type=Path, default=Path(tempfile.gettempdir()) / "bvs-pgen")
    parser.add_argument("--pop", action="append", help="Population code to emit; default all.")
    args = parser.parse_args()

    psam = args.data_dir / "hg38_corrected.psam"
    pop_indices, pop_counts = load_population_indices(psam)
    if args.pop:
        wanted = set(args.pop)
        pop_indices = {pop: idx for pop, idx in pop_indices.items() if pop in wanted}
    populations = sorted(pop_indices)
    if not populations:
        raise SystemExit("No populations selected")

    chroms = set(args.chrom) if args.chrom else None
    shards = discover_shards(args.data_dir, chroms)
    if not shards:
        raise SystemExit("No matching pgen/pvar shards found")

    args.tmp_dir.mkdir(parents=True, exist_ok=True)
    writers, handles = init_writers(args.out_dir, populations)
    stats = defaultdict(int)

    try:
        for chrom, pgen_zst, pvar_zst in shards:
            tmp_pgen = args.tmp_dir / f"{pgen_zst.stem}"
            print(f"decompress\t{pgen_zst}\t{tmp_pgen}", file=sys.stderr, flush=True)
            decompress_zst(pgen_zst, tmp_pgen)
            try:
                reader = pgenlib.PgenReader(bytes(tmp_pgen))
                dosages = np.empty(reader.get_raw_sample_ct(), dtype=np.int8)
                variant_idx = 0
                emitted = 0
                for row in iter_pvar_rows(pvar_zst):
                    reader.read(variant_idx, dosages)
                    variant_idx += 1
                    stats["variants_seen"] += 1
                    if not is_biallelic_snv(row):
                        stats["skipped_non_biallelic_snv"] += 1
                        continue
                    lk = locus_key(row)
                    rsid = clean_rsid(row.get("ID", ""))
                    for pop in populations:
                        vals = dosages[pop_indices[pop]]
                        called = vals >= 0
                        n_called = int(called.sum())
                        if n_called == 0:
                            ac = an = hom = het = 0
                            af = 0.0
                        else:
                            called_vals = vals[called]
                            ac = int(called_vals.sum())
                            an = 2 * n_called
                            hom = int((called_vals == 2).sum())
                            het = int((called_vals == 1).sum())
                            af = ac / an if an else 0.0
                        writers[pop].writerow([lk, ac, an, hom, het, f"{af:.6f}", rsid])
                    emitted += 1
                reader.close()
                stats["alleles_emitted"] += emitted
                print(
                    f"done\tchr{chrom}\tvariants={variant_idx}\temitted={emitted}",
                    file=sys.stderr,
                    flush=True,
                )
            finally:
                tmp_pgen.unlink(missing_ok=True)
    finally:
        for handle in handles.values():
            handle.close()

    for key, value in sorted(stats.items()):
        print(f"{key}\t{value}", file=sys.stderr)
    for pop in populations:
        print(f"population\t{pop}\tsamples\t{pop_counts.get(pop, 0)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
