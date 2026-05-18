#!/usr/bin/env python3
"""Split a Carigenetics (Illumina GSGT Final Report) file into marker buckets.

Outputs (TSV, written next to --outdir):
  <stem>.rsids.tsv      rs-derivable markers   (rsid chrom pos a1 a2 genotype snp_name design)
  <stem>.nonrsids.tsv   no rs id, mapped       (snp_name chrom pos a1 a2 genotype design)
  <stem>.unmapped.tsv   chrom/pos == 0         (snp_name chrom pos a1 a2 design)

The nonrsids file is the input for query_dbsnp_nonrsids.py.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from carigenetics import iter_rows  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input", type=Path, help="Carigenetics raw data .txt")
    ap.add_argument("--outdir", type=Path, default=None,
                    help="Output directory (default: alongside input)")
    args = ap.parse_args()

    outdir = args.outdir or args.input.parent
    outdir.mkdir(parents=True, exist_ok=True)
    stem = args.input.stem

    rs_path = outdir / f"{stem}.rsids.tsv"
    non_path = outdir / f"{stem}.nonrsids.tsv"
    unm_path = outdir / f"{stem}.unmapped.tsv"

    n_rs = n_non = n_unm = n_total = 0
    with rs_path.open("w") as rs_f, non_path.open("w") as non_f, unm_path.open("w") as unm_f:
        rs_f.write("rsid\tchrom\tpos\ta1\ta2\tgenotype\tsnp_name\tdesign\n")
        non_f.write("snp_name\tchrom\tpos\ta1\ta2\tgenotype\tdesign\n")
        unm_f.write("snp_name\tchrom\tpos\ta1\ta2\tdesign\n")
        for r in iter_rows(str(args.input)):
            n_total += 1
            if r.chrom in ("0", "") or r.pos == 0:
                n_unm += 1
                unm_f.write(f"{r.snp_name}\t{r.chrom}\t{r.pos}\t{r.a1}\t{r.a2}\t{r.design}\n")
                continue
            if r.rsid:
                n_rs += 1
                rs_f.write(f"{r.rsid}\t{r.chrom}\t{r.pos}\t{r.a1}\t{r.a2}\t"
                           f"{r.genotype}\t{r.snp_name}\t{r.design}\n")
            else:
                n_non += 1
                non_f.write(f"{r.snp_name}\t{r.chrom}\t{r.pos}\t{r.a1}\t{r.a2}\t"
                            f"{r.genotype}\t{r.design}\n")

    print(f"total data rows   : {n_total}")
    print(f"  rs-derivable    : {n_rs:>7}  -> {rs_path}")
    print(f"  non-rsid mapped : {n_non:>7}  -> {non_path}")
    print(f"  unmapped (0/0)  : {n_unm:>7}  -> {unm_path}")


if __name__ == "__main__":
    main()
