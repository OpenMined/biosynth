#!/usr/bin/env python3
"""Convert a Carigenetics (Illumina GSGT Final Report) file to the old
Dynamic DNA (DDNA) plus-strand genotype .txt format.

Emits the DDNA layout:  rsid<TAB>chromosome<TAB>position<TAB>genotype
(plus the leading `## ` column-header line DDNA uses). Genotype is the
plus-strand call; no-calls become `--`. Markers without an rs id use the
raw SNP Name as the id (position is what the diff matches on anyway).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from carigenetics import iter_rows  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input", type=Path)
    ap.add_argument("-o", "--output", type=Path, required=True)
    ap.add_argument("--rs-only", action="store_true",
                    help="Emit only rs-derivable markers")
    args = ap.parse_args()

    n = n_written = 0
    with args.output.open("w") as out:
        out.write("## rsid\tchromosome\tposition\tgenotype\n")
        for r in iter_rows(str(args.input)):
            n += 1
            if r.chrom in ("0", "") or r.pos == 0:
                continue
            rid = r.rsid
            if rid is None:
                if args.rs_only:
                    continue
                rid = r.snp_name
            out.write(f"{rid}\t{r.chrom}\t{r.pos}\t{r.genotype}\n")
            n_written += 1

    print(f"read {n} rows, wrote {n_written} -> {args.output}")


if __name__ == "__main__":
    main()
