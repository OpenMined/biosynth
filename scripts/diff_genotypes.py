#!/usr/bin/env python3
"""
Diff two genotype files, ignoring gs/baf/lrr columns.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="Reference genotype file")
    parser.add_argument(
        "candidate", type=Path, help="Generated genotype file to compare"
    )
    parser.add_argument(
        "--max-diffs",
        type=int,
        default=20,
        help="Maximum number of differing rows to print (default: 20)",
    )
    return parser.parse_args()


def load_genotypes(path: Path) -> Dict[str, Tuple[str, str, str]]:
    genotypes: Dict[str, Tuple[str, str, str]] = {}
    with path.open() as handle:
        for raw in handle:
            if raw.startswith("#"):
                continue
            parts = raw.strip().split("\t")
            if len(parts) < 4:
                continue
            rsid, chrom, pos, genotype = parts[:4]
            genotypes[rsid] = (chrom, pos, genotype)
    return genotypes


def main() -> None:
    args = parse_args()
    if not args.baseline.exists():
        raise SystemExit(f"Missing baseline file: {args.baseline}")
    if not args.candidate.exists():
        raise SystemExit(f"Missing candidate file: {args.candidate}")

    base = load_genotypes(args.baseline)
    cand = load_genotypes(args.candidate)

    missing = sorted(set(base) - set(cand))
    added = sorted(set(cand) - set(base))

    diffs = []
    for rsid in sorted(set(base) & set(cand)):
        b_chrom, b_pos, b_gt = base[rsid]
        c_chrom, c_pos, c_gt = cand[rsid]
        if (b_chrom, b_pos, b_gt) != (c_chrom, c_pos, c_gt):
            diffs.append(
                {
                    "rsid": rsid,
                    "baseline": (b_chrom, b_pos, b_gt),
                    "candidate": (c_chrom, c_pos, c_gt),
                }
            )

    print(f"Baseline rows: {len(base):,}")
    print(f"Candidate rows: {len(cand):,}")
    print(f"Missing in candidate: {len(missing):,}")
    print(f"Not present in baseline: {len(added):,}")
    print(f"Genotype/position differences: {len(diffs):,}")

    for diff in diffs[: args.max_diffs]:
        rsid = diff["rsid"]
        b = diff["baseline"]
        c = diff["candidate"]
        print(
            f"- {rsid}: baseline {b[0]}:{b[1]} {b[2]} vs candidate {c[0]}:{c[1]} {c[2]}"
        )

    if diffs and len(diffs) > args.max_diffs:
        print(f"... {len(diffs) - args.max_diffs} more differences omitted")

    if missing:
        print("Sample missing rsids:", ", ".join(missing[: args.max_diffs]))
    if added:
        print("Sample new rsids:", ", ".join(added[: args.max_diffs]))


if __name__ == "__main__":
    main()
