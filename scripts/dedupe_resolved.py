#!/usr/bin/env python3
"""Filter dbSNP/Ensembl-resolved non-rsid markers so no resolved rsid
collides with an rsid already present elsewhere in the SAME Carigenetics
file (or maps from more than one non-rs probe).

A resolved probe is only loadable if its rsid is unique. If the file's
own rs-derivable markers already carry that rsid, the resolved probe is
just an unlabeled duplicate -> drop it (the native rs marker is canonical).
If several non-rs probes resolve to the same rsid -> drop all, log for
review (concordance shown so a decision can be made later).

  dedupe_resolved.py RESOLVED.csv RSIDS.tsv [-o CLEAN.csv] [--collisions L.tsv]

Outputs:
  CLEAN.csv        resolved rows with a unique rsid  (reference-load ready)
  collisions.tsv   dropped rows + reason + observed genotypes
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path

RS = re.compile(r"rs(\d+)")


def rs_num(s: str):
    m = RS.search(s or "")
    return m.group(1) if m else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("resolved", type=Path, help="*.nonrsids.resolved.csv")
    ap.add_argument("rsids", type=Path,
                    help="*.rsids.tsv (file's rs-derivable markers)")
    ap.add_argument("-o", "--output", type=Path, default=None)
    ap.add_argument("--collisions", type=Path, default=None)
    args = ap.parse_args()

    out_p = args.output or args.resolved.with_suffix(".dedup.csv")
    coll_p = args.collisions or args.resolved.with_suffix(".collisions.tsv")

    # rsids the file already carries natively (canonical)
    file_rsids = set()
    with args.rsids.open() as fh:
        next(fh, None)
        for line in fh:
            n = rs_num(line.split("\t", 1)[0])
            if n:
                file_rsids.add(n)

    with args.resolved.open() as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = list(reader)

    idx = {name: i for i, name in enumerate(header)}
    ri, si, oi = idx["query_rsid"], idx["snp_name"], idx["observed"]

    by_rsid = defaultdict(list)
    for r in rows:
        by_rsid[rs_num(r[ri])].append(r)

    kept, dropped = [], []
    for num, grp in by_rsid.items():
        if num is None:
            dropped.append((grp, "no_rs_number"))
            continue
        if num in file_rsids:
            dropped.append((grp, "rsid_already_in_file"))
            continue
        if len(grp) > 1:
            dropped.append((grp, "multiple_probes_same_rsid"))
            continue
        kept.append(grp[0])

    with out_p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(kept)

    n_drop = 0
    with coll_p.open("w") as f:
        f.write("rsid\treason\tsnp_name\tobserved\n")
        for grp, reason in dropped:
            for r in grp:
                f.write(f"{r[ri]}\t{reason}\t{r[si]}\t{r[oi]}\n")
                n_drop += 1

    print(f"resolved rows in       : {len(rows)}")
    print(f"unique-rsid kept       : {len(kept):>6}  -> {out_p}")
    print(f"dropped (collisions)   : {n_drop:>6}  -> {coll_p}")
    by_reason = defaultdict(int)
    for grp, reason in dropped:
        by_reason[reason] += len(grp)
    for reason, c in sorted(by_reason.items(), key=lambda x: -x[1]):
        print(f"    {reason:<28}: {c}")
    print(f"\nLoad the deduped set with:")
    print(f"  ./bvs reference-load --sqlite data/genostats.sqlite "
          f"--lookup {out_p}")


if __name__ == "__main__":
    main()
