#!/usr/bin/env python3
"""Resolve non-rsid Carigenetics markers against Ensembl REST (GRCh38).

Fast alternative to the dbSNP VCF path: NCBI throttles bulk/remote access
hard (~8-30h), Ensembl REST is not throttled the same way (~15 req/s ->
~22k markers in ~25 min). Same decision rules and output schema as
query_dbsnp_nonrsids.py, so the resolved CSV loads with `bvs reference-load`.

Input : <stem>.nonrsids.tsv  (from extract_carigenetics_markers.py)
"""

from __future__ import annotations

import argparse
import csv
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from variant_resolve import decide  # noqa: E402

SERVER = "https://rest.ensembl.org"
HEADERS = {"Content-Type": "application/json"}


class RateLimiter:
    """Simple global token rate limiter (Ensembl allows 15 req/s)."""

    def __init__(self, per_sec: float):
        self.min_interval = 1.0 / per_sec
        self.lock = threading.Lock()
        self.next_t = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            if now < self.next_t:
                time.sleep(self.next_t - now)
                now = time.monotonic()
            self.next_t = max(now, self.next_t) + self.min_interval


def norm_chrom(value: str):
    c = value.strip()
    if c.upper().startswith("CHR"):
        c = c[3:]
    if c.upper() == "XY":
        c = "X"
    if c.upper() == "M":
        c = "MT"
    return c if c else None


def fetch_snp_recs(session, limiter, chrom, pos, retries=5):
    """Return list of (ref, [snp_alts], [rs_ids]) for SNPs at exact pos."""
    url = (f"{SERVER}/overlap/region/human/{chrom}:{pos}-{pos}"
           f"?feature=variation")
    for attempt in range(retries):
        limiter.wait()
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
        except requests.RequestException:
            time.sleep(1 + attempt)
            continue
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", 2)) + 0.5)
            continue
        if r.status_code in (400, 404):
            return [], None
        if r.status_code != 200:
            time.sleep(1 + attempt)
            continue
        recs = []
        for v in r.json():
            if v.get("feature_type") != "variation":
                continue
            if v.get("start") != pos or v.get("end") != pos:
                continue  # SNP occupies one base; indels span/shift
            alleles = [a for a in (v.get("alleles") or []) if a]
            if len(alleles) < 2:
                continue
            ref, alts = alleles[0].upper(), [a.upper() for a in alleles[1:]]
            # true SNP: single-base ACGT ref and >=1 single-base ACGT alt.
            # Ensembl encodes deletions as '-', which is len 1 but NOT a SNP.
            if ref not in ("A", "C", "G", "T"):
                continue
            snp_alts = [a for a in alts if a in ("A", "C", "G", "T")]
            if not snp_alts:
                continue
            rid = v.get("id", "")
            recs.append((ref, snp_alts,
                         [rid] if rid.startswith("rs") else []))
        return recs, None
    return None, "exhausted_retries"


def resolve_row(session, limiter, row):
    snp_name, chrom_raw, pos_s, a1, a2, _g, design = row
    chrom = norm_chrom(chrom_raw)
    if chrom is None:
        return ("unres", (snp_name, chrom_raw, pos_s, f"{a1}{a2}",
                          design, "missing_contig"))
    try:
        pos = int(pos_s)
    except ValueError:
        return ("unres", (snp_name, chrom_raw, pos_s, f"{a1}{a2}",
                          design, "bad_pos"))
    recs, err = fetch_snp_recs(session, limiter, chrom, pos)
    if err is not None:
        return ("unres", (snp_name, chrom, pos, f"{a1}{a2}", design,
                          f"fetch_error:{err}"))
    return decide(snp_name, chrom, pos, a1, a2, design, recs)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("nonrsids", type=Path, help="<stem>.nonrsids.tsv")
    ap.add_argument("--outdir", type=Path, default=None)
    ap.add_argument("--rate", type=float, default=13.0,
                    help="requests/sec (Ensembl hard limit is 15)")
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    rows = []
    with args.nonrsids.open() as fh:
        next(fh)
        for line in fh:
            f = line.rstrip("\n").split("\t")
            if len(f) >= 7:
                rows.append(f[:7])
    if args.limit:
        rows = rows[:args.limit]
    total = len(rows)

    outdir = args.outdir or args.nonrsids.parent
    outdir.mkdir(parents=True, exist_ok=True)
    stem = args.nonrsids.name.replace(".nonrsids.tsv", "")
    resolved_p = outdir / f"{stem}.nonrsids.resolved.csv"
    ambig_p = outdir / f"{stem}.nonrsids.ambiguous.tsv"
    unres_p = outdir / f"{stem}.nonrsids.unresolved.tsv"

    limiter = RateLimiter(args.rate)
    session = requests.Session()
    print(f"resolving {total} markers via Ensembl REST GRCh38 "
          f"({args.rate}/s, {args.workers} workers) ...", flush=True)

    n_ok = n_amb = n_unr = done = 0
    t0 = time.time()
    with resolved_p.open("w", newline="") as rf, \
         ambig_p.open("w") as af, unres_p.open("w") as uf, \
         ThreadPoolExecutor(max_workers=args.workers) as ex:
        rw = csv.writer(rf)
        rw.writerow(["query_rsid", "query_chrom", "query_pos", "ref_pos",
                     "ref", "alt", "status", "note", "snp_name",
                     "observed", "design"])
        af.write("snp_name\tchrom\tpos\tobserved\tdesign\treason\tcandidates\n")
        uf.write("snp_name\tchrom\tpos\tobserved\tdesign\treason\n")
        futs = [ex.submit(resolve_row, session, limiter, r) for r in rows]
        for fut in as_completed(futs):
            kind, payload = fut.result()
            if kind == "ok":
                rw.writerow(payload)
                n_ok += 1
            elif kind == "ambig":
                af.write("\t".join(map(str, payload)) + "\n")
                n_amb += 1
            else:
                uf.write("\t".join(map(str, payload)) + "\n")
                n_unr += 1
            done += 1
            if done % 500 == 0 or done == total:
                rate = done / max(time.time() - t0, 1e-6)
                eta = (total - done) / max(rate, 1e-6)
                print(f"  {done}/{total}  ok={n_ok} ambig={n_amb} "
                      f"unres={n_unr}  {rate:.1f}/s  ETA {eta/60:.1f}m",
                      flush=True)

    print(f"\nresolved (1 SNP) : {n_ok:>6}  -> {resolved_p}")
    print(f"ambiguous        : {n_amb:>6}  -> {ambig_p}")
    print(f"unresolved       : {n_unr:>6}  -> {unres_p}")
    print(f"\nLoad resolved into the reference DB with:")
    print(f"  ./bvs reference-load --sqlite data/genostats.sqlite "
          f"--lookup {resolved_p}")


if __name__ == "__main__":
    main()
