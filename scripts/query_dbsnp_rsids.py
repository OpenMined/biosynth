#!/usr/bin/env python3
"""Resolve rsids directly against NCBI dbSNP by RS id (not by position).

For markers that are missing from the position-built reference DB — often
because the array's coordinate disagrees with dbSNP's GRCh38 placement, or
the rs was withdrawn — query the NCBI Variation Services refsnp API:

    https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/<num>

and pull dbSNP's own GRCh38 SPDI placement. This recovers rsids that ARE
still in dbSNP and clearly separates the ones that are not (merged, or
"no longer has supporting observations" -> withdrawn).

Input  : a genotype-to-vcf --missing-log file (lines "missing_reference: rsNNN")
         OR a plain text file with one rsid per line.
Outputs (next to --outdir):
  <stem>.dbsnp_rsid.resolved.csv   reference-load compatible (status=exact)
  <stem>.dbsnp_rsid.withdrawn.tsv  present in dbSNP but no GRCh38 support
  <stem>.dbsnp_rsid.unresolved.tsv not found / error

Rate: NCBI allows ~3 req/s anonymously, ~10 req/s with an API key.
Set NCBI_API_KEY in the environment to go faster.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

API = "https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/"

# RefSeq GRCh38.p14 contig -> chromosome label
NC_TO_CHROM = {
    "NC_000001.11": "1", "NC_000002.12": "2", "NC_000003.12": "3",
    "NC_000004.12": "4", "NC_000005.10": "5", "NC_000006.12": "6",
    "NC_000007.14": "7", "NC_000008.11": "8", "NC_000009.12": "9",
    "NC_000010.11": "10", "NC_000011.10": "11", "NC_000012.12": "12",
    "NC_000013.11": "13", "NC_000014.9": "14", "NC_000015.10": "15",
    "NC_000016.10": "16", "NC_000017.11": "17", "NC_000018.10": "18",
    "NC_000019.10": "19", "NC_000020.11": "20", "NC_000021.9": "21",
    "NC_000022.11": "22", "NC_000023.11": "X", "NC_000024.10": "Y",
    "NC_012920.1": "MT",
}

RS_RE = re.compile(r"rs(\d+)")


class RateLimiter:
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


def read_rsids(path: Path) -> list:
    nums, seen = [], set()
    for line in path.open():
        for m in RS_RE.finditer(line):
            n = m.group(1)
            if n not in seen:
                seen.add(n)
                nums.append(n)
    return nums


def grch38_alleles(doc: dict):
    """Return (chrom, vcf_pos, ref, [alts]) from the GRCh38 SPDI placement,
    or None if dbSNP has no usable GRCh38 placement (withdrawn/unsupported)."""
    psd = doc.get("primary_snapshot_data")
    if not psd:
        return None
    for plc in psd.get("placements_with_allele", []):
        # the top-level genomic placement (is_ptlp) on a GRCh38 NC_ contig
        if not plc.get("is_ptlp"):
            continue
        ref = None
        alts = []
        pos0 = None
        chrom = None
        for al in plc.get("alleles", []):
            spdi = al.get("allele", {}).get("spdi") or {}
            seq = spdi.get("seq_id", "")
            if seq not in NC_TO_CHROM:
                continue
            chrom = NC_TO_CHROM[seq]
            d = spdi.get("deleted_sequence", "")
            i = spdi.get("inserted_sequence", "")
            pos0 = spdi.get("position")
            if ref is None:
                ref = d
            if i != d:  # the identity allele is the reference itself
                alts.append(i)
        if chrom and ref is not None and pos0 is not None and alts:
            # SPDI position is 0-based start of the deleted block;
            # VCF POS is 1-based.
            return chrom, int(pos0) + 1, ref, alts
    return None


def _fetch(session, limiter, num, retries=5):
    """One refsnp GET -> parsed JSON doc or (None, reason)."""
    url = f"{API}{num}"
    key = os.environ.get("NCBI_API_KEY")
    params = {"api_key": key} if key else None
    for attempt in range(retries):
        limiter.wait()
        try:
            r = session.get(url, params=params, timeout=30)
        except requests.RequestException:
            time.sleep(1 + attempt)
            continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(float(r.headers.get("Retry-After", 2)) + attempt)
            continue
        if r.status_code == 404:
            return None, "not_found"
        if r.status_code != 200:
            time.sleep(1 + attempt)
            continue
        return r.json(), None
    return None, "exhausted_retries"


def resolve(session, limiter, num, max_hops=5):
    """Resolve REF/ALT for `num`. Follows merged_into chains so a withdrawn
    rs that was merged still yields the locus alleles, recorded under the
    ORIGINAL rsid (that's what the genotype files carry)."""
    cur = num
    chain = []
    for _ in range(max_hops):
        doc, err = _fetch(session, limiter, cur)
        if doc is None:
            return ("unresolved", num, err or "fetch_failed")
        got = grch38_alleles(doc)
        if got is not None:
            chrom, pos, ref, alts = got
            note = f"merged_chain:{'>'.join(chain)}" if chain else ""
            return ("ok", num, (chrom, pos, ref, alts, note))
        merged = doc.get("merged_snapshot_data") or {}
        targets = merged.get("merged_into") or []
        if not targets:
            return ("withdrawn", num,
                    "withdrawn_no_grch38" if not chain
                    else f"withdrawn_after:{'>'.join(chain)}")
        cur = str(targets[0])
        chain.append(f"rs{cur}")
    return ("withdrawn", num, f"merge_chain_too_deep:{'>'.join(chain)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input", type=Path,
                    help="missing-log (missing_reference: rsNNN) or rsid list")
    ap.add_argument("--outdir", type=Path, default=None)
    ap.add_argument("--rate", type=float, default=None,
                    help="requests/sec (default 3, or 10 if NCBI_API_KEY set)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    rsids = read_rsids(args.input)
    if args.limit:
        rsids = rsids[: args.limit]
    total = len(rsids)
    if total == 0:
        sys.exit("No rsids found in input.")

    rate = args.rate or (10.0 if os.environ.get("NCBI_API_KEY") else 3.0)
    limiter = RateLimiter(rate)
    session = requests.Session()

    outdir = args.outdir or args.input.parent
    outdir.mkdir(parents=True, exist_ok=True)
    stem = args.input.stem
    res_p = outdir / f"{stem}.dbsnp_rsid.resolved.csv"
    wd_p = outdir / f"{stem}.dbsnp_rsid.withdrawn.tsv"
    un_p = outdir / f"{stem}.dbsnp_rsid.unresolved.tsv"

    print(f"querying NCBI dbSNP for {total} rsids @ {rate}/s "
          f"({'API key' if os.environ.get('NCBI_API_KEY') else 'anon'}) ...",
          flush=True)

    n_ok = n_wd = n_un = done = 0
    t0 = time.time()
    with res_p.open("w", newline="") as rf, wd_p.open("w") as wf, \
         un_p.open("w") as uf, \
         ThreadPoolExecutor(max_workers=args.workers) as ex:
        rw = csv.writer(rf)
        rw.writerow(["query_rsid", "query_chrom", "query_pos", "ref_pos",
                     "ref", "alt", "status", "note"])
        wf.write("rsid\treason\n")
        uf.write("rsid\treason\n")
        futs = [ex.submit(resolve, session, limiter, n) for n in rsids]
        for fut in as_completed(futs):
            kind, num, payload = fut.result()
            if kind == "ok":
                chrom, pos, ref, alts, note = payload
                if len(alts) > 1:
                    note = (note + ";multiallelic").lstrip(";")
                rw.writerow([f"rs{num}", chrom, pos, pos, ref,
                             ",".join(alts), "exact", note])
                n_ok += 1
            elif kind == "withdrawn":
                wf.write(f"rs{num}\t{payload}\n")
                n_wd += 1
            else:
                uf.write(f"rs{num}\t{payload}\n")
                n_un += 1
            done += 1
            if done % 200 == 0 or done == total:
                rate_now = done / max(time.time() - t0, 1e-6)
                eta = (total - done) / max(rate_now, 1e-6)
                print(f"  {done}/{total}  ok={n_ok} withdrawn={n_wd} "
                      f"unresolved={n_un}  {rate_now:.1f}/s "
                      f"ETA {eta/60:.1f}m", flush=True)

    print(f"\nresolved   : {n_ok:>6}  -> {res_p}")
    print(f"withdrawn  : {n_wd:>6}  -> {wd_p}")
    print(f"unresolved : {n_un:>6}  -> {un_p}")
    if n_ok:
        print(f"\nLoad with:\n  ./cli/target/debug/bvs reference-load "
              f"--sqlite data/genostats.sqlite --lookup {res_p}")


if __name__ == "__main__":
    main()
