#!/usr/bin/env python3
"""Resolve non-rsid Carigenetics markers against GRCh38 dbSNP.

By default this STREAMS the dbSNP VCF remotely over HTTPS using the small
(~3MB) tabix index, fetching only the bytes for each queried region -- no
27GB bulk download. Pass a local .gz to --vcf to use a downloaded copy.

Input : <stem>.nonrsids.tsv  (from extract_carigenetics_markers.py)
        columns: snp_name chrom pos a1 a2 genotype design

Decision per position:
  * exactly ONE SNP rs id at the exact position, and the probe DESIGN pair
    (e.g. [A/G]) matches dbSNP {ref,alt} on either strand, and the observed
    call is consistent
        -> RESOLVED  -> <stem>.nonrsids.resolved.csv  (reference-load schema)
  * >1 distinct SNP rs id at the position, or design/allele mismatch
        -> AMBIGUOUS -> <stem>.nonrsids.ambiguous.tsv
  * no SNP record at the position
        -> UNRESOLVED -> <stem>.nonrsids.unresolved.tsv
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

import pysam

REMOTE_VCF = ("https://ftp.ncbi.nih.gov/snp/latest_release/VCF/"
              "GCF_000001405.40.gz")

CONTIG_MAP = {
    "1": "NC_000001.11", "2": "NC_000002.12", "3": "NC_000003.12",
    "4": "NC_000004.12", "5": "NC_000005.10", "6": "NC_000006.12",
    "7": "NC_000007.14", "8": "NC_000008.11", "9": "NC_000009.12",
    "10": "NC_000010.11", "11": "NC_000011.10", "12": "NC_000012.12",
    "13": "NC_000013.11", "14": "NC_000014.9", "15": "NC_000015.10",
    "16": "NC_000016.10", "17": "NC_000017.11", "18": "NC_000018.10",
    "19": "NC_000019.10", "20": "NC_000020.11", "21": "NC_000021.9",
    "22": "NC_000022.11", "X": "NC_000023.11", "Y": "NC_000024.10",
    "M": "NC_012920.1", "MT": "NC_012920.1",
}
_COMP = {"A": "T", "T": "A", "C": "G", "G": "C"}


def norm_chrom(value: str):
    c = value.strip().upper()
    if c.startswith("CHR"):
        c = c[3:]
    if c == "XY":
        c = "X"
    return CONTIG_MAP.get(c)


def rs_ids(rec) -> list:
    ids = []
    if rec.id and rec.id != ".":
        ids += [p.strip() for p in rec.id.split(";")
                if p.strip().startswith("rs")]
    info_rs = rec.info.get("RS")
    if info_rs is not None:
        vals = info_rs if isinstance(info_rs, (list, tuple)) else [info_rs]
        ids += [f"rs{int(v)}" for v in vals]
    seen, out = set(), []
    for i in ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


def is_snp(ref: str, alts) -> bool:
    return len(ref) == 1 and any(len(a) == 1 for a in alts)


def comp_set(s):
    return {_COMP.get(b, b) for b in s}


def design_pair(design: str):
    d = design.strip().strip("[]")
    parts = [p.strip().upper() for p in d.split("/") if p.strip()]
    if len(parts) != 2 or any(p not in "ACGT" for p in parts):
        return None
    return set(parts)


def design_consistent(dpair, ref, snp_alts) -> bool:
    """Strand-agnostic: probe design pair must equal dbSNP {ref,alt}
    (bi-allelic) or be a subset (multi-allelic), on either strand."""
    if dpair is None:
        return False
    db = {ref.upper()} | {a.upper() for a in snp_alts}
    if len(snp_alts) == 1:
        return dpair == db or dpair == comp_set(db)
    return dpair <= db or dpair <= comp_set(db)


# ---- per-row resolution ---------------------------------------------------


def fetch_snp_recs(vf, contig, pos, retries=4):
    """Remote-tolerant fetch of SNP records at an exact 1-based position."""
    last = None
    for attempt in range(retries):
        try:
            recs = []
            for rec in vf.fetch(contig, pos - 1, pos):
                if rec.pos != pos:
                    continue
                alts = [a for a in (rec.alts or []) if a]
                if alts and is_snp(rec.ref, alts):
                    recs.append((rec.ref, [a for a in alts if len(a) == 1],
                                 rs_ids(rec), rec.pos))
            return recs, None
        except Exception as e:
            last = e
    return None, last


def resolve_row(vf, row):
    snp_name, chrom, pos_s, a1, a2, _geno, design = row
    observed = f"{a1}{a2}"
    contig = norm_chrom(chrom)
    if contig is None or contig not in vf.header.contigs:
        return ("unres", (snp_name, chrom, pos_s, observed, design,
                          "missing_contig"))
    try:
        pos = int(pos_s)
    except ValueError:
        return ("unres", (snp_name, chrom, pos_s, observed, design, "bad_pos"))

    snp_recs, err = fetch_snp_recs(vf, contig, pos)
    if err is not None:
        return ("unres", (snp_name, chrom, pos, observed, design,
                          f"fetch_error:{type(err).__name__}"))

    if not snp_recs:
        return ("unres", (snp_name, chrom, pos, observed, design,
                          "no_snp_in_dbsnp"))

    distinct_rs = []
    for _ref, _alts, rids, _p in snp_recs:
        for rid in rids:
            if rid not in distinct_rs:
                distinct_rs.append(rid)

    if len(snp_recs) > 1 or len(distinct_rs) > 1:
        cands = " | ".join(
            f"{(rids or ['.'])[0]}:{ref}>{','.join(alts)}"
            for ref, alts, rids, _p in snp_recs)
        return ("ambig", (snp_name, chrom, pos, observed, design,
                          "multiple_variants", cands))

    ref, snp_alts, rids, ref_pos = snp_recs[0]
    rid = distinct_rs[0] if distinct_rs else None
    if rid is None:
        return ("unres", (snp_name, chrom, pos, observed, design, "no_rs_id"))

    dpair = design_pair(design)
    if not design_consistent(dpair, ref, snp_alts):
        return ("ambig", (snp_name, chrom, pos, observed, design,
                          "design_mismatch", f"{rid}:{ref}>{','.join(snp_alts)}"))
    allele_set = {ref.upper()} | {a.upper() for a in snp_alts}
    obs = {x for x in (a1.upper(), a2.upper()) if x in "ACGT"}
    if obs and not obs <= allele_set:
        return ("ambig", (snp_name, chrom, pos, observed, design,
                          "allele_mismatch", f"{rid}:{ref}>{','.join(snp_alts)}"))
    note = "multiallelic" if len(snp_alts) > 1 else ""
    return ("ok", (rid, chrom, pos, ref_pos, ref, ",".join(snp_alts),
                   "exact", note, snp_name, observed, design))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("nonrsids", type=Path, help="<stem>.nonrsids.tsv")
    ap.add_argument("--vcf", default=REMOTE_VCF,
                    help=f"dbSNP VCF (URL or local .gz). Default: {REMOTE_VCF}")
    ap.add_argument("--tbi", type=Path,
                    default=Path("notebooks/data/GCF_000001405.40.gz.tbi"),
                    help="Local tabix index for the VCF")
    ap.add_argument("--outdir", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    if not args.tbi.exists():
        sys.exit(f"Missing tabix index {args.tbi}. Fetch the 3MB index:\n"
                 "  curl -fSL -o notebooks/data/GCF_000001405.40.gz.tbi "
                 "https://ftp.ncbi.nih.gov/snp/latest_release/VCF/"
                 "GCF_000001405.40.gz.tbi")
    vcf_url = str(args.vcf)

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

    # Sort by genomic order so the single remote handle does cheap forward
    # seeks and reuses one HTTP connection (NCBI throttles per-IP, so
    # parallel handles don't help and are unstable over htslib).
    _order = {c: i for i, c in enumerate(CONTIG_MAP.values())}

    def sort_key(r):
        c = norm_chrom(r[1])
        try:
            p = int(r[2])
        except ValueError:
            p = 0
        return (_order.get(c, 999), p)

    rows.sort(key=sort_key)

    outdir = args.outdir or args.nonrsids.parent
    outdir.mkdir(parents=True, exist_ok=True)
    stem = args.nonrsids.name.replace(".nonrsids.tsv", "")
    resolved_p = outdir / f"{stem}.nonrsids.resolved.csv"
    ambig_p = outdir / f"{stem}.nonrsids.ambiguous.tsv"
    unres_p = outdir / f"{stem}.nonrsids.unresolved.tsv"

    src = "remote stream" if vcf_url.startswith("http") else "local"
    print(f"resolving {total} non-rsid markers via {src} dbSNP "
          f"(single sorted handle) ...", flush=True)

    vf = pysam.VariantFile(vcf_url, index_filename=str(args.tbi))

    n_ok = n_amb = n_unr = done = 0
    t0 = time.time()
    with resolved_p.open("w", newline="") as rf, \
         ambig_p.open("w") as af, unres_p.open("w") as uf:
        rw = csv.writer(rf)
        rw.writerow(["query_rsid", "query_chrom", "query_pos", "ref_pos",
                     "ref", "alt", "status", "note", "snp_name",
                     "observed", "design"])
        af.write("snp_name\tchrom\tpos\tobserved\tdesign\treason\tcandidates\n")
        uf.write("snp_name\tchrom\tpos\tobserved\tdesign\treason\n")
        for r in rows:
            kind, payload = resolve_row(vf, r)
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
    try:
        vf.close()
    except Exception:
        pass

    print(f"\nresolved (1 SNP) : {n_ok:>6}  -> {resolved_p}")
    print(f"ambiguous        : {n_amb:>6}  -> {ambig_p}")
    print(f"unresolved       : {n_unr:>6}  -> {unres_p}")
    print(f"\nLoad resolved into the reference DB with:")
    print(f"  ./bvs reference-load --sqlite data/genostats.sqlite "
          f"--lookup {resolved_p}")


if __name__ == "__main__":
    main()
