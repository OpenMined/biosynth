#!/usr/bin/env python3
"""Diff two DDNA-style genotype .txt files by chromosome:position to check
they are the same individual and surface rows we may be misinterpreting.

  python diff_ddna.py REFERENCE.txt CONVERTED.txt [--mismatch-log out.tsv]

Categories reported:
  match               sorted allele pair identical
  palindromic_flip    A/T or C/G site, converted == strand-complement of ref
                      (expected cross-platform artifact, NOT a person mismatch)
  het_hom_mismatch    one het / one hom (or different alleles), non-palindromic
  nocall_ref_only     ref has a call, converted is no-call
  nocall_conv_only    converted has a call, ref is no-call
  indel_skipped       ref is I/D/II/DD (old indel encoding) vs ACGT
  ref_only / conv_only present in only one file
"""

from __future__ import annotations

import argparse
from pathlib import Path

COMP = {"A": "T", "T": "A", "C": "G", "G": "C"}
INDEL = {"I", "D", "II", "DD", "ID", "DI", "-I", "I-", "-D", "D-"}


def pair(g: str):
    g = g.strip().upper()
    if g in ("--", "NN", "..", ".", ""):
        return None, "nocall"
    if g in INDEL or not set(g) <= set("ACGT"):
        return None, "indel"
    if len(g) == 1:
        g = g + g
    if len(g) != 2:
        return None, "indel"
    return "".join(sorted(g)), "ok"


def load(path: Path):
    d = {}
    with path.open() as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            f = line.rstrip("\n").split("\t")
            if len(f) < 4:
                continue
            d[(f[1], f[2])] = (f[0], f[3])
    return d


def is_palindromic(p: str) -> bool:
    return p in ("AT", "CG")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("reference", type=Path)
    ap.add_argument("converted", type=Path)
    ap.add_argument("--mismatch-log", type=Path, default=None)
    args = ap.parse_args()

    ref = load(args.reference)
    conv = load(args.converted)

    cats = {}
    examples = {}
    log_rows = []
    shared = set(ref) & set(conv)
    for key in shared:
        rid, rg = ref[key]
        _, cg = conv[key]
        rp, rs = pair(rg)
        cp, cs = pair(cg)

        if rs == "indel":
            cat = "indel_skipped"
        elif rs == "nocall" and cs == "nocall":
            cat = "match"  # both no-call
        elif rs == "nocall":
            cat = "nocall_ref_only"
        elif cs == "nocall":
            cat = "nocall_conv_only"
        elif rp == cp:
            cat = "match"
        else:
            # complement the converted pair; if it then matches and the
            # site is palindromic it's the known strand-ambiguity artifact
            cflip = "".join(sorted(COMP[a] for a in cp))
            if cflip == rp and is_palindromic(rp):
                cat = "palindromic_flip"
            else:
                cat = "het_hom_mismatch"

        cats[cat] = cats.get(cat, 0) + 1
        if cat not in ("match",):
            examples.setdefault(cat, [])
            if len(examples[cat]) < 8:
                examples[cat].append((key[0], key[1], rid, rg, cg))
            if cat in ("het_hom_mismatch",) and args.mismatch_log:
                log_rows.append((key[0], key[1], rid, rg, cg))

    only_ref = len(set(ref) - set(conv))
    only_conv = len(set(conv) - set(ref))

    total = len(shared)
    callable_ = sum(cats.get(c, 0) for c in
                    ("match", "palindromic_flip", "het_hom_mismatch"))
    concord = cats.get("match", 0) + cats.get("palindromic_flip", 0)

    print(f"reference rows : {len(ref)}")
    print(f"converted rows : {len(conv)}")
    print(f"shared chrom:pos: {total}")
    print(f"  ref_only      : {only_ref}")
    print(f"  conv_only     : {only_conv}")
    print("--- categories (shared) ---")
    for c in ("match", "palindromic_flip", "het_hom_mismatch",
              "nocall_ref_only", "nocall_conv_only", "indel_skipped"):
        if c in cats:
            print(f"  {c:<18}: {cats[c]}")
    if callable_:
        print(f"\nconcordance over comparable calls "
              f"(match+palindromic) : {concord}/{callable_} "
              f"= {100.0*concord/callable_:.4f}%")
    for c, ex in examples.items():
        if c == "indel_skipped":
            continue
        print(f"\nsample {c} (chrom,pos,id,ref_gt,conv_gt):")
        for e in ex:
            print("  ", e)

    if args.mismatch_log and log_rows:
        with args.mismatch_log.open("w") as fh:
            fh.write("chrom\tpos\tid\tref_gt\tconv_gt\n")
            for r in log_rows:
                fh.write("\t".join(r) + "\n")
        print(f"\nwrote {len(log_rows)} het/hom mismatches -> {args.mismatch_log}")


if __name__ == "__main__":
    main()
