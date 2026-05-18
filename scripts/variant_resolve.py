"""Shared decision logic for resolving non-rsid markers against a variant DB
(dbSNP VCF or Ensembl REST). Backends supply candidate SNP records; the
acceptance rules here are identical regardless of source.
"""

from __future__ import annotations

_COMP = {"A": "T", "T": "A", "C": "G", "G": "C"}


def comp_set(s):
    return {_COMP.get(b, b) for b in s}


def design_pair(design: str):
    """'[A/G]' -> {'A','G'}. None for indel/unknown designs."""
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


def decide(snp_name, chrom, pos, a1, a2, design, snp_recs):
    """snp_recs: list of (ref, [snp_alts], [rs_ids]). Returns
    ('ok', resolved_row) | ('ambig', row) | ('unres', row)."""
    observed = f"{a1}{a2}"
    if not snp_recs:
        return ("unres", (snp_name, chrom, pos, observed, design,
                          "no_snp_in_dbsnp"))

    distinct_rs = []
    for _ref, _alts, rids in snp_recs:
        for rid in rids:
            if rid not in distinct_rs:
                distinct_rs.append(rid)

    if len(snp_recs) > 1 or len(distinct_rs) > 1:
        cands = " | ".join(
            f"{(rids or ['.'])[0]}:{ref}>{','.join(alts)}"
            for ref, alts, rids in snp_recs)
        return ("ambig", (snp_name, chrom, pos, observed, design,
                          "multiple_variants", cands))

    ref, snp_alts, rids = snp_recs[0]
    rid = distinct_rs[0] if distinct_rs else None
    if rid is None:
        return ("unres", (snp_name, chrom, pos, observed, design, "no_rs_id"))

    dpair = design_pair(design)
    if not design_consistent(dpair, ref, snp_alts):
        return ("ambig", (snp_name, chrom, pos, observed, design,
                          "design_mismatch",
                          f"{rid}:{ref}>{','.join(snp_alts)}"))
    allele_set = {ref.upper()} | {a.upper() for a in snp_alts}
    obs = {x for x in (a1.upper(), a2.upper()) if x in "ACGT"}
    if obs and not obs <= allele_set:
        return ("ambig", (snp_name, chrom, pos, observed, design,
                          "allele_mismatch",
                          f"{rid}:{ref}>{','.join(snp_alts)}"))
    note = "multiallelic" if len(snp_alts) > 1 else ""
    return ("ok", (rid, chrom, pos, pos, ref, ",".join(snp_alts),
                   "exact", note, snp_name, observed, design))
