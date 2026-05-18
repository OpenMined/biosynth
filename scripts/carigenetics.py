"""Shared parser for the Illumina GSGT Final Report ("Carigenetics") format.

Layout:
    [Header]
    key<TAB>value ...
    [Data]
    Sample ID<TAB>Sample Name<TAB>SNP Name<TAB>SNP<TAB>Chr<TAB>Position<TAB>
        Allele1 - Top<TAB>Allele2 - Top<TAB>Allele1 - Forward<TAB>Allele2 - Forward<TAB>
        Allele1 - Plus<TAB>Allele2 - Plus<TAB>Allele1 - Design<TAB>Allele2 - Design<TAB>
        Allele1 - AB<TAB>Allele2 - AB<TAB>Plus/Minus Strand
    <sample>...

Plus-strand alleles align with the GRCh38 reference (same orientation as the
old Dynamic DNA plus-strand build-38 export), so we always read the
"Allele1 - Plus" / "Allele2 - Plus" columns for genotype.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator, Optional

RS_RE = re.compile(r"rs\d+")


@dataclass
class CarRow:
    snp_name: str          # raw "SNP Name"
    rsid: Optional[str]    # extracted rs\d+ or None
    design: str            # "SNP" column, e.g. [T/C]
    chrom: str             # "Chr"
    pos: int               # "Position" (GRCh38), 0 = unmapped
    a1: str                # Allele1 - Plus
    a2: str                # Allele2 - Plus

    @property
    def is_no_call(self) -> bool:
        return self.a1 == "-" or self.a2 == "-"

    @property
    def genotype(self) -> str:
        """Compact plus-strand genotype, '--' for no-call."""
        if self.is_no_call:
            return "--"
        return f"{self.a1}{self.a2}"


def extract_rsid(snp_name: str) -> Optional[str]:
    m = RS_RE.search(snp_name)
    return m.group(0) if m else None


def iter_rows(path: str) -> Iterator[CarRow]:
    """Yield data rows. Skips the [Header] block and the column header line."""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        in_data = False
        cols: Optional[dict] = None
        for line in fh:
            line = line.rstrip("\n").rstrip("\r")
            if not in_data:
                if line.strip() == "[Data]":
                    in_data = True
                continue
            f = line.split("\t")
            if cols is None:
                # column header row
                norm = [c.strip().lower() for c in f]
                cols = {name: i for i, name in enumerate(norm)}
                required = ["snp name", "chr", "position",
                            "allele1 - plus", "allele2 - plus"]
                missing = [r for r in required if r not in cols]
                if missing:
                    raise ValueError(
                        f"{path}: missing expected columns {missing}; got {norm}"
                    )
                continue
            if not line.strip():
                continue
            try:
                snp_name = f[cols["snp name"]].strip()
                chrom = f[cols["chr"]].strip()
                pos_raw = f[cols["position"]].strip()
                a1 = f[cols["allele1 - plus"]].strip()
                a2 = f[cols["allele2 - plus"]].strip()
                design = f[cols.get("snp", -1)].strip() if "snp" in cols else ""
            except IndexError:
                continue
            try:
                pos = int(pos_raw)
            except ValueError:
                pos = 0
            yield CarRow(
                snp_name=snp_name,
                rsid=extract_rsid(snp_name),
                design=design,
                chrom=chrom,
                pos=pos,
                a1=a1,
                a2=a2,
            )
