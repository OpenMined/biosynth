#!/usr/bin/env python3
"""Lift a SNP-major PLINK .bed/.bim/.fam prefix through a UCSC chain file.

Unmapped or non-SNP rows are omitted from the output BIM and the corresponding
BED variant row is omitted too, keeping the lifted PLINK prefix dimensionally
valid without requiring PLINK to be installed.
"""

from __future__ import annotations

import argparse
import gzip
import shutil
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


BED_MAGIC = b"\x6c\x1b\x01"


@dataclass(frozen=True)
class Block:
    t_chrom: str
    t_start: int
    t_end: int
    q_chrom: str
    q_start: int
    q_strand: str
    q_size: int


def norm_chrom(value: str) -> str:
    value = value.strip()
    if value.startswith("chr"):
        value = value[3:]
    if value == "23":
        return "X"
    if value == "24":
        return "Y"
    if value in {"25", "M"}:
        return "MT"
    return value


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return path.open("rt")


def iter_chain_blocks(chain_path: Path) -> Iterator[Block]:
    with open_text(chain_path) as handle:
        t_chrom = ""
        t_pos = 0
        q_chrom = ""
        q_pos = 0
        q_size = 0
        q_strand = "+"
        in_chain = False

        for raw in handle:
            line = raw.strip()
            if not line:
                in_chain = False
                continue
            parts = line.split()
            if parts[0] == "chain":
                t_chrom = norm_chrom(parts[2])
                t_pos = int(parts[5])
                q_chrom = norm_chrom(parts[7])
                q_size = int(parts[8])
                q_strand = parts[9]
                q_pos = int(parts[10])
                in_chain = True
                continue
            if not in_chain:
                continue

            size = int(parts[0])
            yield Block(
                t_chrom=t_chrom,
                t_start=t_pos,
                t_end=t_pos + size,
                q_chrom=q_chrom,
                q_start=q_pos,
                q_strand=q_strand,
                q_size=q_size,
            )
            t_pos += size
            q_pos += size
            if len(parts) == 3:
                t_pos += int(parts[1])
                q_pos += int(parts[2])


def build_index(chain_path: Path) -> tuple[dict[str, list[int]], dict[str, list[Block]]]:
    starts: dict[str, list[int]] = {}
    blocks: dict[str, list[Block]] = {}
    for block in iter_chain_blocks(chain_path):
        blocks.setdefault(block.t_chrom, []).append(block)
    for chrom, chrom_blocks in blocks.items():
        chrom_blocks.sort(key=lambda b: b.t_start)
        starts[chrom] = [b.t_start for b in chrom_blocks]
    return starts, blocks


def liftover_point(
    starts: dict[str, list[int]],
    blocks: dict[str, list[Block]],
    chrom: str,
    pos_1based: int,
) -> tuple[str, int] | None:
    chrom = norm_chrom(chrom)
    pos0 = pos_1based - 1
    chrom_starts = starts.get(chrom)
    chrom_blocks = blocks.get(chrom)
    if not chrom_starts or not chrom_blocks:
        return None
    idx = bisect_right(chrom_starts, pos0) - 1
    if idx < 0:
        return None
    block = chrom_blocks[idx]
    if not (block.t_start <= pos0 < block.t_end):
        return None
    offset = pos0 - block.t_start
    if block.q_strand == "+":
        lifted0 = block.q_start + offset
    else:
        lifted0 = block.q_size - (block.q_start + offset) - 1
    return block.q_chrom, lifted0 + 1


def prefix_path(prefix: Path, ext: str) -> Path:
    return Path(str(prefix) + ext)


def count_fam(path: Path) -> int:
    with path.open("rt") as handle:
        return sum(1 for line in handle if line.strip())


def is_snp(a1: str, a2: str) -> bool:
    return a1 in {"A", "C", "G", "T"} and a2 in {"A", "C", "G", "T"} and a1 != a2


def lift_prefix(chain: Path, input_prefix: Path, output_prefix: Path) -> tuple[int, int, int]:
    starts, blocks = build_index(chain)
    in_bed = prefix_path(input_prefix, ".bed")
    in_bim = prefix_path(input_prefix, ".bim")
    in_fam = prefix_path(input_prefix, ".fam")
    out_bed = prefix_path(output_prefix, ".bed")
    out_bim = prefix_path(output_prefix, ".bim")
    out_fam = prefix_path(output_prefix, ".fam")

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(in_fam, out_fam)

    n_samples = count_fam(in_fam)
    bytes_per_variant = (n_samples + 3) // 4
    total = lifted = skipped = 0

    with in_bed.open("rb") as bed_in, in_bim.open("rt") as bim_in, out_bed.open("wb") as bed_out, out_bim.open("wt") as bim_out:
        magic = bed_in.read(3)
        if magic != BED_MAGIC:
            raise SystemExit(f"Unsupported BED header in {in_bed}: expected SNP-major 6c 1b 01")
        bed_out.write(BED_MAGIC)

        for line_number, raw in enumerate(bim_in, start=1):
            fields = raw.rstrip("\n").split()
            row = bed_in.read(bytes_per_variant)
            if len(row) != bytes_per_variant:
                raise SystemExit(f"BED ended early at BIM row {line_number}")
            if len(fields) < 6:
                skipped += 1
                continue
            total += 1
            chrom, rsid, cm, pos_text, a1, a2 = fields[:6]
            try:
                pos = int(pos_text)
            except ValueError:
                skipped += 1
                continue
            if not is_snp(a1, a2):
                skipped += 1
                continue
            mapped = liftover_point(starts, blocks, chrom, pos)
            if mapped is None:
                skipped += 1
                continue
            out_chrom, out_pos = mapped
            bim_out.write(f"{out_chrom}\t{rsid}\t{cm}\t{out_pos}\t{a1}\t{a2}\n")
            bed_out.write(row)
            lifted += 1

        extra = bed_in.read(1)
        if extra:
            raise SystemExit(f"BED has trailing bytes after {total} BIM rows: {in_bed}")

    return total, lifted, skipped


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain", required=True, type=Path)
    parser.add_argument("--input-prefix", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    args = parser.parse_args()
    total, lifted, skipped = lift_prefix(args.chain, args.input_prefix, args.output_prefix)
    print(f"input_prefix\t{args.input_prefix}")
    print(f"output_prefix\t{args.output_prefix}")
    print(f"variants_total\t{total}")
    print(f"variants_lifted\t{lifted}")
    print(f"variants_skipped\t{skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
