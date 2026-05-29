#!/usr/bin/env bash
# Reproduce the Stage 1 parity + benchmark:
#   OLD: bvs emit-long (per file) -> bvs aggregate-long --threads 1
#   NEW: bvs fast-allele-freq (fused parse+aggregate, no .bvlr)
# Asserts the two allele_freq.tsv are byte-for-byte identical, reports timing/disk.
#
# Usage: scripts/bench_fast_allele_freq.sh [N_FILES] [LIMIT]
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-100}"
LIMIT="${2:-20000}"
BVS=./cli/target/release/bvs
DB=data/genostats.sqlite
BENCH=test_bench

cargo build --release --manifest-path cli/Cargo.toml >/dev/null

rm -rf "$BENCH"; mkdir -p "$BENCH/ddna" "$BENCH/illumina" "$BENCH/bvlr"

echo "## generate $N DDNA + $N Illumina (biallelic, limit $LIMIT)"
$BVS synthetic --format ddna --biallelic --seed 1 --count "$N" --limit "$LIMIT" \
  --output "$BENCH/ddna/ddna_{index}.txt" --sqlite "$DB" >/dev/null
$BVS synthetic --format illumina --clean-illumina-rsids --biallelic --seed 1 --count "$N" --limit "$LIMIT" \
  --output "$BENCH/illumina/ill_{index}.txt" --sqlite "$DB" >/dev/null

echo "## OLD: emit $((2*N)) -> .bvlr"
t0=$(date +%s)
for f in "$BENCH"/ddna/*.txt "$BENCH"/illumina/*.txt; do
  $BVS emit-long --input "$f" --output "$BENCH/bvlr/$(basename "$f" .txt).bvlr" --sqlite "$DB" >/dev/null 2>>"$BENCH/emit.log"
done
t1=$(date +%s)
echo "## OLD: aggregate-long --threads 1"
$BVS aggregate-long --input "$BENCH/bvlr" --allele-freq-tsv "$BENCH/old_allele_freq.tsv" --threads 1 >/dev/null 2>>"$BENCH/agg.log"
t2=$(date +%s)

echo "## NEW: fast-allele-freq"
t3=$(date +%s)
$BVS fast-allele-freq -i "$BENCH/ddna" -i "$BENCH/illumina" --sqlite "$DB" \
  --allele-freq-tsv "$BENCH/new_allele_freq.tsv" >/dev/null 2>>"$BENCH/new.log"
t4=$(date +%s)

echo
echo "=== PARITY ==="
if cmp -s "$BENCH/old_allele_freq.tsv" "$BENCH/new_allele_freq.tsv"; then
  echo "PASS: byte-for-byte identical ($(( $(wc -l <"$BENCH/old_allele_freq.tsv") - 1 )) loci)"
  echo "  sha256 $(shasum -a 256 "$BENCH/new_allele_freq.tsv" | cut -d' ' -f1)"
else
  echo "FAIL: outputs differ"; diff "$BENCH/old_allele_freq.tsv" "$BENCH/new_allele_freq.tsv" | head; exit 1
fi

echo
echo "=== BENCHMARK ($((2*N)) files) ==="
printf "OLD  emit:      %ss\n" "$((t1-t0))"
printf "OLD  aggregate: %ss\n" "$((t2-t1))"
printf "OLD  total:     %ss   intermediate .bvlr: %s\n" "$((t2-t0))" "$(du -sh "$BENCH/bvlr" | cut -f1)"
printf "NEW  total:     %ss   intermediate: 0\n" "$((t4-t3))"
