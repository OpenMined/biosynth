#!/usr/bin/env bash
# Resume-enabled downloader for the GRCh38 dbSNP VCF (~28GB) + index + md5.
#
#   scripts/download_dbsnp.sh [DEST_DIR]
#
# Safe to re-run: curl -C - resumes partial files. Verifies md5 at the end.
# Default DEST_DIR = notebooks/data (the path biosynth scripts expect).

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="${1:-${ROOT_DIR}/notebooks/data}"
BASE_URL="https://ftp.ncbi.nih.gov/snp/latest_release/VCF"
FILES=("GCF_000001405.40.gz" "GCF_000001405.40.gz.tbi" "GCF_000001405.40.gz.md5")

mkdir -p "${DEST_DIR}"
cd "${DEST_DIR}"

echo "Destination : ${DEST_DIR}"
avail=$(df -h "${DEST_DIR}" | awk 'NR==2{print $4}')
echo "Free space  : ${avail} (need ~28GB for the .gz alone)"

fetch() {
  local f="$1"
  echo "=== ${f} ==="
  if command -v aria2c >/dev/null 2>&1; then
    # 16 parallel segments + resume (-c). NCBI throttles per-connection,
    # so this is ~10-50x faster than a single curl stream.
    aria2c -c -x16 -s16 -k1M --retry-wait=5 --max-tries=0 \
           --file-allocation=none --summary-interval=30 \
           -d . -o "${f}" "${BASE_URL}/${f}"
  else
    curl -fL --retry 10 --retry-delay 5 --retry-connrefused \
         -C - -o "${f}" "${BASE_URL}/${f}"
  fi
}

for f in "${FILES[@]}"; do
  fetch "${f}"
done

echo "=== verifying md5 ==="
if command -v md5sum >/dev/null 2>&1; then
  # NCBI md5 file lists the .gz; check just that line.
  grep 'GCF_000001405.40.gz$' GCF_000001405.40.gz.md5 | md5sum -c -
elif command -v md5 >/dev/null 2>&1; then
  expected=$(awk '/GCF_000001405.40.gz$/{print $1}' GCF_000001405.40.gz.md5)
  actual=$(md5 -q GCF_000001405.40.gz)
  if [[ "${expected}" == "${actual}" ]]; then
    echo "GCF_000001405.40.gz: OK"
  else
    echo "GCF_000001405.40.gz: FAILED (expected ${expected}, got ${actual})" >&2
    exit 1
  fi
else
  echo "No md5 tool found; skipping verification." >&2
fi

echo "Done. dbSNP VCF ready at ${DEST_DIR}/GCF_000001405.40.gz"
