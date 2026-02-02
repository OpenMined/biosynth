#!/usr/bin/env sh
set -eu

IMAGE_TAG="${IMAGE_TAG:-biosynth:ci}"
INPUT_FILE="${INPUT_FILE:-/data/274939/274939_X_X_GSAv3-DTC_GRCh38-12-06-2025.txt}"
OUTPUT_FILE="${OUTPUT_FILE:-/out/out.vcf}"

docker build -f docker/Dockerfile -t "${IMAGE_TAG}" .

mkdir -p out

docker run --rm --read-only \
  --tmpfs /tmp:rw,exec,mode=1777 \
  -v "$PWD/test_data:/data:ro" \
  -v "$PWD/out:/out" \
  "${IMAGE_TAG}" \
  genotype-to-vcf \
  --input "${INPUT_FILE}" \
  --output "${OUTPUT_FILE}"
