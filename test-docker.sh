#!/usr/bin/env sh
set -eu

IMAGE_TAG="${IMAGE_TAG:-biosynth:ci}"
OUTPUT_FILE="${OUTPUT_FILE:-/out/out.vcf}"

docker build -f docker/Dockerfile -t "${IMAGE_TAG}" .

mkdir -p out

docker run --rm --read-only \
  --tmpfs /tmp:rw,exec,mode=1777 \
  -e BVS_READ_ONLY_DB=1 \
  -v "$PWD/out:/out" \
  "${IMAGE_TAG}" \
  synthetic \
  --output /out/genotypes/{index}.txt \
  --count 1 \
  --seed 42

docker run --rm --read-only \
  --tmpfs /tmp:rw,exec,mode=1777 \
  -e BVS_READ_ONLY_DB=1 \
  -v "$PWD/out:/out" \
  "${IMAGE_TAG}" \
  genotype-to-vcf \
  --input /out/genotypes/0001.txt \
  --output "${OUTPUT_FILE}"
