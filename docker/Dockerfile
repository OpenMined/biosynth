FROM rust:1.91-bookworm AS builder

LABEL org.opencontainers.image.source=https://github.com/openmined/biosynth
LABEL org.opencontainers.image.description="Biosynth CLI: synthetic data generator for BioVault"
LABEL org.opencontainers.image.licenses=Apache-2.0

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY cli/Cargo.toml cli/Cargo.lock ./cli/
COPY cli/src ./cli/src

RUN cargo build --release --manifest-path cli/Cargo.toml

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates gzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/cli/target/release/bvs /usr/local/bin/bvs
COPY data/genostats.sqlite /app/data/genostats.sqlite
RUN test -s /app/data/genostats.sqlite

ENTRYPOINT ["bvs"]
CMD ["--help"]
