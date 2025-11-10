#!/usr/bin/env bash
set -euo pipefail

# Simple test runner (fast suite only for now)
# Usage:
#   ./test.sh            # fast (default)
#   ./test.sh --fast     # fast only
#   ./test.sh --all      # same as --fast (placeholder for future suites)

MODE=${1:---fast}

cd cli

echo "==> cargo fmt"
cargo fmt

echo "==> cargo clippy"
cargo clippy --all-targets --all-features -q || true

run_fast() {
  echo "==> Running fast tests"
  cargo test
}

case "$MODE" in
  --fast)
    run_fast
    ;;
  --all)
    run_fast
    ;;
  *)
    echo "Unknown option: $MODE" >&2
    echo "Usage: $0 [--fast|--all]" >&2
    exit 2
    ;;
esac
