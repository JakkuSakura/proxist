#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROWS="${ROWS:-10000000}"
RANDOM_READS="${RANDOM_READS:-$((ROWS/10))}"
CACHE_ROWS="${CACHE_ROWS:-1000000}"
BATCH_SIZE="${BATCH_SIZE:-100000}"
STAMP="$(date +%Y%m%d%H%M%S)"
OUT_DIR="${OUT_DIR:-"${ROOT_DIR}/bench_results/pxd-kx-compare_${STAMP}"}"
BUILD_MODE="release"

mkdir -p "${OUT_DIR}"

if ! command -v q >/dev/null 2>&1; then
  echo "q not found in PATH" >&2
  exit 1
fi

{
  echo "rows=${ROWS}"
  echo "random_reads=${RANDOM_READS}"
  echo "cache_rows=${CACHE_ROWS}"
  echo "batch_size=${BATCH_SIZE}"
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "uname=$(uname -a)"
  if command -v rustc >/dev/null 2>&1; then
    echo "rustc=$(rustc --version)"
  fi
  if command -v cargo >/dev/null 2>&1; then
    echo "cargo=$(cargo --version)"
  fi
  echo "pxd_build=${BUILD_MODE}"
  echo "q=$(q -version 2>/dev/null || true)"
} > "${OUT_DIR}/meta.txt"

BUILD_MODE="release"
cargo run -q -p pxd-bench --bin pxd_nano --release -- \
  --rows "${ROWS}" \
  --random-reads "${RANDOM_READS}" \
  --cache-rows "${CACHE_ROWS}" \
  --batch "${BATCH_SIZE}" \
  > "${OUT_DIR}/pxd.csv"

ROWS="${ROWS}" RANDOM_READS="${RANDOM_READS}" CACHE_ROWS="${CACHE_ROWS}" \
  q "${ROOT_DIR}/benches/pxd-kx-compare/kdb_bench.q" \
  > "${OUT_DIR}/kdb.csv"

echo "Results written to ${OUT_DIR}"
