#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROWS="${ROWS:-10000000}"
RANDOM_READS="${RANDOM_READS:-$((ROWS/10))}"
CACHE_ROWS="${CACHE_ROWS:-1000000}"
BATCH_SIZE="${BATCH_SIZE:-100000}"
SYMBOL_CARD="${SYMBOL_CARD:-100000}"
ALLOW_OOM="${ALLOW_OOM:-0}"
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
  echo "symbol_card=${SYMBOL_CARD}"
  echo "randomread_note=vectorized_lcg_sequence"
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

mem_total_bytes() {
  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n hw.memsize 2>/dev/null || true
  elif [ -r /proc/meminfo ]; then
    awk '/MemTotal:/ {print $2 * 1024; exit}' /proc/meminfo
  else
    echo ""
  fi
}

MEM_TOTAL_BYTES="$(mem_total_bytes)"
if [ -n "${MEM_TOTAL_BYTES}" ] && [ "${ALLOW_OOM}" != "1" ]; then
  EST_BYTES=$((ROWS * 64 + CACHE_ROWS * 64))
  LIMIT_BYTES=$((MEM_TOTAL_BYTES * 60 / 100))
  if [ "${EST_BYTES}" -gt "${LIMIT_BYTES}" ]; then
    echo "Estimated memory ${EST_BYTES} bytes exceeds 60% of RAM ${MEM_TOTAL_BYTES}. Set ALLOW_OOM=1 to override." >&2
    exit 1
  fi
fi

BUILD_MODE="release"
cargo run -q -p pxd-bench --bin pxd_nano --release -- \
  --rows "${ROWS}" \
  --random-reads "${RANDOM_READS}" \
  --cache-rows "${CACHE_ROWS}" \
  --batch "${BATCH_SIZE}" \
  --symbol-card "${SYMBOL_CARD}" \
  > "${OUT_DIR}/pxd.csv"

Q_ULIMIT_KB=""
MEM_LIMIT_MB=""
if [ -n "${MEM_TOTAL_BYTES}" ] && [ "${ALLOW_OOM}" != "1" ]; then
  Q_ULIMIT_KB=$((MEM_TOTAL_BYTES * 60 / 100 / 1024))
  MEM_LIMIT_MB=$((MEM_TOTAL_BYTES * 60 / 100 / 1024 / 1024))
fi
if [ -n "${Q_ULIMIT_KB}" ]; then
  ulimit -v "${Q_ULIMIT_KB}" 2>/dev/null || true
fi

RAW_OUT="${OUT_DIR}/kdb_raw.txt"
CSV_OUT="${OUT_DIR}/kdb.csv"
ROWS="${ROWS}" RANDOM_READS="${RANDOM_READS}" CACHE_ROWS="${CACHE_ROWS}" SYMBOL_CARD="${SYMBOL_CARD}" MEM_LIMIT_MB="${MEM_LIMIT_MB}" \
  q "${ROOT_DIR}/benches/pxd-kx-compare/kdb_bench.q" >"${RAW_OUT}" 2>&1

awk -F',' '/^(test|write|reread|randomread|cpu|cpucache),/ {print}' "${RAW_OUT}" > "${CSV_OUT}"

echo "Results written to ${OUT_DIR}"
