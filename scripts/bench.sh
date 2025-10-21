#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/container-compose.yaml"
COMPOSE_CMD=(docker compose -f "${COMPOSE_FILE}")
PROXIST_PORT="${PROXIST_PORT:-18124}"
PROXIST_ADDR="127.0.0.1:${PROXIST_PORT}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run benchmarks." >&2
  exit 1
fi

echo "Starting ClickHouse for benchmarks..."
"${COMPOSE_CMD[@]}" up -d clickhouse >/dev/null

METADATA_DB="$(mktemp)"
PROXIST_LOG="$(mktemp)"
PROXIST_PID=""

cleanup() {
  if [[ -n "${PROXIST_PID}" ]] && kill -0 "${PROXIST_PID}" >/dev/null 2>&1; then
    echo "Stopping proxistd..."
    kill "${PROXIST_PID}" >/dev/null 2>&1 || true
    wait "${PROXIST_PID}" 2>/dev/null || true
  fi
  rm -f "${METADATA_DB}" "${PROXIST_LOG}"
  echo "Stopping ClickHouse..."
  "${COMPOSE_CMD[@]}" down --remove-orphans >/dev/null
}
trap cleanup EXIT

until "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
  sleep 1
done

echo "Starting proxistd (release build) for benchmarks..."
(
  cd "${ROOT_DIR}" && \
  PROXIST_METADATA_SQLITE_PATH="${METADATA_DB}" \
  PROXIST_HTTP_ADDR="${PROXIST_ADDR}" \
  PROXIST_CLICKHOUSE_ENDPOINT="http://127.0.0.1:18123" \
  PROXIST_CLICKHOUSE_DATABASE="proxist" \
  PROXIST_CLICKHOUSE_TABLE="ticks" \
  cargo run --quiet --release --bin proxistd
) >"${PROXIST_LOG}" 2>&1 &
PROXIST_PID=$!

for _ in {1..60}; do
  if curl -s -o /dev/null -w '%{http_code}' -X POST --data 'SELECT 1 FORMAT TSV' "http://${PROXIST_ADDR}/" | grep -q '^200$'; then
    break
  fi
  sleep 1
  if ! kill -0 "${PROXIST_PID}" >/dev/null 2>&1; then
    echo "proxistd exited unexpectedly. Logs:" >&2
    cat "${PROXIST_LOG}" >&2
    exit 1
  fi
  if [[ $_ -eq 60 ]]; then
    echo "proxistd did not become ready in time. Logs:" >&2
    cat "${PROXIST_LOG}" >&2
    exit 1
  fi
done

echo "Preparing benchmark dataset through proxist..."
SETUP_SQL="
DROP TABLE IF EXISTS ticks;
CREATE TABLE ticks (
    tenant String,
    shard_id String,
    symbol String,
    ts_micros Int64,
    payload_base64 String,
    seq UInt64
) ENGINE = MergeTree ORDER BY (tenant, symbol, ts_micros);
INSERT INTO ticks SELECT 'alpha', 'alpha-shard', concat('SYM', toString(number % 10)), toUnixTimestamp64Micro(toDateTime64('2024-01-01 09:30:00', 6) + number * 0.000001), toString(number), number FROM numbers(500000);
"
HTTP_STATUS=$(curl -sS -o /dev/null -w '%{http_code}' \
  -H 'Content-Type: text/plain' \
  --data-binary "${SETUP_SQL}" \
  "http://${PROXIST_ADDR}/?database=proxist")
if [[ "${HTTP_STATUS}" != "200" ]]; then
  echo "dataset preparation failed (HTTP ${HTTP_STATUS})" >&2
  cat "${PROXIST_LOG}" >&2
  exit 1
fi

QUERY="SELECT symbol, count(), any(seq) FROM ticks GROUP BY symbol FORMAT TSV"

TOTAL_MS=0
ITERATIONS=5

echo "Running proxist benchmark (${ITERATIONS} iterations)..."
for i in $(seq 1 ${ITERATIONS}); do
  START=$(date +%s%3N)
  HTTP_STATUS=$(curl -sS -o /dev/null -w '%{http_code}' \
    -H 'Content-Type: text/plain' \
    --data-binary "${QUERY}" \
    "http://${PROXIST_ADDR}/?database=proxist")
  END=$(date +%s%3N)
  if [[ "${HTTP_STATUS}" != "200" ]]; then
    echo "  iteration ${i} failed (HTTP ${HTTP_STATUS})" >&2
    STATUS=1
    break
  fi
  DURATION=$((END - START))
  echo "  iteration ${i}: ${DURATION} ms"
  TOTAL_MS=$((TOTAL_MS + DURATION))
done

if [[ ${TOTAL_MS} -gt 0 ]]; then
  AVG=$((TOTAL_MS / ITERATIONS))
  echo "Average latency: ${AVG} ms"
fi
