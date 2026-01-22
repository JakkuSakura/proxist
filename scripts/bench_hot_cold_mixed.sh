#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/container-compose.yaml"
COMPOSE_CMD=(docker compose -f "${COMPOSE_FILE}")
PROXIST_PORT="${PROXIST_PORT:-18124}"
PROXIST_ADDR="127.0.0.1:${PROXIST_PORT}"
CUTOFF_MICROS="${PROXIST_PERSISTED_CUTOFF_OVERRIDE_MICROS:-1704103205000000}"

if ! command -v docker >/dev/null 2>&1; then
  if command -v podman >/dev/null 2>&1; then
    COMPOSE_CMD=(podman compose -f "${COMPOSE_FILE}")
    if podman system connection list >/dev/null 2>&1; then
      if podman system connection list | awk '{print $1}' | grep -q '^podman-local$'; then
        export PODMAN_CONNECTION=podman-local
        export CONTAINER_CONNECTION=podman-local
      fi
    fi
  else
    echo "docker or podman is required to run benchmarks." >&2
    exit 1
  fi
fi

TMP_DIR="${ROOT_DIR}/.bench_tmp"
mkdir -p "${TMP_DIR}"
METADATA_DB="${TMP_DIR}/proxist-meta.db"
METADATA_URI="${PROXIST_METADATA_SQLITE_URI:-sqlite::memory:?cache=shared}"
echo "Metadata DB: ${METADATA_URI}"

if command -v lsof >/dev/null 2>&1; then
  EXISTING_PID=$(lsof -nP -iTCP:${PROXIST_PORT} -sTCP:LISTEN 2>/dev/null | awk 'NR==2 {print $2}')
  if [[ -n "${EXISTING_PID}" ]]; then
    echo "Stopping existing proxistd on port ${PROXIST_PORT} (pid ${EXISTING_PID})..."
    kill "${EXISTING_PID}" >/dev/null 2>&1 || true
    sleep 1
  fi
fi
PROXIST_LOG="$(mktemp)"
WAL_DIR="$(mktemp -d)"
PROXIST_PID=""
KEEP_LOG=0

cleanup() {
  if [[ -n "${PROXIST_PID}" ]] && kill -0 "${PROXIST_PID}" >/dev/null 2>&1; then
    echo "Stopping proxistd..."
    kill "${PROXIST_PID}" >/dev/null 2>&1 || true
    wait "${PROXIST_PID}" 2>/dev/null || true
  fi
  if [[ "${KEEP_LOG}" -eq 0 ]]; then
    rm -f "${PROXIST_LOG}"
  else
    echo "proxistd log retained at ${PROXIST_LOG}"
  fi
  rm -rf "${TMP_DIR}"
  rm -rf "${WAL_DIR}"
  echo "Stopping ClickHouse..."
  "${COMPOSE_CMD[@]}" down --remove-orphans >/dev/null
}
trap cleanup EXIT

echo "Starting ClickHouse for hot/cold/mixed benchmarks..."
"${COMPOSE_CMD[@]}" up -d clickhouse >/dev/null

until "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
  sleep 1
done

echo "Starting proxistd (release build)..."
(
  cd "${ROOT_DIR}" && \
  PROXIST_METADATA_SQLITE_PATH="${METADATA_URI}" \
  PROXIST_HTTP_ADDR="${PROXIST_ADDR}" \
  PROXIST_CLICKHOUSE_ENDPOINT="http://127.0.0.1:18123" \
  PROXIST_CLICKHOUSE_DATABASE="proxist" \
  PROXIST_CLICKHOUSE_TABLE="ticks" \
  PROXIST_WAL_DIR="${WAL_DIR}" \
  PROXIST_PERSISTED_CUTOFF_OVERRIDE_MICROS="${CUTOFF_MICROS}" \
  RUST_LOG="debug" \
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

echo "Creating schema in ClickHouse..."
"${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "CREATE DATABASE IF NOT EXISTS proxist" >/dev/null
"${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "DROP TABLE IF EXISTS proxist.ticks" >/dev/null
"${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "CREATE TABLE proxist.ticks (tenant String, shard_id String, symbol String, ts_micros Int64, payload_base64 String, seq UInt64) ENGINE = MergeTree ORDER BY (tenant, symbol, ts_micros)" >/dev/null

COLD_INSERT_SQL="
INSERT INTO proxist.ticks SELECT
  'alpha' AS tenant,
  'alpha-shard' AS shard_id,
  concat('SYM', toString(number % 10)) AS symbol,
  toUnixTimestamp64Micro(toDateTime64('2024-01-01 10:00:00', 6) + number * 0.000001) AS ts_micros,
  base64Encode(toString(number)) AS payload_base64,
  number AS seq
FROM numbers(200000);
"

echo "Loading cold dataset directly into ClickHouse..."
"${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "${COLD_INSERT_SQL}" >/dev/null

if [[ "${PROXIST_BENCH_DEBUG_CH:-0}" == "1" ]]; then
  CH_SAMPLE_FILE="$(mktemp)"
  "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "SELECT symbol, ts_micros, payload_base64 FROM proxist.ticks WHERE tenant = 'alpha' AND symbol IN ('SYM1','SYM2','SYM3') AND ts_micros BETWEEN 1704103200000000 AND 1704103204000000 ORDER BY ts_micros ASC FORMAT JSONEachRow" >"${CH_SAMPLE_FILE}"
  echo "ClickHouse sample output saved to ${CH_SAMPLE_FILE}"
  head -n 5 "${CH_SAMPLE_FILE}"
fi

HOT_INGEST_PAYLOAD=$(cat <<'JSON'
{
  "ticks": [
    {"tenant":"alpha","symbol":"SYM1","timestamp":1704103206000000,"payload":"AQ==","seq": 1},
    {"tenant":"alpha","symbol":"SYM2","timestamp":1704103207000000,"payload":"Ag==","seq": 2},
    {"tenant":"alpha","symbol":"SYM3","timestamp":1704103208000000,"payload":"Aw==","seq": 3}
  ]
}
JSON
)

echo "Ingesting hot dataset through proxistd..."
INGEST_RESPONSE="$(mktemp)"
HTTP_STATUS=$(curl -sS -o "${INGEST_RESPONSE}" -w '%{http_code}' \
  -H 'Content-Type: application/json' \
  --data-binary "${HOT_INGEST_PAYLOAD}" \
  "http://${PROXIST_ADDR}/ingest" || true)
if [[ "${HTTP_STATUS}" != "202" ]]; then
  echo "hot ingest failed (HTTP ${HTTP_STATUS})" >&2
  cat "${INGEST_RESPONSE}" >&2 || true
  if [[ -f "${PROXIST_LOG}" ]]; then
    echo "proxistd logs:" >&2
    cat "${PROXIST_LOG}" >&2
  fi
  KEEP_LOG=1
  exit 1
fi
rm -f "${INGEST_RESPONSE}"

HOT_QUERY=$(cat <<'JSON'
{
  "tenant": "alpha",
  "symbols": ["SYM1", "SYM2", "SYM3"],
  "range": {"start": 1704103206000000, "end": 1704103209000000},
  "include_cold": false,
  "op": "range"
}
JSON
)

COLD_QUERY=$(cat <<'JSON'
{
  "tenant": "alpha",
  "symbols": ["SYM1", "SYM2", "SYM3"],
  "range": {"start": 1704103200000000, "end": 1704103204000000},
  "include_cold": true,
  "op": "range"
}
JSON
)

MIXED_QUERY=$(cat <<'JSON'
{
  "tenant": "alpha",
  "symbols": ["SYM1", "SYM2", "SYM3"],
  "range": {"start": 1704103200000000, "end": 1704103209000000},
  "include_cold": true,
  "op": "range"
}
JSON
)

bench_query() {
  local name="$1"
  local payload="$2"
  local iterations=5
  local total=0

  echo "Running ${name} benchmark (${iterations} iterations)..."
  for i in $(seq 1 ${iterations}); do
    local start
    local end
    start=$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)
    local response_file
    response_file="$(mktemp)"
    local status
    status=$(curl -sS -o "${response_file}" -w '%{http_code}' \
      -H 'Content-Type: application/json' \
      --data-binary "${payload}" \
      "http://${PROXIST_ADDR}/query" || true)
    end=$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)

    local duration=$((end - start))
    if [[ "${status}" != "200" ]]; then
      echo "  iteration ${i}: ${duration} ms (HTTP ${status})"
      cat "${response_file}" >&2 || true
      rm -f "${response_file}"
      KEEP_LOG=1
      total=$((total + duration))
      continue
    fi

    local count
    count=$(python3 - <<PY
import json,sys
payload = json.load(open("${response_file}", "r"))
print(len(payload.get("rows", [])))
PY
)
    rm -f "${response_file}"

    echo "  iteration ${i}: ${duration} ms (rows=${count})"
    total=$((total + duration))
  done

  local avg=$((total / iterations))
  echo "${name} average latency: ${avg} ms"
}

bench_query "hot-only" "${HOT_QUERY}"
bench_query "cold-only" "${COLD_QUERY}"
bench_query "mixed" "${MIXED_QUERY}"

echo "Benchmarks complete. Persisted cutoff override: ${CUTOFF_MICROS} micros."
