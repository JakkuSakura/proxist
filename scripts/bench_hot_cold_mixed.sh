#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/container-compose.yaml"
COMPOSE_CMD=(docker compose -f "${COMPOSE_FILE}")
PROXIST_PORT="${PROXIST_PORT:-18124}"
PROXIST_ADDR="127.0.0.1:${PROXIST_PORT}"
CUTOFF_MICROS="${PROXIST_PERSISTED_CUTOFF_OVERRIDE_MICROS:-1704103205000000}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run benchmarks." >&2
  exit 1
fi

METADATA_DB="$(mktemp)"
PROXIST_LOG="$(mktemp)"
WAL_DIR="$(mktemp -d)"
PROXIST_PID=""

cleanup() {
  if [[ -n "${PROXIST_PID}" ]] && kill -0 "${PROXIST_PID}" >/dev/null 2>&1; then
    echo "Stopping proxistd..."
    kill "${PROXIST_PID}" >/dev/null 2>&1 || true
    wait "${PROXIST_PID}" 2>/dev/null || true
  fi
  rm -f "${METADATA_DB}" "${PROXIST_LOG}"
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
  PROXIST_METADATA_SQLITE_PATH="${METADATA_DB}" \
  PROXIST_HTTP_ADDR="${PROXIST_ADDR}" \
  PROXIST_CLICKHOUSE_ENDPOINT="http://127.0.0.1:18123" \
  PROXIST_CLICKHOUSE_DATABASE="proxist" \
  PROXIST_CLICKHOUSE_TABLE="ticks" \
  PROXIST_WAL_DIR="${WAL_DIR}" \
  PROXIST_PERSISTED_CUTOFF_OVERRIDE_MICROS="${CUTOFF_MICROS}" \
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
CREATE_SQL="
DROP TABLE IF EXISTS ticks;
CREATE TABLE ticks (
    tenant String,
    shard_id String,
    symbol String,
    ts_micros Int64,
    payload_base64 String,
    seq UInt64
) ENGINE = MergeTree ORDER BY (tenant, symbol, ts_micros);
"
"${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "${CREATE_SQL}" >/dev/null

COLD_INSERT_SQL="
INSERT INTO ticks SELECT
  'alpha' AS tenant,
  'alpha-shard' AS shard_id,
  concat('SYM', toString(number % 10)) AS symbol,
  toUnixTimestamp64Micro(toDateTime64('2024-01-01 10:00:00', 6) + number * 0.000001) AS ts_micros,
  toString(number) AS payload_base64,
  number AS seq
FROM numbers(200000);
"

echo "Loading cold dataset directly into ClickHouse..."
"${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "${COLD_INSERT_SQL}" >/dev/null

HOT_INGEST_PAYLOAD=$(cat <<'JSON'
{
  "ticks": [
    {"tenant":"alpha","symbol":"SYM1","timestamp":"2024-01-01T10:00:06Z","payload":"AQ==","seq": 1},
    {"tenant":"alpha","symbol":"SYM2","timestamp":"2024-01-01T10:00:07Z","payload":"Ag==","seq": 2},
    {"tenant":"alpha","symbol":"SYM3","timestamp":"2024-01-01T10:00:08Z","payload":"Aw==","seq": 3}
  ]
}
JSON
)

echo "Ingesting hot dataset through proxistd..."
HTTP_STATUS=$(curl -sS -o /dev/null -w '%{http_code}' \
  -H 'Content-Type: application/json' \
  --data-binary "${HOT_INGEST_PAYLOAD}" \
  "http://${PROXIST_ADDR}/ingest")
if [[ "${HTTP_STATUS}" != "202" ]]; then
  echo "hot ingest failed (HTTP ${HTTP_STATUS})" >&2
  cat "${PROXIST_LOG}" >&2
  exit 1
fi

read -r -d '' HOT_QUERY <<'JSON'
{
  "tenant": "alpha",
  "symbols": ["SYM1", "SYM2", "SYM3"],
  "range": {"start": "2024-01-01T10:00:06Z", "end": "2024-01-01T10:00:09Z"},
  "include_cold": false,
  "op": "range"
}
JSON

read -r -d '' COLD_QUERY <<'JSON'
{
  "tenant": "alpha",
  "symbols": ["SYM1", "SYM2", "SYM3"],
  "range": {"start": "2024-01-01T10:00:00Z", "end": "2024-01-01T10:00:04Z"},
  "include_cold": true,
  "op": "range"
}
JSON

read -r -d '' MIXED_QUERY <<'JSON'
{
  "tenant": "alpha",
  "symbols": ["SYM1", "SYM2", "SYM3"],
  "range": {"start": "2024-01-01T10:00:00Z", "end": "2024-01-01T10:00:09Z"},
  "include_cold": true,
  "op": "range"
}
JSON

bench_query() {
  local name="$1"
  local payload="$2"
  local iterations=5
  local total=0

  echo "Running ${name} benchmark (${iterations} iterations)..."
  for i in $(seq 1 ${iterations}); do
    local start
    local end
    start=$(date +%s%3N)
    local response
    response=$(curl -sS \
      -H 'Content-Type: application/json' \
      --data-binary "${payload}" \
      "http://${PROXIST_ADDR}/query")
    end=$(date +%s%3N)

    local count
    count=$(python - <<PY
import json,sys
payload = json.loads(sys.stdin.read())
print(len(payload.get("rows", [])))
PY
<<<"${response}")

    local duration=$((end - start))
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
