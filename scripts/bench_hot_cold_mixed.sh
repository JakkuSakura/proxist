#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/container-compose.yaml"
COMPOSE_CMD=(docker compose -f "${COMPOSE_FILE}")
PROXIST_PORT="${PROXIST_PORT:-18124}"
PROXIST_ADDR="127.0.0.1:${PROXIST_PORT}"
CUTOFF_MICROS="${PROXIST_PERSISTED_CUTOFF_OVERRIDE_MICROS:-1704103205000000}"
PROXIST_PG_PORT="${PROXIST_PG_PORT:-15432}"
PROXIST_PG_ADDR="127.0.0.1:${PROXIST_PG_PORT}"
PROXIST_PG_USER="${PROXIST_PG_USER:-postgres}"
COLD_ROWS="${PROXIST_BENCH_COLD_ROWS:-2000000}"
HOT_BATCH_SIZE="${PROXIST_BENCH_HOT_BATCH_SIZE:-2000}"
HOT_BATCHES="${PROXIST_BENCH_HOT_BATCHES:-5}"
WINDOW_MICROS="${PROXIST_BENCH_WINDOW_MICROS:-1000000}"

if command -v docker >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose -f "${COMPOSE_FILE}")
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose -f "${COMPOSE_FILE}")
elif command -v podman >/dev/null 2>&1; then
  COMPOSE_CMD=(podman compose -f "${COMPOSE_FILE}")
  if podman system connection list >/dev/null 2>&1; then
    if podman system connection list | awk '{print $1}' | grep -q '^podman-local$'; then
      export PODMAN_CONNECTION=podman-local
      export CONTAINER_CONNECTION=podman-local
    fi
  fi
else
  echo "docker, docker-compose, or podman is required to run benchmarks." >&2
  exit 1
fi

TMP_DIR="${ROOT_DIR}/.bench_tmp"
mkdir -p "${TMP_DIR}"
METADATA_DB="${TMP_DIR}/proxist-meta.db"
METADATA_URI="${PROXIST_METADATA_SQLITE_URI:-sqlite::memory:?cache=shared}"
echo "Metadata DB: ${METADATA_URI}"

if command -v lsof >/dev/null 2>&1; then
  EXISTING_PID=$(lsof -nP -iTCP:${PROXIST_PORT} -sTCP:LISTEN 2>/dev/null | awk 'NR==2 {print $2}' || true)
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
  PROXIST_PG_ADDR="${PROXIST_PG_ADDR}" \
  PROXIST_PG_DIALECT="clickhouse" \
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
  if psql -h 127.0.0.1 -p "${PROXIST_PG_PORT}" -U "${PROXIST_PG_USER}" -d postgres -c "SELECT 1" >/dev/null 2>&1; then
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

COLD_BASE_MICROS=$((CUTOFF_MICROS - COLD_ROWS - 1))
COLD_INSERT_SQL="
INSERT INTO proxist.ticks SELECT
  'alpha' AS tenant,
  'alpha-shard' AS shard_id,
  concat('SYM', toString(number % 10)) AS symbol,
  ${COLD_BASE_MICROS} + number AS ts_micros,
  base64Encode(toString(number)) AS payload_base64,
  number AS seq
FROM numbers(${COLD_ROWS});
"

echo "Loading cold dataset directly into ClickHouse..."
"${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "${COLD_INSERT_SQL}" >/dev/null

if [[ "${PROXIST_BENCH_DEBUG_CH:-0}" == "1" ]]; then
  CH_SAMPLE_FILE="$(mktemp)"
  "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "SELECT symbol, ts_micros, payload_base64 FROM proxist.ticks WHERE tenant = 'alpha' AND symbol IN ('SYM1','SYM2','SYM3') AND ts_micros BETWEEN 1704103200000000 AND 1704103204000000 ORDER BY ts_micros ASC FORMAT JSONEachRow" >"${CH_SAMPLE_FILE}"
  echo "ClickHouse sample output saved to ${CH_SAMPLE_FILE}"
  head -n 5 "${CH_SAMPLE_FILE}"
fi

HOT_BASE_MICROS=$((CUTOFF_MICROS + 1))
HOT_LAST_MICROS="${HOT_BASE_MICROS}"
HOT_SEQ=1

append_hot_batch() {
  local batch_start="${HOT_LAST_MICROS}"
  local batch_size="${HOT_BATCH_SIZE}"
  local sql
  sql=$(python3 - <<PY
batch_start=${batch_start}
batch_size=${batch_size}
seq_start=${HOT_SEQ}
symbols=["SYM1","SYM2","SYM3"]
rows=[]
for i in range(batch_size):
    ts=batch_start+i+1
    sym=symbols[i % len(symbols)]
    rows.append(f"('alpha','{sym}',{ts},'AQ==',{seq_start+i})")
print("INSERT INTO ticks (tenant, symbol, ts_micros, payload_base64, seq) VALUES " + ",".join(rows))
PY
)
  if ! psql -h 127.0.0.1 -p "${PROXIST_PG_PORT}" -U "${PROXIST_PG_USER}" -d postgres -c "${sql}" >/dev/null 2>&1; then
    return 1
  fi
  HOT_SEQ=$((HOT_SEQ + batch_size))
  HOT_LAST_MICROS=$((batch_start + batch_size))
  return 0
}

echo "Appending hot dataset through proxistd (psql)..."
for _ in $(seq 1 "${HOT_BATCHES}"); do
  if ! append_hot_batch; then
    echo "hot append failed via psql" >&2
    if [[ -f "${PROXIST_LOG}" ]]; then
      echo "proxistd logs:" >&2
      cat "${PROXIST_LOG}" >&2
    fi
    KEEP_LOG=1
    exit 1
  fi
done

build_hot_query() {
  local end_micros="$1"
  local start_micros=$((end_micros - WINDOW_MICROS + 1))
  cat <<SQL
SELECT symbol, ts_micros, payload_base64
FROM ticks
WHERE tenant = 'alpha'
  AND symbol IN ('SYM1','SYM2','SYM3')
  AND ts_micros BETWEEN ${start_micros} AND ${end_micros}
ORDER BY ts_micros ASC
FORMAT JSONEachRow
SQL
}

build_cold_query() {
  local cold_start="${COLD_BASE_MICROS}"
  local cold_end=$((CUTOFF_MICROS - 1))
  cat <<SQL
SELECT symbol, ts_micros, payload_base64
FROM ticks
WHERE tenant = 'alpha'
  AND symbol IN ('SYM1','SYM2','SYM3')
  AND ts_micros BETWEEN ${cold_start} AND ${cold_end}
ORDER BY ts_micros ASC
FORMAT JSONEachRow
SQL
}

build_mixed_query() {
  local end_micros="$1"
  local start_micros=$((CUTOFF_MICROS - (WINDOW_MICROS / 2)))
  cat <<SQL
SELECT symbol, ts_micros, payload_base64
FROM ticks
WHERE tenant = 'alpha'
  AND symbol IN ('SYM1','SYM2','SYM3')
  AND ts_micros BETWEEN ${start_micros} AND ${end_micros}
ORDER BY ts_micros ASC
FORMAT JSONEachRow
SQL
}

bench_query() {
  local name="$1"
  local mode="$2"
  local iterations=5
  local total=0

  echo "Running ${name} benchmark (${iterations} iterations)..."
  for i in $(seq 1 ${iterations}); do
    if ! append_hot_batch; then
      echo "hot append failed via psql" >&2
      KEEP_LOG=1
      break
    fi
    local payload
    case "${mode}" in
      hot)
        payload="$(build_hot_query "${HOT_LAST_MICROS}")"
        ;;
      cold)
        payload="$(build_cold_query)"
        ;;
      mixed)
        payload="$(build_mixed_query "${HOT_LAST_MICROS}")"
        ;;
      *)
        echo "unknown bench mode ${mode}" >&2
        KEEP_LOG=1
        break
        ;;
    esac
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
    if psql -h 127.0.0.1 -p "${PROXIST_PG_PORT}" -U "${PROXIST_PG_USER}" -d postgres \
      -At -F $'\t' -c "${payload}" >"${response_file}" 2>/dev/null; then
      status="200"
    else
      status="500"
    fi
    end=$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)

    local duration=$((end - start))
    if [[ "${status}" != "200" ]]; then
      echo "  iteration ${i}: ${duration} ms (PG ${status})"
      cat "${response_file}" >&2 || true
      rm -f "${response_file}"
      KEEP_LOG=1
      total=$((total + duration))
      continue
    fi

    local count
    count=$(wc -l <"${response_file}" | tr -d ' ')
    rm -f "${response_file}"

    echo "  iteration ${i}: ${duration} ms (rows=${count})"
    total=$((total + duration))
  done

  local avg=$((total / iterations))
  echo "${name} average latency: ${avg} ms"
}

bench_query "hot-only" "hot"
bench_query "cold-only" "cold"
bench_query "mixed" "mixed"

echo "Benchmarks complete. Persisted cutoff override: ${CUTOFF_MICROS} micros."
