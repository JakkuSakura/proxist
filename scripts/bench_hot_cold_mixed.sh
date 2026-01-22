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
WINDOW_MICROS="${PROXIST_BENCH_WINDOW_MICROS:-1000000}"
WINDOW_HALF=$((WINDOW_MICROS / 2))
WINDOW_START=$((CUTOFF_MICROS - WINDOW_HALF + 1))
WINDOW_END=$((WINDOW_START + WINDOW_MICROS - 1))
HOT_ROWS="${PROXIST_BENCH_HOT_ROWS:-$WINDOW_MICROS}"
if [[ "${HOT_ROWS}" -le 0 ]]; then
  echo "HOT_ROWS must be > 0" >&2
  exit 1
fi
HOT_BATCHES=$(( (HOT_ROWS + HOT_BATCH_SIZE - 1) / HOT_BATCH_SIZE ))

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
HOT_SEQ=1
HOT_LAST_MICROS=$((CUTOFF_MICROS + 1))

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

for _ in {1..60}; do
  if "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ $_ -eq 60 ]]; then
    echo "ClickHouse did not become ready in time." >&2
    exit 1
  fi
done

start_proxistd() {
  local cutoff="$1"
  local replay_persist="$2"
  PROXIST_LOG="$(mktemp)"
  echo "Starting proxistd (release build) with cutoff ${cutoff}..."
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
    PROXIST_WAL_REPLAY_PERSIST="${replay_persist}" \
    PROXIST_PERSISTED_CUTOFF_OVERRIDE_MICROS="${cutoff}" \
    RUST_LOG="debug" \
    cargo run --quiet --release --bin proxistd
  ) >"${PROXIST_LOG}" 2>&1 &
  PROXIST_PID=$!

  for _ in {1..60}; do
    if psql -h 127.0.0.1 -p "${PROXIST_PG_PORT}" -U "${PROXIST_PG_USER}" -d postgres -c "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    if ! kill -0 "${PROXIST_PID}" >/dev/null 2>&1; then
      echo "proxistd exited unexpectedly. Logs:" >&2
      cat "${PROXIST_LOG}" >&2
      return 1
    fi
  done
  echo "proxistd did not become ready in time. Logs:" >&2
  cat "${PROXIST_LOG}" >&2
  return 1
}

stop_proxistd() {
  if [[ -n "${PROXIST_PID}" ]] && kill -0 "${PROXIST_PID}" >/dev/null 2>&1; then
    echo "Stopping proxistd..."
    kill "${PROXIST_PID}" >/dev/null 2>&1 || true
    wait "${PROXIST_PID}" 2>/dev/null || true
  fi
  PROXIST_PID=""
}

echo "Ensuring database exists in ClickHouse..."
ch_query() {
  local sql="$1"
  local retries=10
  for _ in $(seq 1 ${retries}); do
    if "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "${sql}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ClickHouse query failed after ${retries} attempts: ${sql}" >&2
  return 1
}

ch_query "CREATE DATABASE IF NOT EXISTS proxist"

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

HOT_BASE_MICROS="${WINDOW_START}"
HOT_LAST_MICROS="${HOT_BASE_MICROS}"
HOT_SEQ=1

append_hot_batch() {
  local batch_start="${HOT_LAST_MICROS}"
  local batch_size="${1}"
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

append_hot_rows() {
  local remaining="${HOT_ROWS}"
  local batches="${HOT_BATCHES}"
  while [[ "${remaining}" -gt 0 ]]; do
    local size="${HOT_BATCH_SIZE}"
    if [[ "${remaining}" -lt "${size}" ]]; then
      size="${remaining}"
    fi
    if ! append_hot_batch "${size}"; then
      return 1
    fi
    remaining=$((remaining - size))
  done
  return 0
}

build_query() {
  cat <<SQL
SELECT symbol, ts_micros, payload_base64
FROM ticks
WHERE tenant = 'alpha'
  AND symbol IN ('SYM1','SYM2','SYM3')
  AND ts_micros BETWEEN ${WINDOW_START} AND ${WINDOW_END}
ORDER BY ts_micros ASC
SQL
}

bench_query() {
  local name="$1"
  local iterations=5
  local total=0
  local total_rows=0

  echo "Running ${name} benchmark (${iterations} iterations)..."
  for i in $(seq 1 ${iterations}); do
    local payload
    payload="$(build_query)"
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
    total_rows=$((total_rows + count))
  done

  local avg=$((total / iterations))
  local rows_per_sec
  rows_per_sec=$(python3 - <<PY
total_rows=${total_rows}
total_ms=${total}
if total_ms <= 0:
    print("0")
else:
    print(f"{(total_rows * 1000.0) / total_ms:.2f}")
PY
)
  echo "${name} average latency: ${avg} ms"
  echo "${name} rows/sec: ${rows_per_sec}"
}

setup_dataset() {
  echo "=== dataset setup ==="
  ch_query "DROP TABLE IF EXISTS proxist.ticks"
  if ! start_proxistd "${CUTOFF_MICROS}" "true"; then
    KEEP_LOG=1
    exit 1
  fi

  CREATE_SQL=$(cat <<'SQL'
CREATE TABLE proxist.ticks (
  tenant String,
  shard_id String,
  symbol String,
  ts_micros Int64,
  payload_base64 String,
  seq UInt64
) ENGINE = MergeTree
ORDER BY (tenant, symbol, ts_micros)
COMMENT 'proxist: order_col=ts_micros, payload_col=payload_base64, filter_cols=tenant symbol; seq_col=seq'
SQL
)
  CREATE_OUT="$(mktemp)"
  if ! psql -h 127.0.0.1 -p "${PROXIST_PG_PORT}" -U "${PROXIST_PG_USER}" -d postgres -v ON_ERROR_STOP=1 -c "${CREATE_SQL}" >"${CREATE_OUT}" 2>&1; then
    echo "create table failed via psql" >&2
    cat "${CREATE_OUT}" >&2 || true
    KEEP_LOG=1
    rm -f "${CREATE_OUT}"
    stop_proxistd
    exit 1
  fi
  rm -f "${CREATE_OUT}"

  echo "Loading cold dataset directly into ClickHouse..."
  ch_query "${COLD_INSERT_SQL}"

  if [[ "${PROXIST_BENCH_DEBUG_CH:-0}" == "1" ]]; then
    CH_SAMPLE_FILE="$(mktemp)"
    "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "SELECT symbol, ts_micros, payload_base64 FROM proxist.ticks WHERE tenant = 'alpha' AND symbol IN ('SYM1','SYM2','SYM3') AND ts_micros BETWEEN ${WINDOW_START} AND ${WINDOW_END} ORDER BY ts_micros ASC FORMAT JSONEachRow" >"${CH_SAMPLE_FILE}"
    echo "ClickHouse sample output saved to ${CH_SAMPLE_FILE}"
    head -n 5 "${CH_SAMPLE_FILE}"
  fi

  HOT_LAST_MICROS="${HOT_BASE_MICROS}"
  HOT_SEQ=1
  echo "Appending hot dataset through proxistd (psql)..."
  if ! append_hot_rows; then
    echo "hot append failed via psql" >&2
    KEEP_LOG=1
    stop_proxistd
    exit 1
  fi

  stop_proxistd
}

run_mode() {
  local name="$1"
  local cutoff="$2"
  echo "=== ${name} (cutoff ${cutoff}) ==="
  if ! start_proxistd "${cutoff}" "false"; then
    KEEP_LOG=1
    exit 1
  fi
  bench_query "${name}"
  stop_proxistd
}

HOT_ONLY_CUTOFF=$((WINDOW_START - 1))
COLD_ONLY_CUTOFF=$((WINDOW_END + 1))
MIXED_CUTOFF=$((WINDOW_START + 1000))

setup_dataset
run_mode "hot-only" "${HOT_ONLY_CUTOFF}"
run_mode "cold-only" "${COLD_ONLY_CUTOFF}"
run_mode "mixed" "${MIXED_CUTOFF}"

echo "Benchmarks complete. Persisted cutoff override: ${CUTOFF_MICROS} micros."
