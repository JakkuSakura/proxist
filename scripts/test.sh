#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/container-compose.yaml"
COMPOSE_CMD=(docker compose -f "${COMPOSE_FILE}")
PROXIST_PORT="${PROXIST_PORT:-18124}"
PROXIST_ADDR="127.0.0.1:${PROXIST_PORT}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run the SQL tests." >&2
  exit 1
fi

echo "Starting ClickHouse via docker compose..."
"${COMPOSE_CMD[@]}" up -d clickhouse >/dev/null

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

until "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
  sleep 1
done

echo "Starting proxistd..."
(
  cd "${ROOT_DIR}" && \
  PROXIST_METADATA_SQLITE_PATH="${METADATA_DB}" \
  PROXIST_HTTP_ADDR="${PROXIST_ADDR}" \
  PROXIST_CLICKHOUSE_ENDPOINT="http://127.0.0.1:18123" \
  PROXIST_CLICKHOUSE_DATABASE="proxist" \
  PROXIST_CLICKHOUSE_TABLE="ticks" \
  PROXIST_WAL_DIR="${WAL_DIR}" \
  cargo run --quiet --bin proxistd
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

echo "proxistd is ready."

STATUS=0
TMP_OUTPUT="$(mktemp)"
RAW_OUTPUT="$(mktemp)"

for sql_file in "${ROOT_DIR}"/tests/sql/*.sql; do
  [[ -e "$sql_file" ]] || continue
  base="$(basename "${sql_file}" .sql)"
  expected_file="${ROOT_DIR}/tests/sql/${base}.expected"

  echo "Executing SQL script: ${base}"
  HTTP_STATUS=$(curl -sS -o "${RAW_OUTPUT}" -w '%{http_code}' \
    -H 'Content-Type: text/plain' \
    --data-binary @"${sql_file}" \
    "http://${PROXIST_ADDR}/?database=proxist")

  if [[ "${HTTP_STATUS}" != "200" ]]; then
    echo "  ❌ execution failed for ${sql_file} (HTTP ${HTTP_STATUS})" >&2
    cat "${RAW_OUTPUT}" >&2
    STATUS=1
    continue
  fi

  tr -d '\r' <"${RAW_OUTPUT}" |
    awk '{ if ($0 == "Ok." || $0 == "") next; print $0 }' >"${TMP_OUTPUT}"

  if [[ -f "${expected_file}" ]]; then
    if ! diff -u "${expected_file}" "${TMP_OUTPUT}"; then
      echo "  ❌ output mismatch for ${base}" >&2
      STATUS=1
    else
      echo "  ✅ output matches expected results"
    fi
  else
    echo "  ⚠️  no expected file for ${base}; raw output:"
    cat "${TMP_OUTPUT}"
  fi

done

rm -f "${TMP_OUTPUT}" "${RAW_OUTPUT}"
exit "${STATUS}"
