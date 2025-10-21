#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/container-compose.yaml"
COMPOSE_CMD=(docker compose -f "${COMPOSE_FILE}")

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run the SQL tests." >&2
  exit 1
fi

echo "Starting ClickHouse via docker compose..."
"${COMPOSE_CMD[@]}" up -d clickhouse >/dev/null

cleanup() {
  echo "Stopping ClickHouse..."
  "${COMPOSE_CMD[@]}" down --remove-orphans >/dev/null
}
trap cleanup EXIT

echo "Waiting for ClickHouse to become ready..."
until "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
  sleep 1
done

STATUS=0
TMP_OUTPUT="$(mktemp)"

for sql_file in "${ROOT_DIR}"/sql/*.sql; do
  [[ -e "$sql_file" ]] || continue
  base="$(basename "${sql_file}" .sql)"
  expected_file="${ROOT_DIR}/sql/${base}.expected"

  echo "Executing SQL script: ${base}"
  if ! "${COMPOSE_CMD[@]}" exec -T clickhouse clickhouse-client \
      --database proxist \
      --multiquery \
      --send_logs_level=none \
      < "${sql_file}" \
      | tr -d '\r' \
      | awk 'index($0,"\t") > 0' > "${TMP_OUTPUT}"; then
    echo "  ❌ execution failed for ${sql_file}" >&2
    STATUS=1
    continue
  fi

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

rm -f "${TMP_OUTPUT}"
exit "${STATUS}"
