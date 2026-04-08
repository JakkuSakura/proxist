#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/container-compose.yaml"

cleanup() {
  docker compose -f "${COMPOSE_FILE}" down >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -f "${COMPOSE_FILE}" up -d clickhouse

echo "Waiting for ClickHouse..."
for i in {1..30}; do
  if curl -fsS "http://127.0.0.1:18123/?query=SELECT%201" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS "http://127.0.0.1:18123/?query=SELECT%201" >/dev/null 2>&1; then
  echo "ClickHouse did not become ready in time."
  exit 1
fi

export CLICKHOUSE_URL="http://127.0.0.1:18123"

cargo test -p px-chd --test clickhouse -- --ignored
