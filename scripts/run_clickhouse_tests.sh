#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE=${COMPOSE_FILE:-container-compose.yaml}
COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-proxist-test}

cleanup() {
  echo "[cleanup] docker compose down" >&2
  docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT_NAME" down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

mkdir -p .tmp-meta

cleanup

echo "[run] docker compose up tester" >&2
docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT_NAME" up --build --abort-on-container-exit tester
