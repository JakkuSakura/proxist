# Proxist

Hot-path time-series proxy with ClickHouse persistence and cloud-native control of metadata and clusters.

Proxist targets sub-millisecond analytics on live market ticks held in RAM, while streaming durable history to ClickHouse and coordinating fleet-wide operations through an integrated metadata and cluster management layer.

## Why Proxist

- Sub-ms queries on hot tick data via a vectorized, in-memory engine.
- Durable persistence through a write-ahead log and ClickHouse partitions stitched behind a clear `T_persisted` watermark.
- Seam-aware queries that combine hot RAM with cold history without sacrificing determinism.
- Built-in metadata, topology, and lifecycle management so operators can treat Proxist like a first-class cloud service.
- Extensible crates that separate the core engine, persistence, WAL, and public APIs for future connectors.

## Component Layout

- `proxistd`: daemon that owns ingest, hot-set storage, metadata coordination, and background persistence.
- `pxctl`: operator CLI for deployment, cluster health, and tenant management.
- Core crates (workspace members):
  - `proxist-core`: time-series engine, vector operators, watermark logic.
  - `proxist-mem`: hot-set column store and snapshotting.
  - `proxist-wal`: append-only write-ahead log and recovery tooling.
  - `proxist-ch`: ClickHouse sink, batching, and deduplicated persistence.
  - `proxist-api`: RPC/front-end surface for queries, subscriptions, and management.

## Design Goals

Proxist adopts the proven kdb+ “tick” pattern—log-first durability, memory-resident hot data, columnar historical storage—and adapts it to Rust and ClickHouse. Highlights:

- Performance targets: last-by p50 < 200 µs, asof p50 < 1 ms, rolling windows p50 < 500 µs on a 100–500 GB hot set.
- Ingest discipline: ack on WAL < 100 µs, persist to ClickHouse within 5–50 ms steady state, linear concurrency across cores.
- Consistency: read-after-write on the hot set, seq-based deduplication, deterministic seam stitching guarded by cached boundary rows.
- Cloud-native operations: single static binaries, NVMe-friendly storage, shardable by tenant/day/symbol range, plus node-level observability and RBAC-secured endpoints.
- Metadata & cluster manager: keeps authoritative state for symbol dictionaries, watermarks, shard placement, snapshots, and ClickHouse lag; exposes operators and automation workflows.

For the full design matrix, see `docs/design-goals.md`.

## Documentation

- Architecture overview, state machines, and metadata responsibilities: `docs/architecture.md`.
- Design goals and MVP acceptance criteria: `docs/design-goals.md`.
- Roadmap, CLI ergonomics, and deployment guidance (in progress): `docs/roadmap.md`.

## Current Capabilities

- HTTP daemon (`proxistd`) exposes `/ingest`, `/query`, `/status`, `/assignments`, and `/health`.
- Ingest pipeline appends to an in-memory WAL, writes into the hot column store, and optionally flushes batches to ClickHouse via JSONEachRow.
- Metadata lives in SQLite with shard assignments, symbol dictionaries, and shard health snapshots surfaced through `/status`.
- `pxctl` CLI drives status, ingest, query, and assignment workflows over REST.
- Integration harness (`scripts/run_clickhouse_tests.sh`) launches ClickHouse with Docker Compose and runs `cargo test`.

## MVP Scope

Must-have features before calling Proxist “MVP”:

- Durable ingest: disk-backed WAL, replay + snapshots, <100 µs ack.
- Seam-aware queries: `asof`, `last-by`, rolling windows stitched across `T_persisted`.
- ClickHouse persistence: batched inserts with retries/idempotence and watermark advancement.
- Authoritative metadata: shard placement, watermarks, symbol dictionaries managed via `pxctl`.
- Operability: metrics/tracing, diagnostics bundle, automated ingest→ClickHouse→replay tests.
- Security hooks: TLS/auth/secrets ready for controlled deployments.

## Scale Targets

After MVP lands, focus shifts to:

- Ingest ≥2–5M rows/sec with <100 µs ack and <50 ms ClickHouse lag on a single node.
- Latency targets met for `asof`/`last-by` on 100–500 GB hot sets.
- Large-scale seam validation and replay suites for `T_persisted`.
- Degradation + recovery workflows that guarantee zero data loss after ClickHouse outages.

## Branding Quick Reference

- Pronunciation: **PROX-ist**
- Meaning: proxy that persists hot ticks to cold storage.
- Backronym: **PROX**y for **I**nteractive **S**treaming **T**icks.

## Status

Working alpha focused on wiring the ingest pipeline, metadata cache, and ClickHouse persistence. Expect rapid iteration on durability, seam-aware queries, and operational tooling while closing out the minimal MVP scope.
