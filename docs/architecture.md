# Architecture Overview

Proxist is a Rust-native time-series proxy that keeps the hot working set in memory, persists history to ClickHouse, and coordinates deployment through an embedded metadata and cluster management plane. The goal is to combine kdb+-like latency with cloud-native ergonomics.

## Implementation Status

- **Working today**: HTTP control surface (`/status`, `/assignments`, `/health`, `/diagnostics`) plus ClickHouse-compatible SQL ingest, disk or in-memory WAL with replay snapshots, hot column store, ClickHouse persistence with retry/backoff, SQLite-backed metadata + persistence trackers, metadata-driven ingest wiring via DDL annotations, `pxctl` CLI (status/apply/hot-summary), and Docker-based integration test harness.
- **MVP outstanding**: none — core durability, seam-aware queries, metadata workflows, observability, and regression coverage are in place.

## High-Level Components

1. **SQL Ingest (ClickHouse-compatible HTTP)** — accepts DDL and INSERT statements; ingest mapping is driven by `proxist:` annotations.
2. **Hot Set Store (`proxist-mem`)** — columnar, time-sorted storage optimized for sub-millisecond reads; maintains symbol dictionaries and per-symbol indexes.
3. **Write-Ahead Log (`proxist-wal`)** — append-only, checksum-protected log on local NVMe; forms the durability boundary for acknowledgments.
4. **Persistence Sink (`proxist-ch`)** — micro-batches log segments to ClickHouse using ordered inserts and deduplication guards.
5. **Query Engine (`proxist-core`)** — vectorized operators for `asof`, `last-by`, rolling windows, and seam stitching.
6. **Metadata & Control Plane (`proxistd` control loop)** — authoritative state for shards, tenants, symbol dictionaries, watermarks, snapshots, secrets, and ClickHouse lag.
7. **Operator Surface (`pxctl`, `proxist-api`)** — declarative cluster management, observability endpoints, RBAC, and automation hooks.

## Data Flow

### Ingest Path

1. Client submits CREATE TABLE (with `proxist:` annotation) and INSERT statements through the SQL endpoint.
2. `proxistd` derives the ingest mapping from the annotation (order/payload/filter/seq columns).
3. Data is appended into the WAL (disk-backed when `PROXIST_WAL_DIR` is set, in-memory otherwise) and streamed into the hot column store.
4. Acknowledgment returns as soon as the WAL fsync completes (target < 100 µs).
5. The metadata service updates per-table high-watermarks, out-of-order buffers, and ingest metrics.

### Persistence Path

1. WAL segments are cut on time/size thresholds and queued for ClickHouse flush.
2. The persistence worker reorders (if needed) by `(symbol, timestamp)` and attaches a monotonically increasing `version`.
3. Inserts are written to ClickHouse `ReplacingMergeTree` partitions keyed by `toDate(timestamp)`.
4. On success, the worker advances `T_persisted` for the affected shard in the metadata store.
5. Failure triggers retry with idempotent semantics; persistent failures raise operator alerts via the control plane.

### Query Path

1. Query arrives with a time range and optional filters (tenant, symbol set, window config).
2. The planner consults the metadata store for `T_persisted`, shard placement, and cached seam rows.
3. The hot-set executor serves data for `(T_persisted, now]`, operating purely in memory, with support for `range`, `last_by`, and `asof` semantics.
4. ClickHouse handles `(-∞, T_persisted]`; the daemon merges ClickHouse rows with hot results when callers request cold inclusion.
5. A seam stitcher leverages cached boundary rows to bridge gaps when neither tier has a direct match at the persistence boundary.

## Persistence Watermark State Machine

Each shard maintains a lightweight state machine around `T_persisted`:

1. **Capture** — ingest rows from SQL INSERT, append to WAL, mark `T_wal` high-watermark.
2. **FlushReady** — once batch thresholds met, segment transitions to ready state.
3. **Persisting** — ClickHouse insert in flight; metadata records pending batch ID.
4. **Committed** — ClickHouse confirms insert; `T_persisted` advances to the batch end timestamp.
5. **Published** — seam cache updates last-row-per-symbol and window frontiers; observers notified.
6. **Checkpointed** — background snapshot incorporates the committed data and records WAL truncate offsets.

During recovery, the node loads the latest snapshot, replays WAL entries newer than the snapshot offset, rebuilds seam caches, and resumes at **Capture** without losing in-flight batches.

## Metadata & Cluster Management

- **Authoritative Store** — Raft-backed or transactional KV containing tenants, schemas, shard assignments, symbol dictionaries, snapshots, watermarks, WAL manifests, secrets, and RBAC policy.
- **Placement & Scale** — control loop places shards on available nodes based on resource hints (CPU, RAM, NVMe), tracks leases/heartbeats, and orchestrates rebalancing or failover.
- **Lifecycle Automation** — rolling upgrades, snapshot scheduling, WAL compaction, and ClickHouse lag remediation run as state machines driven by metadata events.
- **Operator Interfaces** — `pxctl` issues declarative specs (clusters, tables, tenants) and surfaces seam health (`pxctl hot-summary`). Control plane computes diffs, applies configs, and reports status with structured events.
- **Audit & Versioning** — every change is revisioned; CLI can diff historical specs, roll back, or generate change plans for review.

## Deployment Model

- Single static binary per role (`proxistd`); runs inside containers or bare metal with node/pod affinity and host networking.
- Requires local NVMe (or equivalent low-latency storage) for WAL and snapshots; ClickHouse can be remote or co-located.
- Horizontal scale through sharding by `(tenant, day, symbol range)`; metadata service coordinates migrations with no downtime.
- Supports multi-tenant isolation by combining RBAC, namespace-specific watermarks, and per-tenant WAL streams.

## Observability & Operations

- Metrics endpoints expose ingest lag, ClickHouse flush latency, WAL depth, seam stitch counts, query latency histograms, and control-plane leader health.
- Tracing spans capture end-to-end flow across ingress, hot path, seam, and ClickHouse.
- Diagnostics bundle (`pxctl diagnostics collect`) gathers logs, snapshots, metadata revisions, and ClickHouse status (including last flush/error) for incident response.
- Diagnostics payload now surfaces hot/cold seam summaries and shard persistence trackers so operators can inspect watermarks without scraping metrics.

## Future Extensions

- Alternate persistence backends (object store, Iceberg) by implementing the persistence trait.
- Connector SDK for pushing changes into downstream systems (Fluvio, ksqlDB, etc.).
- Federation across regions via async replication with conflict-free watermarks.
