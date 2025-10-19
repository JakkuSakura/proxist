# Proxist Design Goals

## Purpose

Deliver sub-millisecond, kdb+-like analytics on hot market ticks held entirely in memory, while ClickHouse provides durable history and large-scale scans. Proxist couples this with a first-class metadata and cluster management layer so operations feel cloud-native from day one.

## Supported Workloads (MVP)

- Asof joins (e.g., trades → quotes) on time-sorted data.
- `last-by` / `first-by` around a timestamp for order book recon.
- Rolling/tumbling windows (VWAP, EMA, custom aggregations).
- Point-in-time lookups over recent ticks.
- Real-time subscriptions with consistent replay semantics.

## Performance Targets

- Hot-set latency on a single node in RAM:
  - `last-by` p50 < 200 µs.
  - `asof` p50 < 1 ms.
  - Rolling window p50 < 500 µs.
- Ingest: acknowledge on WAL < 100 µs; persist to ClickHouse within 5–50 ms under steady load.
- Concurrency: scale linearly with available cores; support 10–50 concurrent readers without widening p95 tail latency.

## Data Model

- Columnar storage, time-sorted by `(symbol, timestamp)`.
- Unique key `(symbol, timestamp, seq)` for idempotent deduplication.
- Per-day partitions with per-symbol indexes and optional shard keys.

## Consistency and Durability

- Read-after-write guarantees on the hot set.
- Exactly-once semantics within a partition via sequence-based deduplication.
- Persistence watermark `T_persisted` defines the seam: results = `ClickHouse[-∞..T_persisted] + memory(T_persisted..now]`.
- Crash recovery through WAL replay plus periodic in-memory snapshots.

## Metadata & Cluster Management

- Centralized metadata service tracks tenants, shards, symbol dictionaries, watermarks, and snapshot manifests.
- Placement engine assigns shards to nodes and orchestrates rebalancing, rolling upgrades, and failure recovery.
- Health model watches ingest lag, WAL backlog, ClickHouse part counts, and node heartbeats; exposes automation hooks for remediation.
- Configuration layers (profiles, cluster policies, secrets) stored durably and versioned for audit.
- CLI (`pxctl`) and API offer declarative control over topology, schema evolution, and security policies.

## Boundary Handling

- Cache “last row per symbol at `T_persisted`” locally to resolve `asof` queries at the hot/cold seam without a ClickHouse round-trip.
- Windows that span the seam stitch deterministic aggregates by storing frontier statistics (count, sum, sumsq, custom deltas).

## Ingest Pipeline

- Zero-copy append to in-memory columns.
- Append-only WAL on local NVMe with checksums and high-watermark manifests.
- Async micro-batches into ClickHouse, ordered by `(symbol, timestamp)` using `ReplacingMergeTree(version)` or equivalent deduplication.
- Backpressure triggers when lag or part counts exceed thresholds; retries remain idempotent.

## Query Engine

- Vectorized operators over sorted columns for predictable cache behavior.
- Merge-scan `asof`, `last-by/first-by` using monotonic indexes.
- Prefix-sum based window functions with optional SIMD acceleration.
- Historical scans pushed down to ClickHouse; proxy stitches results with hot data.

## Observability

- Metrics: ingest/flush latency, WAL depth, ClickHouse insert rates, p50/p95/p99 per query type, seam-stitch counts.
- Distributed tracing spans cross-boundary queries (memory + ClickHouse).
- Slow-query logs capture input cardinalities and operator plans for tuning.

## Security

- TLS/mTLS for RPCs; token-based or RBAC identities for clients and operators.
- Encrypted WAL and snapshot artifacts.
- Scoped ClickHouse credentials stored via the metadata service.
- Multi-tenant isolation enforced at the shard and API layers.

## Compatibility

- ClickHouse schema: `MergeTree`, `PARTITION BY toDate(ts)`, `ORDER BY (symbol, ts)`, `LowCardinality(String)` for symbol, `ReplacingMergeTree(version)` for deduplication.
- Optional connectors: Kafka/NATS/Aeron for ingest; gRPC/JSON-RPC for queries and streaming.

## Resource Efficiency

- Lock-free or sharded-lock designs, NUMA-aware memory placement, hugepage support, and pinned execution threads.
- Bounded memory via ring buffers and spill-over policies driven by metadata manager.

## Reliability

- Graceful ClickHouse outages: continue ingesting to WAL/memory with bounded backlog; catch up without blocking queries.
- Deterministic replays for auditability; idempotent persistence and schema evolution.
- Snapshot rotation and WAL compaction coordinated via cluster metadata to avoid fleet-wide pauses.

## Extensibility

- Modular crates separate the core engine, WAL, persistence sinks, API surface, and connectors.
- Pluggable persistence backends beyond ClickHouse in future phases.
- Connector SDK for streaming brokers and real-time analytics integrations.

## Non-goals (Initial Phases)

- Not a general SQL engine or full ClickHouse replacement for multi-TB batch analytics.
- No arbitrary joins beyond time-series patterns; focus remains on ordered, symbol-centric workloads.
- No guarantee of sub-ms latency once data is fully cold in ClickHouse; cold scans are delegated entirely to ClickHouse.

## MVP Criteria

- Durable ingest path with on-disk WAL, fast replay, and regular snapshots that keep ack latency under 100 µs.
- Seam-aware query operators (`asof`, `last-by`, rolling windows) over the hot set with deterministic stitching across `T_persisted`.
- ClickHouse flush loop with retry/backoff, idempotent inserts, and watermark advancement surfaced through `/status`.
- Authoritative metadata flows (`pxctl status/apply`), shard placement, and symbol dictionaries stored in the metadata service.
- Baseline observability: metrics, tracing, diagnostics bundle, and automated integration tests covering ingest → ClickHouse → replay.
- Security hooks (TLS/auth/secrets) sufficient for controlled deployments.

## Scale Targets

- Sustained ingest ≥ 2–5 million rows/sec on one node with < 100 µs WAL ack and < 50 ms ClickHouse persistence lag.
- `asof` join and `last-by` meet latency targets on a 100–500 GB RAM-resident hot set.
- Seam-consistent results validated against large replay suites across `T_persisted`.
- Clean degradation and recovery when ClickHouse is unavailable, with no data loss after WAL replay and snapshot restore.
