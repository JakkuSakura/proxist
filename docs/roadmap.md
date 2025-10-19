# Proxist Roadmap

## 0.1 — Foundations (In Flight)

- Persist WAL segments to disk with checksums, replay, and configurable snapshot cadence; keep ack latency within budget.
- Implement seam-aware query operators (`asof`, `last-by`, rolling windows) over the hot set with deterministic stitching at `T_persisted`.
- Harden the ClickHouse sink with retry/backoff, idempotent inserts, and watermark advancement surfaced via `/status`.
- Deliver authoritative metadata flows (`pxctl status/apply`, shard placement, symbol dictionary) with guardrails.
- Ship baseline observability: metrics, tracing, diagnostics bundle, and automated integration tests that cover ingest → ClickHouse → replay.

## 0.2 — Cloud-Native Operations

- Expand metadata service into a Raft-backed store; implement multi-node leader election and leases.
- Add tenant-aware RBAC, secrets management, and per-tenant resource quotas.
- Deliver observability stack: metrics, tracing, slow-query logging, and cluster health dashboards.
- Automate snapshots, WAL compaction, and ClickHouse lag remediation with control-plane workflows.
- Publish Helm charts/Terraform modules for common Kubernetes + bare-metal deployments.

## 0.3 — Advanced Analytics & Connectors

- SIMD-accelerated vector operators for rolling windows and custom aggregations.
- Programmable UDF/UDAF interface with sandboxing and rate limiting.
- Streaming connectors for Kafka/NATS/Aeron with idempotent resume tokens.
- Materialized subscription feeds with persistence-aware offsets.
- Query planner enhancements for predicate pushdown and multi-shard fan-out.

## 0.4 — Resilience & Federation

- Cross-AZ replication with async WAL shipping and deterministic conflict resolution.
- Disaster recovery tooling: snapshot export/import, cold-start restore automation.
- Cluster federation APIs for multi-region or multi-environment coordination.
- ClickHouse cluster management integration (partition rebalancing, schema evolution, retention policies).

## Wishlist / Exploration

- Alternative persistence layers (Iceberg, object-storage columnar formats).
- Serverless ingestion endpoints for bursty workloads.
- Integrated cost-aware autoscaling.
- In-browser or notebook-native query explorer backed by proxist APIs.
