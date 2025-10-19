# Proxist Roadmap

## 0.1 — Foundations (In Flight)

- Define schemas for WAL records, symbol dictionary persistence, and snapshot manifests.
- Implement minimal hot-set column store with sorted append, seam cache, and WAL replay.
- Build ClickHouse sink with ordered micro-batching and watermark advancement logic.
- Stand up control-plane skeleton: metadata store abstraction, shard registry, heartbeat ingestion, and CLI scaffolding (`pxctl status`, `pxctl apply`).
- Expose basic ingest/query APIs (gRPC + Arrow Flight or protobuf stubs) behind auth tokens.
- Unit and integration tests for WAL recovery, seam stitching, and ClickHouse retry semantics.

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
