# Hot / Cold Seam Semantics

Proxist keeps every symbol’s recent history in RAM (“hot”) while streaming
durable history into ClickHouse (“cold”). Queries cross that boundary
deterministically so callers always see a single, ordered timeline.

## Ingest Flow

1. **Append** – writes are appended to the WAL and inserted into the in-memory
   column store.
2. **Persist** – background flush batches ship those rows to ClickHouse. Once
   ClickHouse confirms the batch, the shard’s persistence tracker advances its
   persisted watermark.
3. **Seam** – any timestamp greater than `watermark.persisted` is considered
   hot. Everything at or below that timestamp is cold.

Hot rows remain queryable immediately (read-after-write) while ClickHouse
provides the durable history.

## When Is Data Considered Hot?

- Incoming writes are hot as soon as they land in the in-memory store.
- Data stays hot until the corresponding batch is confirmed durable and the
  `persisted` watermark moves past the row’s timestamp.
- After the watermark advances, the row becomes cold; future hot scans will not
  re-read it.

## Query Strategy

Every query is evaluated in two stages:

1. **Hot scan** – the daemon reads directly from the in-memory store within the
   requested time bounds. This covers `(watermark.persisted, now]`.
2. **Cold query** – if the caller requested cold inclusion, proxist rewrites and
   forwards a ClickHouse query covering `(-∞, watermark.persisted]`.

The two result sets are merged by timestamp before returning to the caller.

## Aggregations Across the Seam

Aggregations follow the same split:

- Hot rows are aggregated in the proxy. For example, `SUM(volume)` is computed
  over the in-memory data.
- Cold history is aggregated inside ClickHouse.
- The proxy combines the two aggregations (e.g., adding partial sums, merging
  counts) before responding.

Because the seam is deterministic, aggregations remain idempotent: a row is
ever only counted once (hot or cold). When the watermark advances, the hot
partial is recomputed without the newly cold rows.
