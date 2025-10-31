Proxist Table Annotations

Overview
- Proxist routes read queries between DuckDB (hot) and ClickHouse (cold) by analyzing query predicates and a lightweight table mapping provided inline with your DDL.
- You do not need a separate API to register tables; just send your CREATE TABLE DDL through the existing SQL endpoint. Proxist will parse an annotation comment and remember the mapping.

Annotating DDL
- Add a single-line SQL comment anywhere in your DDL with a `proxist:` directive. Keys are comma-separated.

Example
-- proxist: order_col=ts_micros, payload_col=payload_base64, filter_cols=group_key,entity_key, seq_col=seq

Keys
- order_col: Name of the column used for range predicates (e.g., time or monotonic order column).
- payload_col: Name of the column holding the payload bytes (encoded base64 in ClickHouse).
- filter_cols: Comma-separated list of key columns (minimum two). The first is the group/partition key; the second is the per-entity selector.
- seq_col (optional): Sequence column name if applicable.

How It’s Used
- Proxist parses SELECT/WITH queries and detects simple single-table forms that filter by:
  - filter_cols[0] equality
  - filter_cols[1] IN (...) or equality
  - order_col range (Between or >= / > and < / <=)
- If a persisted cutoff watermark is set and the query window is hot-only, rows are loaded from the in-memory store into DuckDB using your DDL and column names, and the query runs locally.
- Otherwise, the query is forwarded to ClickHouse.

Notes
- No table names or column names are hard-coded in Proxist. Your DDL + proxist annotation fully determines mapping.
- For complex queries or joins, Proxist will fall back to ClickHouse.
- We will expand predicate support and seam-aware union as needed while keeping configuration declarative via the DDL annotation.

