# Hot SQL Engine

This document defines the in-memory SQL engine used when proxistd runs without
ClickHouse. The engine aims to be a general SQL execution layer aligned with
ClickHouse semantics.

## Goals

- Execute ClickHouse-flavored SQL in hot-only mode without external engines.
- Preserve ClickHouse behavior where practical.
- Provide a predictable, documented surface for operators and tests.

## Supported Statements

- `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`
- `CREATE VIEW`, `DROP VIEW`
- `INSERT` (VALUES and INSERT-SELECT)
- `SELECT` (projection, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, DISTINCT)
- `JOIN` (INNER, LEFT)
- `UNION ALL`
- `ALTER TABLE ... UPDATE ... WHERE ...`
- `ALTER TABLE ... DELETE WHERE ...`

## Types

The engine uses a typed value system and attempts to preserve ClickHouse
semantics for:

- Numeric: Int*, UInt*, Float*, Decimal
- String: String, LowCardinality(String)
- Date/Time: DateTime, DateTime64
- UUID, IPv4
- Complex: Array, Tuple, Map, Nested
- Nullable

## Functions

Function registry targets ClickHouse behavior. Initial coverage includes:

- Time: `toDateTime`, `toDateTime64`, `toUnixTimestamp`, `toUnixTimestamp64Micro`, `now`
- Aggregates: `count`, `sum`, `min`, `max`, `argMax`, `countMerge`, `sumMerge`
- Collections: `arrayJoin`, `map`, `mapKeys`, `tupleElement`
- JSON: `JSONExtractString`
- Misc: `round`, `toYYYYMM`, `generateUUIDv4`

## Semantics Notes

- For features with complex ClickHouse execution (TTL, engine family behaviors,
  mutations), the hot engine emulates query-visible behavior without storage
  reordering or background merges.
- View definitions are stored in the catalog and evaluated at query time.

## Extending

Add new SQL support by extending the parser-to-plan layer and the function
registry, then document the change here.
