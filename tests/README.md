# Test Coverage Roadmap

## Recently Added

- pxctl TSV parser tests cover both neutral and legacy summary headers.
- Ingest service unit test validates hot summary counts per symbol.
- Proxist daemon WAL bootstrap test verifies disk replay restores the hot column store.
- Query executor seam test validates `last_by` with `include_cold` returns persisted rows via seam caches when ClickHouse is unavailable.

This file sketches the next wave of coverage we want to add. The goal is to move
from the current “happy-path e2e smoke” to a matrix that exercises the new
system summary semantics, pxctl parsing, and legacy compatibility.

## End-to-End SQL Fixtures

- **Legacy Summary Alias**  
  Run a script that selects from `system.proxist_hot_summary` and verifies TSV
  output still renders the legacy headers. Guards against regressions in the
  compatibility layer.

- **Neutral Summary Alias**  
  Extend the existing fixture with a second query that hits `system.proxist_ingest_summary`
  (or the proxist schema alias) alongside unrelated DDL/DML. Confirms the handler
  recognizes the neutral naming without breaking normal proxying.

- **Mixed-Format Batch**  
  A multi-statement POST that includes INSERTs, a proxied SELECT, and a summary
  query with `FORMAT JSONEachRow`. Validates that format detection and batching
  logic continue to cooperate with the summary intercepts.

## pxctl CLI Coverage

- **Header Matrix Parsing**  
  Table-driven unit tests that feed the `parse_ingest_summary_tsv` helper both
  neutral and legacy headers (including reordered column lists) and assert the
  resulting JSON matches expectations.

- **Render Table Smoke**  
  Snapshot-style test that renders a small set of neutral rows and checks the
  column naming/justification. Ensures future column renames don’t silently
  break the console UX.

## Daemon-Level Unit Tests

- **Summary Detection**  
  Expand the existing `match_system_summary_query` checks to cover quoted
  identifiers, whitespace, and mixed-case keywords.

- **TSV Rendering**  
  Validate `render_system_summary` directly, comparing headers and values for
  both summary views (legacy + neutral). Keeps column alias changes explicit.

## Future Ideas

- **ClickHouse Failover**  
  Simulated failure during summary queries to confirm we fall back to the raw
  ClickHouse client cleanly.

- **pxctl JSON Output Golden**  
  CLI integration test that shells out `pxctl hot-summary --json` against a
  mocked HTTP server to ensure response parsing and serialization stay stable.

These entries define intent only—no test code yet. Once prioritized, each bullet
should become either a new SQL script, a Rust test module, or an integration
target under `tests/`.
