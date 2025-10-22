//! In-memory columnar storage for hot tick data.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use proxist_core::{metadata::TenantId, query::QueryRange, ShardPersistenceTracker};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemConfig {
    pub max_rows: u64,
    pub symbol_cache_capacity: usize,
}

impl Default for MemConfig {
    fn default() -> Self {
        Self {
            max_rows: 250_000_000,
            symbol_cache_capacity: 32_768,
        }
    }
}

/// Represents a row at the seam between hot and cold tiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeamBoundaryRow {
    pub symbol: String,
    #[serde(with = "proxist_core::time::serde_micros")]
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct HotRow {
    pub symbol: String,
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct HotSymbolSummary {
    pub tenant: TenantId,
    pub symbol: String,
    pub rows: u64,
    pub first_timestamp: Option<SystemTime>,
    pub last_timestamp: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct RollingWindowRow {
    pub symbol: String,
    pub window_end: SystemTime,
    pub count: u64,
}

#[derive(Debug, Clone)]
struct Row {
    timestamp: SystemTime,
    micros: i64,
    payload: Vec<u8>,
}

#[derive(Default, Debug)]
struct SymbolStore {
    rows: Vec<Row>,
}

#[derive(Default, Debug)]
struct TenantStore {
    symbols: HashMap<String, SymbolStore>,
    total_rows: u64,
}

#[derive(Debug)]
pub struct InMemoryHotColumnStore {
    config: MemConfig,
    inner: RwLock<HashMap<TenantId, TenantStore>>,
}

impl InMemoryHotColumnStore {
    pub fn new(config: MemConfig) -> Self {
        Self {
            config,
            inner: RwLock::new(HashMap::new()),
        }
    }
}

fn system_time_to_micros(ts: SystemTime) -> i64 {
    ts.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_micros() as i64
}

#[async_trait]
pub trait HotColumnStore: Send + Sync {
    async fn append_row(
        &self,
        tenant: &TenantId,
        symbol: &str,
        timestamp: SystemTime,
        payload: &[u8],
    ) -> anyhow::Result<()>;

    async fn scan_range(
        &self,
        tenant: &TenantId,
        range: &QueryRange,
        symbols: &[String],
    ) -> anyhow::Result<Vec<HotRow>>;

    async fn seam_rows_at(
        &self,
        tenant: &TenantId,
        seam: SystemTime,
    ) -> anyhow::Result<Vec<SeamBoundaryRow>>;

    async fn snapshot(&self, shard_tracker: &ShardPersistenceTracker) -> anyhow::Result<Vec<u8>>;

    async fn last_by(
        &self,
        tenant: &TenantId,
        symbols: &[String],
        at: SystemTime,
    ) -> anyhow::Result<Vec<HotRow>>;

    async fn asof(
        &self,
        tenant: &TenantId,
        symbols: &[String],
        at: SystemTime,
    ) -> anyhow::Result<Vec<HotRow>>;

    async fn hot_summary(&self) -> anyhow::Result<Vec<HotSymbolSummary>>;

    async fn rolling_window(
        &self,
        tenant: &TenantId,
        symbols: &[String],
        window_end: SystemTime,
        window: Duration,
        lower_bound: Option<SystemTime>,
    ) -> anyhow::Result<Vec<RollingWindowRow>>;
}

#[async_trait]
impl HotColumnStore for InMemoryHotColumnStore {
    async fn append_row(
        &self,
        tenant: &TenantId,
        symbol: &str,
        timestamp: SystemTime,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let micros = system_time_to_micros(timestamp);
        let mut guard = self.inner.write().await;
        let tenant_store = guard
            .entry(tenant.clone())
            .or_insert_with(TenantStore::default);
        let symbol_store = tenant_store
            .symbols
            .entry(symbol.to_string())
            .or_insert_with(SymbolStore::default);

        let insert_idx = symbol_store
            .rows
            .partition_point(|row| row.micros <= micros);

        symbol_store.rows.insert(
            insert_idx,
            Row {
                timestamp,
                micros,
                payload: payload.to_vec(),
            },
        );

        tenant_store.total_rows += 1;

        if tenant_store.total_rows > self.config.max_rows {
            warn!(
                tenant = %tenant,
                current_rows = tenant_store.total_rows,
                max_rows = self.config.max_rows,
                "tenant hot set exceeded max rows; consider increasing capacity"
            );
        }

        Ok(())
    }

    async fn scan_range(
        &self,
        tenant: &TenantId,
        range: &QueryRange,
        symbols: &[String],
    ) -> anyhow::Result<Vec<HotRow>> {
        let start_micros = system_time_to_micros(range.start);
        let end_micros = system_time_to_micros(range.end);

        let guard = self.inner.read().await;
        let Some(tenant_store) = guard.get(tenant) else {
            return Ok(Vec::new());
        };

        let mut results = Vec::new();

        let symbol_keys: Vec<&String> = if symbols.is_empty() {
            tenant_store.symbols.keys().collect()
        } else {
            symbols
                .iter()
                .filter(|sym| tenant_store.symbols.contains_key(*sym))
                .collect()
        };

        for symbol in symbol_keys {
            let store = tenant_store
                .symbols
                .get(symbol)
                .expect("symbol existence verified above");
            let start_idx = store.rows.partition_point(|row| row.micros < start_micros);
            let end_idx = store.rows.partition_point(|row| row.micros < end_micros);

            for row in &store.rows[start_idx..end_idx] {
                results.push(HotRow {
                    symbol: symbol.clone(),
                    timestamp: row.timestamp,
                    payload: row.payload.clone(),
                });
            }
        }

        Ok(results)
    }

    async fn seam_rows_at(
        &self,
        tenant: &TenantId,
        seam: SystemTime,
    ) -> anyhow::Result<Vec<SeamBoundaryRow>> {
        let boundary = system_time_to_micros(seam);

        let guard = self.inner.read().await;
        let Some(tenant_store) = guard.get(tenant) else {
            return Ok(Vec::new());
        };

        let mut seam_rows = Vec::new();

        for (symbol, store) in &tenant_store.symbols {
            let idx = store.rows.partition_point(|row| row.micros <= boundary);
            if idx == 0 {
                continue;
            }
            let row = &store.rows[idx - 1];
            seam_rows.push(SeamBoundaryRow {
                symbol: symbol.clone(),
                timestamp: row.timestamp,
                payload: row.payload.clone(),
            });
        }

        Ok(seam_rows)
    }

    async fn snapshot(&self, _shard_tracker: &ShardPersistenceTracker) -> anyhow::Result<Vec<u8>> {
        #[derive(Serialize)]
        struct SnapshotRow<'a> {
            symbol: &'a str,
            timestamp_micros: i64,
            payload: &'a [u8],
        }

        #[derive(Serialize)]
        struct SnapshotTenant<'a> {
            tenant: &'a str,
            rows: Vec<SnapshotRow<'a>>,
        }

        let guard = self.inner.read().await;
        let mut snapshot = Vec::new();

        for (tenant, tenant_store) in guard.iter() {
            let mut rows = Vec::new();
            for (symbol, store) in &tenant_store.symbols {
                for row in &store.rows {
                    rows.push(SnapshotRow {
                        symbol,
                        timestamp_micros: row.micros,
                        payload: &row.payload,
                    });
                }
            }
            snapshot.push(SnapshotTenant { tenant, rows });
        }

        let bytes = serde_json::to_vec(&snapshot)?;
        debug!(
            tenants = snapshot.len(),
            bytes = bytes.len(),
            "generated in-memory snapshot"
        );
        Ok(bytes)
    }

    async fn last_by(
        &self,
        tenant: &TenantId,
        symbols: &[String],
        at: SystemTime,
    ) -> anyhow::Result<Vec<HotRow>> {
        let micros = system_time_to_micros(at);
        let guard = self.inner.read().await;
        let Some(tenant_store) = guard.get(tenant) else {
            return Ok(Vec::new());
        };

        let mut results = Vec::new();
        let symbol_keys: Vec<&String> = if symbols.is_empty() {
            tenant_store.symbols.keys().collect()
        } else {
            symbols
                .iter()
                .filter(|sym| tenant_store.symbols.contains_key(*sym))
                .collect()
        };

        for symbol in symbol_keys {
            if let Some(store) = tenant_store.symbols.get(symbol) {
                if let Some(row) = find_last_le(&store.rows, micros) {
                    results.push(HotRow {
                        symbol: symbol.clone(),
                        timestamp: row.timestamp,
                        payload: row.payload.clone(),
                    });
                }
            }
        }

        Ok(results)
    }

    async fn asof(
        &self,
        tenant: &TenantId,
        symbols: &[String],
        at: SystemTime,
    ) -> anyhow::Result<Vec<HotRow>> {
        // For hot data, ASOF behaves like last_by on the requested timestamp.
        self.last_by(tenant, symbols, at).await
    }

    async fn hot_summary(&self) -> anyhow::Result<Vec<HotSymbolSummary>> {
        let guard = self.inner.read().await;
        let mut summaries = Vec::new();

        for (tenant, tenant_store) in guard.iter() {
            for (symbol, symbol_store) in tenant_store.symbols.iter() {
                if symbol_store.rows.is_empty() {
                    summaries.push(HotSymbolSummary {
                        tenant: tenant.clone(),
                        symbol: symbol.clone(),
                        rows: 0,
                        first_timestamp: None,
                        last_timestamp: None,
                    });
                    continue;
                }

                let first = symbol_store.rows.first().map(|row| row.timestamp);
                let last = symbol_store.rows.last().map(|row| row.timestamp);

                summaries.push(HotSymbolSummary {
                    tenant: tenant.clone(),
                    symbol: symbol.clone(),
                    rows: symbol_store.rows.len() as u64,
                    first_timestamp: first,
                    last_timestamp: last,
                });
            }
        }

        Ok(summaries)
    }

    async fn rolling_window(
        &self,
        tenant: &TenantId,
        symbols: &[String],
        window_end: SystemTime,
        window: Duration,
        lower_bound: Option<SystemTime>,
    ) -> anyhow::Result<Vec<RollingWindowRow>> {
        let window_length = window.as_micros().min(i64::MAX as u128) as i64;
        let end_micros = system_time_to_micros(window_end);
        let start_micros = end_micros.saturating_sub(window_length);
        let lower_bound_micros = lower_bound.map(system_time_to_micros);

        let guard = self.inner.read().await;
        let Some(tenant_store) = guard.get(tenant) else {
            return Ok(Vec::new());
        };

        let mut results = Vec::new();
        let emit_zero = !symbols.is_empty();
        let symbol_keys: Vec<String> = if symbols.is_empty() {
            tenant_store.symbols.keys().cloned().collect()
        } else {
            symbols.iter().cloned().collect()
        };

        for symbol in symbol_keys {
            if let Some(store) = tenant_store.symbols.get(&symbol) {
                let start_idx = store.rows.partition_point(|row| row.micros < start_micros);
                let end_idx = store.rows.partition_point(|row| row.micros <= end_micros);
                let slice = &store.rows[start_idx..end_idx];
                let count = match lower_bound_micros {
                    Some(bound) => slice.iter().filter(|row| row.micros > bound).count() as u64,
                    None => slice.len() as u64,
                };
                if count > 0 || emit_zero {
                    let row = RollingWindowRow {
                        symbol,
                        window_end,
                        count,
                    };
                    results.push(row);
                }
            } else if emit_zero {
                results.push(RollingWindowRow {
                    symbol,
                    window_end,
                    count: 0,
                });
            }
        }

        Ok(results)
    }
}

fn find_last_le(rows: &[Row], micros: i64) -> Option<&Row> {
    if rows.is_empty() {
        return None;
    }
    let idx = rows.partition_point(|row| row.micros <= micros);
    if idx == 0 {
        None
    } else {
        Some(&rows[idx - 1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(offset_ms: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(offset_ms)
    }

    #[tokio::test]
    async fn last_by_returns_latest_row_before_timestamp() -> anyhow::Result<()> {
        let store = InMemoryHotColumnStore::new(MemConfig::default());
        store
            .append_row(&"tenant".into(), "AAPL", ts(1_000), b"r1")
            .await?;
        store
            .append_row(&"tenant".into(), "AAPL", ts(2_000), b"r2")
            .await?;

        let rows = store
            .last_by(&"tenant".into(), &["AAPL".into()], ts(1_500))
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload, b"r1".to_vec());

        let rows = store
            .last_by(&"tenant".into(), &["AAPL".into()], ts(2_500))
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload, b"r2".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn asof_aliases_last_by() -> anyhow::Result<()> {
        let store = InMemoryHotColumnStore::new(MemConfig::default());
        store
            .append_row(&"tenant".into(), "AAPL", ts(1_000), b"r1")
            .await?;
        let rows = store
            .asof(&"tenant".into(), &["AAPL".into()], ts(1_500))
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload, b"r1".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn rolling_window_counts_rows_within_duration() -> anyhow::Result<()> {
        let store = InMemoryHotColumnStore::new(MemConfig::default());
        let tenant: TenantId = "tenant".into();
        store
            .append_row(&tenant, "AAPL", ts(1_000), b"r1")
            .await?;
        store
            .append_row(&tenant, "AAPL", ts(2_000), b"r2")
            .await?;
        store
            .append_row(&tenant, "AAPL", ts(4_000), b"r3")
            .await?;

        let window_end = ts(3_500);
        let rows = store
            .rolling_window(
                &tenant,
                &["AAPL".into()],
                window_end,
                Duration::from_millis(2_500),
                None,
            )
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].count, 2);
        assert_eq!(rows[0].symbol, "AAPL");
        assert_eq!(rows[0].window_end, window_end);
        Ok(())
    }
}
