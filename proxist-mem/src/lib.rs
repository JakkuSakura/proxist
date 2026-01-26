//! In-memory columnar storage for hot tick data.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use proxist_core::{query::QueryRange, ShardPersistenceTracker};
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
    pub key1: Bytes,
    pub order_micros: i64,
    pub values: Vec<Bytes>,
}

#[derive(Debug, Clone)]
pub struct HotRow {
    pub key1: Bytes,
    pub order_micros: i64,
    pub values: Vec<Bytes>,
}

#[derive(Debug, Clone)]
pub struct HotSymbolSummary {
    pub table: String,
    pub key0: Bytes,
    pub key1: Bytes,
    pub rows: u64,
    pub first_micros: Option<i64>,
    pub last_micros: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct RollingWindowRow {
    pub key1: Bytes,
    pub window_end_micros: i64,
    pub count: u64,
}

#[derive(Debug, Clone)]
struct Row {
    micros: i64,
    values: Vec<Bytes>,
}

#[derive(Default, Debug)]
struct SymbolStore {
    rows: Vec<Row>,
}

#[derive(Default, Debug)]
struct TenantStore {
    symbols: HashMap<Bytes, SymbolStore>,
    total_rows: u64,
}

#[derive(Debug)]
pub struct InMemoryHotColumnStore {
    config: MemConfig,
    inner: RwLock<HashMap<(String, Bytes), TenantStore>>,
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
        table: &str,
        key0: Bytes,
        key1: Bytes,
        order_micros: i64,
        values: Vec<Bytes>,
    ) -> anyhow::Result<()>;

    async fn scan_range(
        &self,
        table: &str,
        key0: &Bytes,
        range: &QueryRange,
        key1_list: &[Bytes],
    ) -> anyhow::Result<Vec<HotRow>>;

    async fn seam_rows_at(
        &self,
        table: &str,
        key0: &Bytes,
        seam: SystemTime,
    ) -> anyhow::Result<Vec<SeamBoundaryRow>>;

    async fn snapshot(&self, shard_tracker: &ShardPersistenceTracker) -> anyhow::Result<Vec<u8>>;

    async fn restore_snapshot(&self, bytes: &[u8]) -> anyhow::Result<()>;

    async fn last_by(
        &self,
        table: &str,
        key0: &Bytes,
        key1_list: &[Bytes],
        at: SystemTime,
    ) -> anyhow::Result<Vec<HotRow>>;

    async fn asof(
        &self,
        table: &str,
        key0: &Bytes,
        key1_list: &[Bytes],
        at: SystemTime,
    ) -> anyhow::Result<Vec<HotRow>>;

    async fn hot_summary(&self) -> anyhow::Result<Vec<HotSymbolSummary>>;

    async fn rolling_window(
        &self,
        table: &str,
        key0: &Bytes,
        key1_list: &[Bytes],
        window_end: SystemTime,
        window: Duration,
        lower_bound: Option<SystemTime>,
    ) -> anyhow::Result<Vec<RollingWindowRow>>;
}

#[async_trait]
impl HotColumnStore for InMemoryHotColumnStore {
    async fn append_row(
        &self,
        table: &str,
        key0: Bytes,
        key1: Bytes,
        order_micros: i64,
        values: Vec<Bytes>,
    ) -> anyhow::Result<()> {
        let mut guard = self.inner.write().await;
        let tenant_store = guard
            .entry((table.to_string(), key0))
            .or_insert_with(TenantStore::default);
        let symbol_store = tenant_store
            .symbols
            .entry(key1)
            .or_insert_with(SymbolStore::default);

        let insert_idx = symbol_store
            .rows
            .partition_point(|row| row.micros <= order_micros);

        symbol_store.rows.insert(
            insert_idx,
            Row {
                micros: order_micros,
                values,
            },
        );

        tenant_store.total_rows += 1;

        if tenant_store.total_rows > self.config.max_rows {
            warn!(
                table = %table,
                current_rows = tenant_store.total_rows,
                max_rows = self.config.max_rows,
                "hot set exceeded max rows; consider increasing capacity"
            );
        }

        Ok(())
    }

    async fn scan_range(
        &self,
        table: &str,
        key0: &Bytes,
        range: &QueryRange,
        key1_list: &[Bytes],
    ) -> anyhow::Result<Vec<HotRow>> {
        let start_micros = system_time_to_micros(range.start);
        let end_micros = system_time_to_micros(range.end);

        let guard = self.inner.read().await;
        let Some(tenant_store) = guard.get(&(table.to_string(), key0.clone())) else {
            return Ok(Vec::new());
        };

        let mut results = Vec::new();

        let symbol_keys: Vec<&Bytes> = if key1_list.is_empty() {
            tenant_store.symbols.keys().collect()
        } else {
            key1_list
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
                        key1: symbol.clone(),
                        order_micros: row.micros,
                        values: row.values.clone(),
                    });
            }
        }

        Ok(results)
    }

    async fn seam_rows_at(
        &self,
        table: &str,
        key0: &Bytes,
        seam: SystemTime,
    ) -> anyhow::Result<Vec<SeamBoundaryRow>> {
        let boundary = system_time_to_micros(seam);

        let guard = self.inner.read().await;
        let Some(tenant_store) = guard.get(&(table.to_string(), key0.clone())) else {
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
                key1: symbol.clone(),
                order_micros: row.micros,
                values: row.values.clone(),
            });
        }

        Ok(seam_rows)
    }

    async fn snapshot(&self, _shard_tracker: &ShardPersistenceTracker) -> anyhow::Result<Vec<u8>> {
        #[derive(Serialize)]
        struct SnapshotRow<'a> {
            key1: &'a Bytes,
            order_micros: i64,
            values: &'a [Bytes],
        }

        #[derive(Serialize)]
        struct SnapshotTenant<'a> {
            table: &'a str,
            key0: &'a Bytes,
            rows: Vec<SnapshotRow<'a>>,
        }

        let guard = self.inner.read().await;
        let mut snapshot = Vec::new();

        for ((table, key0), tenant_store) in guard.iter() {
            let mut rows = Vec::new();
            for (symbol, store) in &tenant_store.symbols {
                for row in &store.rows {
                    rows.push(SnapshotRow {
                        key1: symbol,
                        order_micros: row.micros,
                        values: &row.values,
                    });
                }
            }
            snapshot.push(SnapshotTenant {
                table,
                key0,
                rows,
            });
        }

        let bytes = serde_json::to_vec(&snapshot)?;
        debug!(
            tenants = snapshot.len(),
            bytes = bytes.len(),
            "generated in-memory snapshot"
        );
        Ok(bytes)
    }

    async fn restore_snapshot(&self, bytes: &[u8]) -> anyhow::Result<()> {
        #[derive(Deserialize)]
        struct SnapshotRow {
            key1: Bytes,
            order_micros: i64,
            values: Vec<Bytes>,
        }

        #[derive(Deserialize)]
        struct SnapshotTenant {
            table: String,
            key0: Bytes,
            rows: Vec<SnapshotRow>,
        }

        let tenants: Vec<SnapshotTenant> = serde_json::from_slice(bytes)?;
        let mut guard = self.inner.write().await;
        guard.clear();

        for tenant in tenants {
            let tenant_store = guard
                .entry((tenant.table.clone(), tenant.key0.clone()))
                .or_insert_with(TenantStore::default);
            for row in tenant.rows {
                let symbol_store = tenant_store
                    .symbols
                    .entry(row.key1.clone())
                    .or_insert_with(SymbolStore::default);
                symbol_store.rows.push(Row {
                    micros: row.order_micros,
                    values: row.values,
                });
                tenant_store.total_rows += 1;
            }
        }

        for tenant_store in guard.values_mut() {
            for store in tenant_store.symbols.values_mut() {
                store.rows.sort_by_key(|row| row.micros);
            }
        }

        Ok(())
    }

    async fn last_by(
        &self,
        table: &str,
        key0: &Bytes,
        key1_list: &[Bytes],
        at: SystemTime,
    ) -> anyhow::Result<Vec<HotRow>> {
        let micros = system_time_to_micros(at);
        let guard = self.inner.read().await;
        let Some(tenant_store) = guard.get(&(table.to_string(), key0.clone())) else {
            return Ok(Vec::new());
        };

        let mut results = Vec::new();
        let symbol_keys: Vec<&Bytes> = if key1_list.is_empty() {
            tenant_store.symbols.keys().collect()
        } else {
            key1_list
                .iter()
                .filter(|sym| tenant_store.symbols.contains_key(*sym))
                .collect()
        };

        for symbol in symbol_keys {
            if let Some(store) = tenant_store.symbols.get(symbol) {
                if let Some(row) = find_last_le(&store.rows, micros) {
                    results.push(HotRow {
                        key1: symbol.clone(),
                        order_micros: row.micros,
                        values: row.values.clone(),
                    });
                }
            }
        }

        Ok(results)
    }

    async fn asof(
        &self,
        table: &str,
        key0: &Bytes,
        key1_list: &[Bytes],
        at: SystemTime,
    ) -> anyhow::Result<Vec<HotRow>> {
        // For hot data, ASOF behaves like last_by on the requested timestamp.
        self.last_by(table, key0, key1_list, at).await
    }

    async fn hot_summary(&self) -> anyhow::Result<Vec<HotSymbolSummary>> {
        let guard = self.inner.read().await;
        let mut summaries = Vec::new();

        for ((table, key0), tenant_store) in guard.iter() {
            for (symbol, symbol_store) in tenant_store.symbols.iter() {
                if symbol_store.rows.is_empty() {
                    summaries.push(HotSymbolSummary {
                        table: table.clone(),
                        key0: key0.clone(),
                        key1: symbol.clone(),
                        rows: 0,
                        first_micros: None,
                        last_micros: None,
                    });
                    continue;
                }

                let first = symbol_store.rows.first().map(|row| row.micros);
                let last = symbol_store.rows.last().map(|row| row.micros);

                summaries.push(HotSymbolSummary {
                    table: table.clone(),
                    key0: key0.clone(),
                    key1: symbol.clone(),
                    rows: symbol_store.rows.len() as u64,
                    first_micros: first,
                    last_micros: last,
                });
            }
        }

        Ok(summaries)
    }

    async fn rolling_window(
        &self,
        table: &str,
        key0: &Bytes,
        key1_list: &[Bytes],
        window_end: SystemTime,
        window: Duration,
        lower_bound: Option<SystemTime>,
    ) -> anyhow::Result<Vec<RollingWindowRow>> {
        let window_length = window.as_micros().min(i64::MAX as u128) as i64;
        let end_micros = system_time_to_micros(window_end);
        let start_micros = end_micros.saturating_sub(window_length);
        let lower_bound_micros = lower_bound.map(system_time_to_micros);

        let guard = self.inner.read().await;
        let Some(tenant_store) = guard.get(&(table.to_string(), key0.clone())) else {
            return Ok(Vec::new());
        };

        let mut results = Vec::new();
        let emit_zero = !key1_list.is_empty();
        let symbol_keys: Vec<Bytes> = if key1_list.is_empty() {
            tenant_store.symbols.keys().cloned().collect()
        } else {
            key1_list.iter().cloned().collect()
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
                        key1: symbol,
                        window_end_micros: end_micros,
                        count,
                    };
                    results.push(row);
                }
            } else if emit_zero {
                results.push(RollingWindowRow {
                    key1: symbol,
                    window_end_micros: end_micros,
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
    use bytes::Bytes;

    fn ts(offset_ms: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(offset_ms)
    }

    #[tokio::test]
    async fn last_by_returns_latest_row_before_timestamp() -> anyhow::Result<()> {
        let store = InMemoryHotColumnStore::new(MemConfig::default());
        let table = "ticks";
        let key0 = Bytes::copy_from_slice(b"tenant");
        let key1 = Bytes::copy_from_slice(b"AAPL");
        store
            .append_row(
                table,
                key0.clone(),
                key1.clone(),
                system_time_to_micros(ts(1_000)),
                vec![Bytes::from_static(b"r1")],
            )
            .await?;
        store
            .append_row(
                table,
                key0.clone(),
                key1.clone(),
                system_time_to_micros(ts(2_000)),
                vec![Bytes::from_static(b"r2")],
            )
            .await?;

        let rows = store
            .last_by(table, &key0, &[key1.clone()], ts(1_500))
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Bytes::from_static(b"r1"));

        let rows = store
            .last_by(table, &key0, &[key1.clone()], ts(2_500))
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Bytes::from_static(b"r2"));
        Ok(())
    }

    #[tokio::test]
    async fn asof_aliases_last_by() -> anyhow::Result<()> {
        let store = InMemoryHotColumnStore::new(MemConfig::default());
        let table = "ticks";
        let key0 = Bytes::copy_from_slice(b"tenant");
        let key1 = Bytes::copy_from_slice(b"AAPL");
        store
            .append_row(
                table,
                key0.clone(),
                key1.clone(),
                system_time_to_micros(ts(1_000)),
                vec![Bytes::from_static(b"r1")],
            )
            .await?;
        let rows = store
            .asof(table, &key0, &[key1.clone()], ts(1_500))
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Bytes::from_static(b"r1"));
        Ok(())
    }

    #[tokio::test]
    async fn rolling_window_counts_rows_within_duration() -> anyhow::Result<()> {
        let store = InMemoryHotColumnStore::new(MemConfig::default());
        let table = "ticks";
        let key0 = Bytes::copy_from_slice(b"tenant");
        let key1 = Bytes::copy_from_slice(b"AAPL");
        store
            .append_row(
                table,
                key0.clone(),
                key1.clone(),
                system_time_to_micros(ts(1_000)),
                vec![Bytes::from_static(b"r1")],
            )
            .await?;
        store
            .append_row(
                table,
                key0.clone(),
                key1.clone(),
                system_time_to_micros(ts(2_000)),
                vec![Bytes::from_static(b"r2")],
            )
            .await?;
        store
            .append_row(
                table,
                key0.clone(),
                key1.clone(),
                system_time_to_micros(ts(4_000)),
                vec![Bytes::from_static(b"r3")],
            )
            .await?;

        let window_end = ts(3_500);
        let rows = store
            .rolling_window(
                table,
                &key0,
                &[key1.clone()],
                window_end,
                Duration::from_millis(2_500),
                None,
            )
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].count, 2);
        assert_eq!(rows[0].key1, key1);
        assert_eq!(rows[0].window_end_micros, system_time_to_micros(window_end));
        Ok(())
    }
}
