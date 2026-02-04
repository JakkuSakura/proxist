use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::{Instant, SystemTime},
};

use crate::clickhouse::{ClickhouseHttpClient, ClickhouseSink, ClickhouseTarget as SinkTarget};
use crate::metadata_sqlite::SqliteMetadataStore;
use crate::scheduler::TableRegistry;
use anyhow::Result;
use bytes::Bytes;
use proxist_core::api::{ClickhouseStatus, ClickhouseTarget};
use proxist_core::ingest::{IngestRow, IngestSegment};
use proxist_core::{
    metadata::{ClusterMetadata, ShardHealth, TenantId},
    MetadataStore, PersistenceBatch, PersistenceTransition, ShardPersistenceTracker,
};
use proxist_mem::{HotColumnStore, HotSymbolSummary};
use proxist_wal::WalManager;
use tokio::sync::Mutex;

pub struct IngestService {
    metadata: SqliteMetadataStore,
    hot_store: Arc<dyn HotColumnStore>,
    clickhouse: Option<Arc<dyn ClickhouseSink>>,
    clickhouse_target: Option<ClickhouseTarget>,
    clickhouse_client: Option<Arc<ClickhouseHttpClient>>,
    wal: Option<Arc<WalManager>>,
    registry: Arc<TableRegistry>,
    last_flush_micros: AtomicI64,
    clickhouse_error: Mutex<Option<String>>,
    persistence_trackers: Mutex<HashMap<String, ShardPersistenceTracker>>,
    symbol_shards: Mutex<HashMap<(TenantId, String), String>>,
    tenant_assignments: Mutex<HashMap<TenantId, String>>,
}

#[derive(Debug, Clone)]
pub struct HotColdSummaryRow {
    pub tenant: TenantId,
    pub symbol: String,
    pub shard_id: Option<String>,
    pub hot_rows: u64,
    pub hot_first: Option<SystemTime>,
    pub hot_last: Option<SystemTime>,
    pub persisted_through: Option<SystemTime>,
    pub wal_high: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct ShardBatchPlan {
    shard_id: String,
    tenant: TenantId,
    batch: PersistenceBatch,
    rows: usize,
}

#[derive(Debug)]
struct ShardBatchAccumulator {
    shard_id: String,
    tenant: TenantId,
    start: SystemTime,
    end: SystemTime,
    rows: usize,
}

impl ShardBatchAccumulator {
    fn new(record: &IngestRow) -> Self {
        Self {
            shard_id: record.shard_id.clone(),
            tenant: bytes_to_str_unchecked(&record.key0).to_string(),
            start: micros_to_system_time(record.order_micros),
            end: micros_to_system_time(record.order_micros),
            rows: 1,
        }
    }

    fn observe(&mut self, record: &IngestRow) {
        debug_assert_eq!(
            self.shard_id, record.shard_id,
            "shard mismatch within batch accumulator"
        );
        let ts = micros_to_system_time(record.order_micros);
        if ts < self.start {
            self.start = ts;
        }
        if ts > self.end {
            self.end = ts;
        }
        self.rows += 1;
    }

    fn into_plan(self) -> ShardBatchPlan {
        let batch_id = format!(
            "{}-{}",
            self.shard_id.replace(':', "_"),
            system_time_to_micros(self.start)
        );
        ShardBatchPlan {
            shard_id: self.shard_id,
            tenant: self.tenant,
            batch: PersistenceBatch::new(batch_id, self.start, self.end),
            rows: self.rows,
        }
    }
}

impl IngestService {
    pub fn new(
        metadata: SqliteMetadataStore,
        hot_store: Arc<dyn HotColumnStore>,
        clickhouse: Option<(
            Arc<dyn ClickhouseSink>,
            SinkTarget,
            Arc<ClickhouseHttpClient>,
        )>,
        wal: Option<Arc<WalManager>>,
        registry: Arc<TableRegistry>,
    ) -> Self {
        let (clickhouse_sink, _sink_target, client, api_target) = match clickhouse {
            Some((sink, target, client)) => {
                let api_target = ClickhouseTarget {
                    endpoint: target.endpoint.clone(),
                    database: target.database.clone(),
                    table: target.table.clone(),
                };
                (Some(sink), Some(target), Some(client), Some(api_target))
            }
            None => (None, None, None, None),
        };
        Self {
            metadata,
            hot_store,
            clickhouse: clickhouse_sink,
            clickhouse_target: api_target,
            clickhouse_client: client,
            wal,
            registry,
            last_flush_micros: AtomicI64::new(-1),
            clickhouse_error: Mutex::new(None),
            persistence_trackers: Mutex::new(HashMap::new()),
            symbol_shards: Mutex::new(HashMap::new()),
            tenant_assignments: Mutex::new(HashMap::new()),
        }
    }

    pub async fn apply_metadata(&self, metadata: &ClusterMetadata) -> anyhow::Result<()> {
        {
            let mut tenant_map = self.tenant_assignments.lock().await;
            tenant_map.clear();
            for assignment in &metadata.assignments {
                tenant_map.insert(assignment.tenant_id.clone(), assignment.shard_id.clone());
            }
        }

        {
            let mut symbol_map = self.symbol_shards.lock().await;
            symbol_map.retain(|(tenant, symbol), _| {
                metadata
                    .symbol_dictionaries
                    .get(tenant)
                    .map_or(false, |symbols| symbols.contains(symbol))
            });
            for assignment in &metadata.assignments {
                if let Some(symbols) = metadata.symbol_dictionaries.get(&assignment.tenant_id) {
                    for symbol in symbols {
                        symbol_map.insert(
                            (assignment.tenant_id.clone(), symbol.clone()),
                            assignment.shard_id.clone(),
                        );
                    }
                }
            }
        }

        {
            let mut trackers = self.persistence_trackers.lock().await;
            trackers.retain(|shard_id, _| {
                metadata
                    .assignments
                    .iter()
                    .any(|assignment| &assignment.shard_id == shard_id)
            });
            for assignment in &metadata.assignments {
                trackers
                    .entry(assignment.shard_id.clone())
                    .or_insert_with(|| ShardPersistenceTracker::new(assignment.shard_id.clone()));
            }
        }

        tracing::info!(
            shards = metadata.assignments.len(),
            tenants = metadata.symbol_dictionaries.len(),
            "applied metadata snapshot to ingest service"
        );
        metrics::gauge!("proxist_assigned_shards", metadata.assignments.len() as f64);
        Ok(())
    }

    async fn resolve_shard(&self, tenant: &TenantId, symbol: &str) -> anyhow::Result<String> {
        let key = (tenant.clone(), symbol.to_string());
        if let Some(shard) = {
            let map = self.symbol_shards.lock().await;
            map.get(&key).cloned()
        } {
            return Ok(shard);
        }

        if let Some(shard) = {
            let map = self.tenant_assignments.lock().await;
            map.get(tenant).cloned()
        } {
            let mut symbol_map = self.symbol_shards.lock().await;
            symbol_map.insert(key, shard.clone());
            return Ok(shard);
        }

        let snapshot = self.metadata.get_cluster_metadata().await?;
        self.apply_metadata(&snapshot).await?;
        if let Some(assignment) = snapshot
            .assignments
            .iter()
            .find(|assign| &assign.tenant_id == tenant)
        {
            let shard_id = assignment.shard_id.clone();
            let mut symbol_map = self.symbol_shards.lock().await;
            symbol_map.insert((tenant.clone(), symbol.to_string()), shard_id.clone());
            return Ok(shard_id);
        }

        Ok(format!("{tenant}::{symbol}"))
    }

    fn plan_shard_batches(segment: &IngestSegment) -> Vec<ShardBatchPlan> {
        let mut accumulators: HashMap<String, ShardBatchAccumulator> = HashMap::new();

        for record in &segment.rows {
            accumulators
                .entry(record.shard_id.clone())
                .and_modify(|acc| acc.observe(record))
                .or_insert_with(|| ShardBatchAccumulator::new(record));
        }

        accumulators
            .into_iter()
            .map(|(_, acc)| acc.into_plan())
            .collect()
    }

    async fn begin_persistence(&self, plans: &[ShardBatchPlan]) -> anyhow::Result<()> {
        if plans.is_empty() {
            return Ok(());
        }
        let mut trackers = self.persistence_trackers.lock().await;
        let mut publish = Vec::with_capacity(plans.len());
        for plan in plans {
            let tracker = trackers
                .entry(plan.shard_id.clone())
                .or_insert_with(|| ShardPersistenceTracker::new(plan.shard_id.clone()));
            tracker.apply(PersistenceTransition::Enqueue {
                batch: plan.batch.clone(),
            })?;
            tracker.apply(PersistenceTransition::BeginPersist {
                batch_id: plan.batch.id.clone(),
            })?;
            publish.push((plan.shard_id.clone(), tracker.clone()));
        }
        drop(trackers);

        for (shard, tracker) in publish {
            self.publish_shard_health(&shard, &tracker).await?;
        }

        Ok(())
    }

    async fn commit_persistence(&self, plans: &[ShardBatchPlan]) -> anyhow::Result<()> {
        if plans.is_empty() {
            return Ok(());
        }
        let mut trackers = self.persistence_trackers.lock().await;
        let mut publish = Vec::with_capacity(plans.len());
        for plan in plans {
            if let Some(tracker) = trackers.get_mut(&plan.shard_id) {
                tracker.apply(PersistenceTransition::ConfirmPersist {
                    batch_id: plan.batch.id.clone(),
                })?;
                tracker.apply(PersistenceTransition::Publish {
                    batch_id: plan.batch.id.clone(),
                })?;
                tracker.apply(PersistenceTransition::Reset)?;
                metrics::counter!(
                    "proxist_persistence_commits_total",
                    1,
                    "shard" => plan.shard_id.clone()
                );
                tracing::debug!(
                    shard = %plan.shard_id,
                    batch = %plan.batch.id,
                    rows = plan.rows,
                    "persistence batch committed"
                );
                publish.push((plan.shard_id.clone(), tracker.clone()));
            }
        }
        drop(trackers);

        for (shard, tracker) in publish {
            self.publish_shard_health(&shard, &tracker).await?;
        }

        Ok(())
    }

    async fn fail_persistence(&self, plans: &[ShardBatchPlan]) -> anyhow::Result<()> {
        if plans.is_empty() {
            return Ok(());
        }
        let mut trackers = self.persistence_trackers.lock().await;
        let mut publish = Vec::with_capacity(plans.len());
        for plan in plans {
            if let Some(tracker) = trackers.get_mut(&plan.shard_id) {
                tracker.apply(PersistenceTransition::Reset)?;
                metrics::counter!(
                    "proxist_persistence_failures_total",
                    1,
                    "shard" => plan.shard_id.clone()
                );
                publish.push((plan.shard_id.clone(), tracker.clone()));
            }
        }
        drop(trackers);

        for (shard, tracker) in publish {
            self.publish_shard_health(&shard, &tracker).await?;
        }

        Ok(())
    }

    async fn publish_shard_health(
        &self,
        shard_id: &str,
        tracker: &ShardPersistenceTracker,
    ) -> anyhow::Result<()> {
        let wal_backlog_bytes = self
            .wal
            .as_ref()
            .map(|wal| wal.backlog_bytes())
            .unwrap_or(0);
        let health = ShardHealth {
            shard_id: shard_id.to_string(),
            is_leader: true,
            wal_backlog_bytes,
            clickhouse_lag_ms: 0,
            watermark: tracker.watermark,
            persistence_state: tracker.state.clone(),
        };
        self.metadata.record_shard_health(health).await?;
        if let Some(persisted) = tracker.watermark.persisted {
            metrics::gauge!(
                "proxist_persisted_watermark_micros",
                system_time_to_micros(persisted) as f64,
                "shard" => shard_id.to_string()
            );
        }
        if let Some(wal) = tracker.watermark.wal_high {
            metrics::gauge!(
                "proxist_wal_high_micros",
                system_time_to_micros(wal) as f64,
                "shard" => shard_id.to_string()
            );
        }
        Ok(())
    }

    pub async fn ingest_records(&self, records: Vec<IngestRow>) -> Result<()> {
        let start = Instant::now();
        let mut rows_written = 0_u64;
        let span = tracing::info_span!("ingest", rows = records.len());
        let _guard = span.enter();
        metrics::counter!("proxist_ingest_requests_total", 1);
        let mut pending_hot: Vec<(String, bytes::Bytes, bytes::Bytes, i64, Vec<bytes::Bytes>)> =
            Vec::with_capacity(records.len());
        let mut records = records;
        for record in &mut records {
            if self.registry.table_config(&record.table).is_none() {
                anyhow::bail!("table schema not registered for {}", record.table);
            }
            let tenant = bytes_to_str_unchecked(&record.key0).to_string();
            let symbol = bytes_to_str_unchecked(&record.key1);
            let shard_id = self.resolve_shard(&tenant, symbol).await?;
            self.metadata.alloc_symbol(&tenant, symbol).await?;
            record.shard_id = shard_id;
            pending_hot.push((
                record.table.clone(),
                record.key0.clone(),
                record.key1.clone(),
                record.order_micros,
                record.values.clone(),
            ));
        }

        if !records.is_empty() {
            let mut wal_append = None;
            if let Some(wal) = &self.wal {
                let append = wal.append_records(&records)?;
                if append.rows_written > 0 {
                    metrics::counter!(
                        "proxist_wal_rows_total",
                        append.rows_written as u64
                    );
                    metrics::counter!(
                        "proxist_wal_bytes_total",
                        append.bytes_written as u64
                    );
                }
                wal_append = Some(append);
            }

            {
                let mut map = self.symbol_shards.lock().await;
                for record in &records {
                    let tenant = bytes_to_str_unchecked(&record.key0).to_string();
                    let symbol = bytes_to_str_unchecked(&record.key1).to_string();
                    map.insert((tenant, symbol), record.shard_id.clone());
                }
            }

            for (table, key0, key1, order_micros, values) in pending_hot.drain(..) {
                self.hot_store
                    .append_row(&table, key0, key1, order_micros, values)
                    .await?;
                rows_written += 1;
            }

            let segment = IngestSegment::new(records);
            self.persist_segment(segment).await?;

            if let (Some(wal), Some(append)) = (self.wal.as_ref(), wal_append) {
                if append.last_seq > 0 && wal.snapshot_due() {
                    let snapshot = self
                        .hot_store
                        .snapshot(&ShardPersistenceTracker::new("wal"))
                        .await?;
                    let _ = wal.maybe_snapshot(snapshot, append.last_seq, append.last_micros);
                }
            }
        }

        if rows_written > 0 {
            let elapsed = start.elapsed();
            metrics::counter!("proxist_ingest_rows_total", rows_written);
            metrics::counter!("proxist_ingest_success_total", 1);
            metrics::histogram!(
                "proxist_ingest_latency_usec",
                elapsed.as_secs_f64() * 1_000_000.0
            );
        }

        Ok(())
    }

    async fn persist_segment(&self, segment: IngestSegment) -> Result<()> {
        let shard_plans = Self::plan_shard_batches(&segment);
        self.begin_persistence(&shard_plans).await?;

        if let Some(sink) = &self.clickhouse {
            if let Err(err) = sink.flush_segment(&segment).await {
                *self.clickhouse_error.lock().await = Some(err.to_string());
                metrics::counter!("proxist_clickhouse_flush_total", 1, "status" => "error");
                metrics::counter!("proxist_ingest_failures_total", 1);
                self.fail_persistence(&shard_plans).await?;
                return Err(err);
            }
            *self.clickhouse_error.lock().await = None;
            metrics::counter!("proxist_clickhouse_flush_total", 1, "status" => "ok");
        }

        if let Some(last) = segment.rows.iter().map(|record| record.order_micros).max() {
            self.last_flush_micros.store(last, Ordering::SeqCst);
        }

        if self.clickhouse.is_none() {
            *self.clickhouse_error.lock().await = None;
        }

        self.commit_persistence(&shard_plans).await?;
        for plan in &shard_plans {
            tracing::debug!(
                shard = %plan.shard_id,
                tenant = %plan.tenant,
                rows = plan.rows,
                start = ?plan.batch.start,
                end = ?plan.batch.end,
                "persisted shard batch"
            );
        }

        Ok(())
    }

    pub async fn persist_replayed(&self, records: &[IngestRow]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let segment = IngestSegment::new(records.to_vec());
        self.persist_segment(segment).await
    }

    pub async fn warm_from_records(&self, records: &[IngestRow]) -> anyhow::Result<()> {
        let mut last_micros: Option<i64> = None;
        {
            let mut map = self.symbol_shards.lock().await;
            for record in records {
                let tenant = bytes_to_str_unchecked(&record.key0).to_string();
                let symbol = bytes_to_str_unchecked(&record.key1).to_string();
                map.insert((tenant, symbol), record.shard_id.clone());
            }
        }

        {
            let mut trackers = self.persistence_trackers.lock().await;
            for record in records {
                let tracker = trackers
                    .entry(record.shard_id.clone())
                    .or_insert_with(|| ShardPersistenceTracker::new(record.shard_id.clone()));
                tracker
                    .watermark
                    .bump_wal(micros_to_system_time(record.order_micros));
            }
        }

        for record in records {
            let tenant = bytes_to_str_unchecked(&record.key0).to_string();
            let symbol = bytes_to_str_unchecked(&record.key1);
            self.metadata.alloc_symbol(&tenant, symbol).await?;
            self.hot_store
                .append_row(
                    &record.table,
                    record.key0.clone(),
                    record.key1.clone(),
                    record.order_micros,
                    record.values.clone(),
                )
                .await?;
            last_micros = Some(record.order_micros);
        }
        if let Some(micros) = last_micros {
            self.last_flush_micros.store(micros, Ordering::SeqCst);
        }
        Ok(())
    }

    pub async fn hydrate_from_hot_store(&self) -> anyhow::Result<()> {
        let summary = self.hot_store.hot_summary().await?;
        if summary.is_empty() {
            return Ok(());
        }

        for entry in summary {
            let tenant = bytes_to_str_unchecked(&entry.key0).to_string();
            let symbol = bytes_to_str_unchecked(&entry.key1);
            self.metadata.alloc_symbol(&tenant, symbol).await?;
            let shard_id = self.resolve_shard(&tenant, symbol).await?;
            {
                let mut symbol_map = self.symbol_shards.lock().await;
                symbol_map.insert((tenant.clone(), symbol.to_string()), shard_id.clone());
            }
            let mut trackers = self.persistence_trackers.lock().await;
            let tracker = trackers
                .entry(shard_id.clone())
                .or_insert_with(|| ShardPersistenceTracker::new(shard_id));
            if let Some(last_micros) = entry.last_micros {
                tracker
                    .watermark
                    .bump_wal(micros_to_system_time(last_micros));
            }
        }

        Ok(())
    }

    pub fn clickhouse_status(&self) -> ClickhouseStatus {
        let micros = self.last_flush_micros.load(Ordering::SeqCst);
        let last_flush = if micros >= 0 {
            Some(micros_to_system_time(micros))
        } else {
            None
        };

        let target = self
            .clickhouse_target
            .as_ref()
            .map(|t| ClickhouseTarget {
                endpoint: t.endpoint.clone(),
                database: t.database.clone(),
                table: t.table.clone(),
            })
            .or_else(|| {
                self.clickhouse_client.as_ref().map(|client| {
                    let t = client.target();
                    ClickhouseTarget {
                        endpoint: t.endpoint,
                        database: t.database,
                        table: t.table,
                    }
                })
            });

        let last_error = self
            .clickhouse_error
            .try_lock()
            .ok()
            .and_then(|guard| guard.clone());

        ClickhouseStatus {
            enabled: self.clickhouse.is_some(),
            target,
            last_flush,
            last_error,
        }
    }

    pub fn last_persisted_timestamp(&self) -> Option<std::time::SystemTime> {
        let micros = self.last_flush_micros.load(Ordering::SeqCst);
        if micros >= 0 {
            Some(micros_to_system_time(micros))
        } else {
            None
        }
    }

    pub fn clickhouse_client(&self) -> Option<Arc<ClickhouseHttpClient>> {
        self.clickhouse_client.as_ref().map(Arc::clone)
    }

    pub async fn tracker_snapshot(&self) -> Vec<ShardPersistenceTracker> {
        let guard = self.persistence_trackers.lock().await;
        guard.values().cloned().collect()
    }

    pub async fn hot_cold_summary(&self) -> anyhow::Result<Vec<HotColdSummaryRow>> {
        let hot_stats = self.hot_store.hot_summary().await?;
        let mut hot_map: HashMap<(TenantId, String), HotSymbolSummary> =
            HashMap::with_capacity(hot_stats.len());
        for entry in hot_stats {
            let tenant = bytes_to_str_unchecked(&entry.key0).to_string();
            let symbol = bytes_to_str_unchecked(&entry.key1).to_string();
            hot_map.insert((tenant, symbol), entry);
        }

        let tracker_map = {
            let guard = self.persistence_trackers.lock().await;
            guard.clone()
        };

        let symbol_map = {
            let guard = self.symbol_shards.lock().await;
            guard.clone()
        };

        if hot_map.is_empty() && symbol_map.is_empty() {
            return Ok(Vec::new());
        }

        let mut keys: Vec<(TenantId, String)> = hot_map.keys().cloned().collect();
        let mut seen: HashSet<(TenantId, String)> = keys.iter().cloned().collect();
        for key in symbol_map.keys() {
            if seen.insert(key.clone()) {
                keys.push(key.clone());
            }
        }

        keys.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut rows = Vec::with_capacity(keys.len());
        for key in keys {
            let shard_id = symbol_map.get(&key).cloned();
            let hot_entry = hot_map.get(&key);
            let (tenant, symbol) = key;
            let tenant_label = tenant.clone();
            let symbol_label = symbol.clone();

            let (hot_rows, hot_first, hot_last) = match hot_entry {
                Some(entry) => (
                    entry.rows,
                    entry.first_micros.map(micros_to_system_time),
                    entry.last_micros.map(micros_to_system_time),
                ),
                None => (0, None, None),
            };

            let (persisted, wal_high) = shard_id
                .as_ref()
                .and_then(|shard| tracker_map.get(shard))
                .map(|tracker| (tracker.watermark.persisted, tracker.watermark.wal_high))
                .unwrap_or((None, None));

            rows.push(HotColdSummaryRow {
                tenant,
                symbol,
                shard_id,
                hot_rows,
                hot_first,
                hot_last,
                persisted_through: persisted,
                wal_high,
            });
            metrics::gauge!(
                "proxist_hot_rows",
                hot_rows as f64,
                "tenant" => tenant_label,
                "symbol" => symbol_label
            );
        }

        Ok(rows)
    }
}

fn system_time_to_micros(ts: std::time::SystemTime) -> i64 {
    ts.duration_since(std::time::UNIX_EPOCH)
        .map(|dur| dur.as_micros() as i64)
        .unwrap_or_else(|err| {
            let dur = err.duration();
            -(dur.as_micros() as i64)
        })
}

fn micros_to_system_time(micros: i64) -> std::time::SystemTime {
    if micros >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_micros(micros as u64)
    } else {
        std::time::UNIX_EPOCH - std::time::Duration::from_micros((-micros) as u64)
    }
}

fn bytes_to_str_unchecked(value: &Bytes) -> &str {
    unsafe { std::str::from_utf8_unchecked(value) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::{ClickhouseConfig, ClickhouseHttpClient};
    use crate::scheduler::{ColumnType, TableConfig, TableRegistry};
    use async_trait::async_trait;
    use bytes::Bytes;
    use proxist_core::ingest::{IngestRow, IngestSegment};
    use proxist_core::{metadata::ShardAssignment, query::QueryRange};
    use proxist_mem::{InMemoryHotColumnStore, MemConfig, SeamBoundaryRow};
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::{NamedTempFile, TempDir};
    use tokio::sync::Mutex;

    fn register_ticks(registry: &TableRegistry) {
        registry.register_table(
            "ticks",
            TableConfig {
                order_col: "ts_micros".to_string(),
                filter_cols: vec!["tenant".to_string(), "symbol".to_string()],
                seq_col: Some("seq".to_string()),
                columns: vec![
                    "tenant".to_string(),
                    "symbol".to_string(),
                    "ts_micros".to_string(),
                    "payload".to_string(),
                    "seq".to_string(),
                ],
                column_types: vec![
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Int64,
                    ColumnType::Text,
                    ColumnType::Int64,
                ],
            },
        );
    }

    #[derive(Default, Clone)]
    struct TestHotStore {
        rows: Arc<Mutex<Vec<IngestRow>>>,
    }

    #[async_trait]
    impl HotColumnStore for TestHotStore {
        async fn append_row(
            &self,
            table: &str,
            key0: Bytes,
            key1: Bytes,
            order_micros: i64,
            values: Vec<Bytes>,
        ) -> anyhow::Result<()> {
            self.rows.lock().await.push(IngestRow {
                table: table.to_string(),
                key0,
                key1,
                order_micros,
                seq: 0,
                shard_id: String::new(),
                values,
            });
            Ok(())
        }

        async fn scan_range(
            &self,
            _table: &str,
            _key0: &Bytes,
            _range: &QueryRange,
            _key1_list: &[Bytes],
        ) -> anyhow::Result<Vec<proxist_mem::HotRow>> {
            Ok(Vec::new())
        }

        async fn seam_rows_at(
            &self,
            _table: &str,
            _key0: &Bytes,
            _seam: std::time::SystemTime,
        ) -> anyhow::Result<Vec<SeamBoundaryRow>> {
            Ok(Vec::new())
        }

        async fn snapshot(
            &self,
            _shard_tracker: &proxist_core::ShardPersistenceTracker,
        ) -> anyhow::Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn restore_snapshot(&self, _bytes: &[u8]) -> anyhow::Result<()> {
            Ok(())
        }

        async fn last_by(
            &self,
            _table: &str,
            _key0: &Bytes,
            _key1_list: &[Bytes],
            _at: std::time::SystemTime,
        ) -> anyhow::Result<Vec<proxist_mem::HotRow>> {
            Ok(Vec::new())
        }

        async fn asof(
            &self,
            table: &str,
            key0: &Bytes,
            key1_list: &[Bytes],
            at: std::time::SystemTime,
        ) -> anyhow::Result<Vec<proxist_mem::HotRow>> {
            self.last_by(table, key0, key1_list, at).await
        }

        async fn hot_summary(&self) -> anyhow::Result<Vec<proxist_mem::HotSymbolSummary>> {
            let rows = self.rows.lock().await;
            let mut grouped: HashMap<
                (String, Bytes, Bytes),
                (u64, Option<i64>, Option<i64>),
            > = HashMap::new();
            for row in rows.iter() {
                let entry = grouped
                    .entry((row.table.clone(), row.key0.clone(), row.key1.clone()))
                    .or_insert((0, None, None));
                entry.0 += 1;
                entry.1 = Some(match entry.1 {
                    Some(existing) if row.order_micros < existing => row.order_micros,
                    Some(existing) => existing,
                    None => row.order_micros,
                });
                entry.2 = Some(match entry.2 {
                    Some(existing) if row.order_micros > existing => row.order_micros,
                    Some(existing) => existing,
                    None => row.order_micros,
                });
            }

            let summaries = grouped
                .into_iter()
                .map(
                    |((table, key0, key1), (count, first, last))| {
                        proxist_mem::HotSymbolSummary {
                            table,
                            key0,
                            key1,
                            rows: count,
                            first_micros: first,
                            last_micros: last,
                        }
                    },
                )
                .collect();

            Ok(summaries)
        }

        async fn rolling_window(
            &self,
            _table: &str,
            _key0: &Bytes,
            _key1_list: &[Bytes],
            _window_end: std::time::SystemTime,
            _window: Duration,
            _lower_bound: Option<std::time::SystemTime>,
        ) -> anyhow::Result<Vec<proxist_mem::RollingWindowRow>> {
            Ok(Vec::new())
        }
    }

    #[derive(Default, Clone)]
    struct RecordingClickhouseSink {
        flushed_counts: Arc<Mutex<Vec<usize>>>,
        flushed_records: Arc<Mutex<Vec<IngestRow>>>,
    }

    #[async_trait]
    impl ClickhouseSink for RecordingClickhouseSink {
        async fn flush_segment(&self, segment: &IngestSegment) -> anyhow::Result<()> {
            self.flushed_counts.lock().await.push(segment.rows.len());
            self.flushed_records
                .lock()
                .await
                .extend(segment.rows.iter().cloned());
            Ok(())
        }
    }

    impl RecordingClickhouseSink {
        async fn counts(&self) -> Vec<usize> {
            self.flushed_counts.lock().await.clone()
        }

        async fn records(&self) -> Vec<IngestRow> {
            self.flushed_records.lock().await.clone()
        }
    }

    #[tokio::test]
    async fn ingest_pipeline_wal_clickhouse_and_memory() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let metadata = SqliteMetadataStore::connect(temp.path().to_str().unwrap()).await?;
        let hot_store = TestHotStore::default();
        let hot_store_arc: Arc<dyn HotColumnStore> = Arc::new(hot_store.clone());
        let mock_sink = RecordingClickhouseSink::default();
        let registry = Arc::new(TableRegistry::new());
        register_ticks(&registry);
        let target = SinkTarget {
            endpoint: "http://localhost:8123".into(),
            database: "proxist".into(),
            table: "ticks".into(),
        };
        let clickhouse_client = Arc::new(ClickhouseHttpClient::new(ClickhouseConfig::default())?);

        let service = IngestService::new(
            metadata.clone(),
            hot_store_arc,
            Some((
                Arc::new(mock_sink.clone()),
                target.clone(),
                Arc::clone(&clickhouse_client),
            )),
            None,
            Arc::clone(&registry),
        );

        let now = std::time::SystemTime::now();
        let records = vec![IngestRow {
            table: "ticks".to_string(),
            key0: Bytes::copy_from_slice(b"alpha"),
            key1: Bytes::copy_from_slice(b"AAPL"),
            order_micros: system_time_to_micros(now),
            seq: 7,
            shard_id: String::new(),
            values: vec![
                Bytes::copy_from_slice(b"alpha"),
                Bytes::copy_from_slice(b"AAPL"),
                Bytes::copy_from_slice(system_time_to_micros(now).to_string().as_bytes()),
                Bytes::copy_from_slice(b"1"),
                Bytes::copy_from_slice(b"7"),
            ],
        }];

        service.ingest_records(records).await?;

        let rows = hot_store.rows.lock().await.clone();
        assert_eq!(rows.len(), 1);
        assert_eq!(bytes_to_str_unchecked(&rows[0].key0), "alpha");
        assert_eq!(bytes_to_str_unchecked(&rows[0].key1), "AAPL");

        let flushed = mock_sink.counts().await;
        assert_eq!(flushed, vec![1]);

        let status = service.clickhouse_status();
        assert!(status.enabled);
        assert_eq!(status.target.unwrap().table, target.table);
        assert!(status.last_flush.is_some());
        assert!(status.last_error.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn ingest_and_query_hot_data_with_persisted_segments() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let db_path = dir.path().join("meta.db");
        let metadata = SqliteMetadataStore::connect(db_path.to_str().unwrap()).await?;
        metadata
            .put_shard_assignment(ShardAssignment {
                shard_id: "alpha-shard".into(),
                tenant_id: "alpha".into(),
                node_id: "node-a".into(),
                symbol_range: ("A".into(), "Z".into()),
            })
            .await?;

        let hot_store = Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let hot_store_trait: Arc<dyn HotColumnStore> = hot_store.clone();

        let recording_sink = RecordingClickhouseSink::default();
        let target = SinkTarget {
            endpoint: "http://localhost:8123".into(),
            database: "proxist".into(),
            table: "ticks".into(),
        };
        let clickhouse_client = Arc::new(ClickhouseHttpClient::new(ClickhouseConfig::default())?);
        let registry = Arc::new(TableRegistry::new());
        register_ticks(&registry);

        let service = IngestService::new(
            metadata.clone(),
            hot_store_trait,
            Some((
                Arc::new(recording_sink.clone()),
                target.clone(),
                Arc::clone(&clickhouse_client),
            )),
            None,
            Arc::clone(&registry),
        );

        let base = std::time::SystemTime::now();
        let tick_range = QueryRange::new(
            base - std::time::Duration::from_secs(5),
            base + std::time::Duration::from_secs(5),
        );

        let records = vec![
            IngestRow {
                table: "ticks".to_string(),
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"AAPL"),
                order_micros: system_time_to_micros(base - std::time::Duration::from_secs(1)),
                seq: 10,
                shard_id: String::new(),
                values: vec![
                    Bytes::copy_from_slice(b"alpha"),
                    Bytes::copy_from_slice(b"AAPL"),
                    Bytes::copy_from_slice(
                        system_time_to_micros(base - std::time::Duration::from_secs(1))
                            .to_string()
                            .as_bytes(),
                    ),
                    Bytes::copy_from_slice(b"1"),
                    Bytes::copy_from_slice(b"10"),
                ],
            },
            IngestRow {
                table: "ticks".to_string(),
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"AAPL"),
                order_micros: system_time_to_micros(base),
                seq: 11,
                shard_id: String::new(),
                values: vec![
                    Bytes::copy_from_slice(b"alpha"),
                    Bytes::copy_from_slice(b"AAPL"),
                    Bytes::copy_from_slice(system_time_to_micros(base).to_string().as_bytes()),
                    Bytes::copy_from_slice(b"2"),
                    Bytes::copy_from_slice(b"11"),
                ],
            },
        ];

        service.ingest_records(records).await?;

        // Hot store should have both rows available via range query.
        let hot_rows = hot_store
            .scan_range(
                "ticks",
                &Bytes::copy_from_slice(b"alpha"),
                &tick_range,
                &[Bytes::copy_from_slice(b"AAPL")],
            )
            .await?;
        assert_eq!(hot_rows.len(), 2);

        // Symbol dictionary should reflect allocation persisted via SQL.
        let metadata_snapshot = metadata.get_cluster_metadata().await?;
        assert_eq!(
            metadata_snapshot
                .symbol_dictionaries
                .get("alpha")
                .cloned()
                .unwrap_or_default(),
            vec!["AAPL".to_string()]
        );

        // ClickHouse sink captured a single flush with both rows.
        let counts = recording_sink.counts().await;
        assert_eq!(counts, vec![2]);
        let records = recording_sink.records().await;
        assert_eq!(records.len(), 2);
        assert!(records
            .iter()
            .all(|rec| bytes_to_str_unchecked(&rec.key1) == "AAPL"));

        // Shard health is recorded with persistence metadata.
        let health = metadata.list_shard_health().await?;
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].shard_id, "alpha-shard");
        assert!(health[0].watermark.persisted.is_some());
        assert_eq!(health[0].watermark.persisted, health[0].watermark.wal_high);

        let status = service.clickhouse_status();
        assert!(status.enabled);
        assert!(status.last_flush.is_some());
        assert!(status.last_error.is_none());
        assert_eq!(status.target.unwrap().table, target.table);

        Ok(())
    }

    #[tokio::test]
    async fn hot_summary_matches_ingested_counts() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let metadata = SqliteMetadataStore::connect(temp.path().to_str().unwrap()).await?;
        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let registry = Arc::new(TableRegistry::new());
        register_ticks(&registry);
        let service = IngestService::new(
            metadata.clone(),
            hot_store,
            None,
            None,
            Arc::clone(&registry),
        );

        let records = vec![
            IngestRow {
                table: "ticks".to_string(),
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"AAPL"),
                order_micros: 1,
                seq: 1,
                shard_id: String::new(),
                values: vec![Bytes::copy_from_slice(b"alpha"), Bytes::copy_from_slice(b"AAPL")],
            },
            IngestRow {
                table: "ticks".to_string(),
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"MSFT"),
                order_micros: 2,
                seq: 2,
                shard_id: String::new(),
                values: vec![Bytes::copy_from_slice(b"alpha"), Bytes::copy_from_slice(b"MSFT")],
            },
            IngestRow {
                table: "ticks".to_string(),
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"AAPL"),
                order_micros: 3,
                seq: 3,
                shard_id: String::new(),
                values: vec![Bytes::copy_from_slice(b"alpha"), Bytes::copy_from_slice(b"AAPL")],
            },
            IngestRow {
                table: "ticks".to_string(),
                key0: Bytes::copy_from_slice(b"beta"),
                key1: Bytes::copy_from_slice(b"GOOG"),
                order_micros: 4,
                seq: 1,
                shard_id: String::new(),
                values: vec![Bytes::copy_from_slice(b"beta"), Bytes::copy_from_slice(b"GOOG")],
            },
        ];

        service.ingest_records(records).await?;
        let summary = service.hot_cold_summary().await?;

        let mut map = std::collections::HashMap::new();
        for row in summary {
            map.insert((row.tenant.clone(), row.symbol.clone()), row.hot_rows);
        }

        assert_eq!(map.get(&("alpha".into(), "AAPL".into())), Some(&2));
        assert_eq!(map.get(&("alpha".into(), "MSFT".into())), Some(&1));
        assert_eq!(map.get(&("beta".into(), "GOOG".into())), Some(&1));

        Ok(())
    }

    #[tokio::test]
    async fn metadata_application_populates_symbol_assignments() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let metadata = SqliteMetadataStore::connect(temp.path().to_str().unwrap()).await?;
        metadata
            .put_shard_assignment(ShardAssignment {
                shard_id: "alpha-shard".into(),
                tenant_id: "alpha".into(),
                node_id: "node-a".into(),
                symbol_range: ("A".into(), "Z".into()),
            })
            .await?;
        metadata.alloc_symbol(&"alpha".into(), "AAPL").await?;

        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let service = IngestService::new(
            metadata.clone(),
            hot_store,
            None,
            None,
            Arc::new(TableRegistry::new()),
        );

        let snapshot = metadata.get_cluster_metadata().await?;
        service.apply_metadata(&snapshot).await?;

        let summary = service.hot_cold_summary().await?;
        assert_eq!(summary.len(), 1);
        let row = &summary[0];
        assert_eq!(row.tenant, "alpha");
        assert_eq!(row.symbol, "AAPL");
        assert_eq!(row.shard_id.as_deref(), Some("alpha-shard"));
        assert_eq!(row.hot_rows, 0);
        Ok(())
    }
}
