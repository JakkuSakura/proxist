use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::{Instant, SystemTime},
};

use anyhow::Result;
use proxist_api::{ClickhouseStatus, IngestBatchRequest};
use proxist_ch::{ClickhouseHttpClient, ClickhouseSink, ClickhouseTarget};
use proxist_core::{
    metadata::{ShardHealth, TenantId},
    MetadataStore, PersistenceBatch, PersistenceTransition, ShardPersistenceTracker,
};
use proxist_mem::{HotColumnStore, HotSymbolSummary};
use proxist_metadata_sqlite::SqliteMetadataStore;
use proxist_wal::{WalRecord, WalSegment, WalWriter};
use serde_bytes::ByteBuf;
use tokio::sync::Mutex;

pub struct IngestService {
    metadata: SqliteMetadataStore,
    wal: Arc<dyn WalWriter>,
    hot_store: Arc<dyn HotColumnStore>,
    clickhouse: Option<Arc<dyn ClickhouseSink>>,
    clickhouse_target: Option<ClickhouseTarget>,
    clickhouse_client: Option<Arc<ClickhouseHttpClient>>,
    last_flush_micros: AtomicI64,
    clickhouse_error: Mutex<Option<String>>,
    persistence_trackers: Mutex<HashMap<String, ShardPersistenceTracker>>,
    symbol_shards: Mutex<HashMap<(TenantId, String), String>>,
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
    wal_first_offset: u64,
    wal_last_offset: u64,
    rows: usize,
}

#[derive(Debug)]
struct ShardBatchAccumulator {
    shard_id: String,
    tenant: TenantId,
    start: SystemTime,
    end: SystemTime,
    wal_first_offset: u64,
    wal_last_offset: u64,
    rows: usize,
}

impl ShardBatchAccumulator {
    fn new(record: &WalRecord, offset: u64) -> Self {
        Self {
            shard_id: record.shard_id.clone(),
            tenant: record.tenant.clone(),
            start: record.timestamp,
            end: record.timestamp,
            wal_first_offset: offset,
            wal_last_offset: offset,
            rows: 1,
        }
    }

    fn observe(&mut self, record: &WalRecord, offset: u64) {
        debug_assert_eq!(
            self.shard_id, record.shard_id,
            "shard mismatch within batch accumulator"
        );
        if record.timestamp < self.start {
            self.start = record.timestamp;
        }
        if record.timestamp > self.end {
            self.end = record.timestamp;
        }
        if offset < self.wal_first_offset {
            self.wal_first_offset = offset;
        }
        if offset > self.wal_last_offset {
            self.wal_last_offset = offset;
        }
        self.rows += 1;
    }

    fn into_plan(self) -> ShardBatchPlan {
        let batch_id = format!(
            "{}-{}",
            self.shard_id.replace(':', "_"),
            self.wal_first_offset
        );
        ShardBatchPlan {
            shard_id: self.shard_id,
            tenant: self.tenant,
            batch: PersistenceBatch::new(batch_id, self.start, self.end),
            wal_first_offset: self.wal_first_offset,
            wal_last_offset: self.wal_last_offset,
            rows: self.rows,
        }
    }
}

impl IngestService {
    pub fn new(
        metadata: SqliteMetadataStore,
        wal: Arc<dyn WalWriter>,
        hot_store: Arc<dyn HotColumnStore>,
        clickhouse: Option<(
            Arc<dyn ClickhouseSink>,
            ClickhouseTarget,
            Arc<ClickhouseHttpClient>,
        )>,
    ) -> Self {
        let (clickhouse_sink, target, client) = match clickhouse {
            Some((sink, target, client)) => (Some(sink), Some(target), Some(client)),
            None => (None, None, None),
        };
        Self {
            metadata,
            wal,
            hot_store,
            clickhouse: clickhouse_sink,
            clickhouse_target: target,
            clickhouse_client: client,
            last_flush_micros: AtomicI64::new(-1),
            clickhouse_error: Mutex::new(None),
            persistence_trackers: Mutex::new(HashMap::new()),
            symbol_shards: Mutex::new(HashMap::new()),
        }
    }

    fn plan_shard_batches(segment: &WalSegment) -> Vec<ShardBatchPlan> {
        let mut accumulators: HashMap<String, ShardBatchAccumulator> = HashMap::new();

        for (idx, record) in segment.records.iter().enumerate() {
            let offset = segment.base_offset.0 + idx as u64;
            accumulators
                .entry(record.shard_id.clone())
                .and_modify(|acc| acc.observe(record, offset))
                .or_insert_with(|| ShardBatchAccumulator::new(record, offset));
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
        let health = ShardHealth {
            shard_id: shard_id.to_string(),
            is_leader: true,
            wal_backlog_bytes: 0,
            clickhouse_lag_ms: 0,
            watermark: tracker.watermark,
            persistence_state: tracker.state.clone(),
        };
        self.metadata.record_shard_health(health).await?;
        Ok(())
    }

    pub async fn ingest(&self, batch: IngestBatchRequest) -> Result<()> {
        let start = Instant::now();
        let mut rows_written = 0_u64;
        let span = tracing::info_span!("ingest", rows = batch.ticks.len());
        let _guard = span.enter();
        metrics::counter!("proxist_ingest_requests_total", 1);
        for tick in &batch.ticks {
            let shard_id = resolve_shard(&self.metadata, &tick.tenant, &tick.symbol).await?;

            self.metadata
                .alloc_symbol(&tick.tenant, &tick.symbol)
                .await?;

            {
                let mut map = self.symbol_shards.lock().await;
                map.insert((tick.tenant.clone(), tick.symbol.clone()), shard_id.clone());
            }

            let record = WalRecord {
                tenant: tick.tenant.clone(),
                shard_id,
                symbol: tick.symbol.clone(),
                timestamp: tick.timestamp,
                payload: payload_to_vec(&tick.payload),
                seq: tick.seq,
            };

            self.wal.append(record).await?;
            self.hot_store
                .append_row(
                    &tick.tenant,
                    &tick.symbol,
                    tick.timestamp,
                    tick.payload.as_ref(),
                )
                .await?;
            rows_written += 1;
        }

        if !batch.ticks.is_empty() {
            let segment = self.wal.flush_segment().await?;
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

            if let Some(last) = segment
                .records
                .iter()
                .map(|record| system_time_to_micros(record.timestamp))
                .max()
            {
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
                    wal_first = plan.wal_first_offset,
                    wal_last = plan.wal_last_offset,
                    start = ?plan.batch.start,
                    end = ?plan.batch.end,
                    "persisted shard batch"
                );
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

    pub async fn warm_from_records(&self, records: &[WalRecord]) -> anyhow::Result<()> {
        let mut last_ts: Option<SystemTime> = None;
        for record in records {
            self.metadata
                .alloc_symbol(&record.tenant, &record.symbol)
                .await?;
            self.hot_store
                .append_row(
                    &record.tenant,
                    &record.symbol,
                    record.timestamp,
                    &record.payload,
                )
                .await?;
            last_ts = Some(record.timestamp);
        }
        if let Some(ts) = last_ts {
            self.last_flush_micros
                .store(system_time_to_micros(ts), Ordering::SeqCst);
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
            .map(|t| proxist_api::ClickhouseTarget {
                endpoint: t.endpoint.clone(),
                database: t.database.clone(),
                table: t.table.clone(),
            })
            .or_else(|| {
                self.clickhouse_client.as_ref().map(|client| {
                    let t = client.target();
                    proxist_api::ClickhouseTarget {
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

    pub fn clickhouse_table(&self) -> Option<String> {
        self.clickhouse_target.as_ref().map(|t| t.table.clone())
    }

    pub async fn hot_cold_summary(&self) -> anyhow::Result<Vec<HotColdSummaryRow>> {
        let hot_stats = self.hot_store.hot_summary().await?;
        let mut hot_map: HashMap<(TenantId, String), HotSymbolSummary> =
            HashMap::with_capacity(hot_stats.len());
        for entry in hot_stats {
            hot_map.insert((entry.tenant.clone(), entry.symbol.clone()), entry);
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
            let tenant = key.0;
            let symbol = key.1;

            let (hot_rows, hot_first, hot_last) = match hot_entry {
                Some(entry) => (entry.rows, entry.first_timestamp, entry.last_timestamp),
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
        }

        Ok(rows)
    }
}

async fn resolve_shard(
    metadata: &SqliteMetadataStore,
    tenant: &TenantId,
    symbol: &str,
) -> Result<String> {
    let snapshot = metadata.get_cluster_metadata().await?;
    if let Some(assignment) = snapshot
        .assignments
        .iter()
        .find(|assign| &assign.tenant_id == tenant)
    {
        return Ok(assignment.shard_id.clone());
    }

    Ok(format!("{tenant}::{symbol}"))
}

fn payload_to_vec(buf: &ByteBuf) -> Vec<u8> {
    buf.as_ref().to_vec()
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use proxist_api::IngestTick;
    use proxist_ch::{ClickhouseConfig, ClickhouseHttpClient};
    use proxist_core::{metadata::ShardAssignment, query::QueryRange};
    use proxist_mem::{InMemoryHotColumnStore, MemConfig, SeamBoundaryRow};
    use proxist_wal::{InMemoryWal, WalRecord, WalSegment};
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::{NamedTempFile, TempDir};
    use tokio::sync::Mutex;

    #[derive(Default, Clone)]
    struct TestHotStore {
        rows: Arc<Mutex<Vec<(TenantId, String, std::time::SystemTime, Vec<u8>)>>>,
    }

    #[async_trait]
    impl HotColumnStore for TestHotStore {
        async fn append_row(
            &self,
            tenant: &TenantId,
            symbol: &str,
            timestamp: std::time::SystemTime,
            payload: &[u8],
        ) -> anyhow::Result<()> {
            self.rows.lock().await.push((
                tenant.clone(),
                symbol.to_string(),
                timestamp,
                payload.to_vec(),
            ));
            Ok(())
        }

        async fn scan_range(
            &self,
            _tenant: &TenantId,
            _range: &QueryRange,
            _symbols: &[String],
        ) -> anyhow::Result<Vec<proxist_mem::HotRow>> {
            Ok(Vec::new())
        }

        async fn seam_rows_at(
            &self,
            _tenant: &TenantId,
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

        async fn last_by(
            &self,
            tenant: &TenantId,
            symbols: &[String],
            at: std::time::SystemTime,
        ) -> anyhow::Result<Vec<proxist_mem::HotRow>> {
            let rows = self.rows.lock().await;
            let mut result = Vec::new();
            for symbol in symbols {
                let mut best: Option<(std::time::SystemTime, Vec<u8>)> = None;
                for (row_tenant, row_symbol, ts, payload) in rows.iter() {
                    if row_tenant == tenant && row_symbol == symbol && ts <= &at {
                        if best.as_ref().map_or(true, |(best_ts, _)| ts > best_ts) {
                            best = Some((*ts, payload.clone()));
                        }
                    }
                }
                if let Some((ts, payload)) = best {
                    result.push(proxist_mem::HotRow {
                        symbol: symbol.clone(),
                        timestamp: ts,
                        payload,
                    });
                }
            }
            Ok(result)
        }

        async fn asof(
            &self,
            tenant: &TenantId,
            symbols: &[String],
            at: std::time::SystemTime,
        ) -> anyhow::Result<Vec<proxist_mem::HotRow>> {
            self.last_by(tenant, symbols, at).await
        }

        async fn hot_summary(&self) -> anyhow::Result<Vec<proxist_mem::HotSymbolSummary>> {
            let rows = self.rows.lock().await;
            let mut grouped: HashMap<
                (TenantId, String),
                (
                    u64,
                    Option<std::time::SystemTime>,
                    Option<std::time::SystemTime>,
                ),
            > = HashMap::new();
            for (tenant, symbol, ts, _) in rows.iter() {
                let entry = grouped
                    .entry((tenant.clone(), symbol.clone()))
                    .or_insert((0, None, None));
                entry.0 += 1;
                entry.1 = Some(match entry.1 {
                    Some(existing) if *ts < existing => *ts,
                    Some(existing) => existing,
                    None => *ts,
                });
                entry.2 = Some(match entry.2 {
                    Some(existing) if *ts > existing => *ts,
                    Some(existing) => existing,
                    None => *ts,
                });
            }

            let summaries = grouped
                .into_iter()
                .map(
                    |((tenant, symbol), (count, first, last))| proxist_mem::HotSymbolSummary {
                        tenant,
                        symbol,
                        rows: count,
                        first_timestamp: first,
                        last_timestamp: last,
                    },
                )
                .collect();

            Ok(summaries)
        }
    }

    #[derive(Default, Clone)]
    struct RecordingClickhouseSink {
        flushed_counts: Arc<Mutex<Vec<usize>>>,
        flushed_records: Arc<Mutex<Vec<WalRecord>>>,
    }

    #[async_trait]
    impl ClickhouseSink for RecordingClickhouseSink {
        async fn flush_segment(&self, segment: &WalSegment) -> anyhow::Result<()> {
            self.flushed_counts.lock().await.push(segment.records.len());
            self.flushed_records
                .lock()
                .await
                .extend(segment.records.iter().cloned());
            Ok(())
        }

        fn target(&self) -> ClickhouseTarget {
            ClickhouseTarget {
                endpoint: "http://localhost:8123".into(),
                database: "proxist".into(),
                table: "ticks".into(),
            }
        }
    }

    impl RecordingClickhouseSink {
        async fn counts(&self) -> Vec<usize> {
            self.flushed_counts.lock().await.clone()
        }

        async fn records(&self) -> Vec<WalRecord> {
            self.flushed_records.lock().await.clone()
        }
    }

    #[tokio::test]
    async fn ingest_pipeline_wal_clickhouse_and_memory() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let metadata = SqliteMetadataStore::connect(temp.path().to_str().unwrap()).await?;
        let wal: Arc<dyn WalWriter> = Arc::new(InMemoryWal::new());
        let hot_store = TestHotStore::default();
        let hot_store_arc: Arc<dyn HotColumnStore> = Arc::new(hot_store.clone());
        let mock_sink = RecordingClickhouseSink::default();
        let target = mock_sink.target();
        let clickhouse_client = Arc::new(ClickhouseHttpClient::new(ClickhouseConfig::default())?);

        let service = IngestService::new(
            metadata.clone(),
            wal,
            hot_store_arc,
            Some((
                Arc::new(mock_sink.clone()),
                target.clone(),
                Arc::clone(&clickhouse_client),
            )),
        );

        let now = std::time::SystemTime::now();
        let batch = IngestBatchRequest {
            ticks: vec![IngestTick {
                tenant: "alpha".into(),
                symbol: "AAPL".into(),
                timestamp: now,
                payload: ByteBuf::from(vec![1, 2, 3]),
                seq: 7,
            }],
        };

        service.ingest(batch).await?;

        let rows = hot_store.rows.lock().await.clone();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "alpha");
        assert_eq!(rows[0].1, "AAPL");

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

        let wal: Arc<dyn WalWriter> = Arc::new(InMemoryWal::new());
        let hot_store = Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let hot_store_trait: Arc<dyn HotColumnStore> = hot_store.clone();

        let recording_sink = RecordingClickhouseSink::default();
        let target = recording_sink.target();
        let clickhouse_client = Arc::new(ClickhouseHttpClient::new(ClickhouseConfig::default())?);

        let service = IngestService::new(
            metadata.clone(),
            wal,
            hot_store_trait,
            Some((
                Arc::new(recording_sink.clone()),
                target.clone(),
                Arc::clone(&clickhouse_client),
            )),
        );

        let base = std::time::SystemTime::now();
        let tick_range = QueryRange::new(
            base - std::time::Duration::from_secs(5),
            base + std::time::Duration::from_secs(5),
        );

        let batch = IngestBatchRequest {
            ticks: vec![
                IngestTick {
                    tenant: "alpha".into(),
                    symbol: "AAPL".into(),
                    timestamp: base - std::time::Duration::from_secs(1),
                    payload: ByteBuf::from(vec![1, 2, 3]),
                    seq: 10,
                },
                IngestTick {
                    tenant: "alpha".into(),
                    symbol: "AAPL".into(),
                    timestamp: base,
                    payload: ByteBuf::from(vec![4, 5, 6]),
                    seq: 11,
                },
            ],
        };

        service.ingest(batch).await?;

        // Hot store should have both rows available via range query.
        let hot_rows = hot_store
            .scan_range(&"alpha".into(), &tick_range, &["AAPL".into()])
            .await?;
        assert_eq!(hot_rows.len(), 2);
        assert_eq!(hot_rows[0].payload, vec![1, 2, 3]);
        assert_eq!(hot_rows[1].payload, vec![4, 5, 6]);

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
        assert!(records.iter().all(|rec| rec.symbol == "AAPL"));

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
        let wal: Arc<dyn WalWriter> = Arc::new(InMemoryWal::new());
        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let service = IngestService::new(metadata.clone(), wal, hot_store, None);

        let ticks = vec![
            IngestTick {
                tenant: "alpha".into(),
                symbol: "AAPL".into(),
                timestamp: SystemTime::UNIX_EPOCH + Duration::from_micros(1),
                payload: ByteBuf::from(vec![]),
                seq: 1,
            },
            IngestTick {
                tenant: "alpha".into(),
                symbol: "MSFT".into(),
                timestamp: SystemTime::UNIX_EPOCH + Duration::from_micros(2),
                payload: ByteBuf::from(vec![]),
                seq: 2,
            },
            IngestTick {
                tenant: "alpha".into(),
                symbol: "AAPL".into(),
                timestamp: SystemTime::UNIX_EPOCH + Duration::from_micros(3),
                payload: ByteBuf::from(vec![]),
                seq: 3,
            },
            IngestTick {
                tenant: "beta".into(),
                symbol: "GOOG".into(),
                timestamp: SystemTime::UNIX_EPOCH + Duration::from_micros(4),
                payload: ByteBuf::from(vec![]),
                seq: 1,
            },
        ];

        service.ingest(IngestBatchRequest { ticks }).await?;
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
}
