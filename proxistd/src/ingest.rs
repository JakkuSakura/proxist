use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use anyhow::Result;
use proxist_api::{ClickhouseStatus, IngestBatchRequest};
use proxist_ch::{ClickhouseSink, ClickhouseTarget};
use proxist_core::metadata::TenantId;
use proxist_core::MetadataStore;
use proxist_mem::HotColumnStore;
use proxist_metadata_sqlite::SqliteMetadataStore;
use proxist_wal::{WalRecord, WalWriter};
use serde_bytes::ByteBuf;

pub struct IngestService {
    metadata: SqliteMetadataStore,
    wal: Arc<dyn WalWriter>,
    hot_store: Arc<dyn HotColumnStore>,
    clickhouse: Option<Arc<dyn ClickhouseSink>>,
    clickhouse_target: Option<ClickhouseTarget>,
    last_flush_micros: AtomicI64,
}

impl IngestService {
    pub fn new(
        metadata: SqliteMetadataStore,
        wal: Arc<dyn WalWriter>,
        hot_store: Arc<dyn HotColumnStore>,
        clickhouse: Option<(Arc<dyn ClickhouseSink>, ClickhouseTarget)>,
    ) -> Self {
        let (clickhouse_sink, target) = match clickhouse {
            Some((sink, target)) => (Some(sink), Some(target)),
            None => (None, None),
        };
        Self {
            metadata,
            wal,
            hot_store,
            clickhouse: clickhouse_sink,
            clickhouse_target: target,
            last_flush_micros: AtomicI64::new(-1),
        }
    }

    pub async fn ingest(&self, batch: IngestBatchRequest) -> Result<()> {
        for tick in &batch.ticks {
            let shard_id = resolve_shard(&self.metadata, &tick.tenant, &tick.symbol).await?;

            self.metadata
                .alloc_symbol(&tick.tenant, &tick.symbol)
                .await?;

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
        }

        if !batch.ticks.is_empty() {
            let segment = self.wal.flush_segment().await?;
            if let Some(sink) = &self.clickhouse {
                sink.flush_segment(&segment).await?;
                if let Some(last) = segment
                    .records
                    .last()
                    .map(|record| system_time_to_micros(record.timestamp))
                {
                    self.last_flush_micros.store(last, Ordering::SeqCst);
                }
            }
            let batch = segment.to_persistence_batch()?;
            tracing::debug!(id = %batch.id, rows = segment.records.len(), "flushed WAL segment");
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
            });

        ClickhouseStatus {
            enabled: self.clickhouse.is_some(),
            target,
            last_flush,
        }
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
    use proxist_core::query::QueryRange;
    use proxist_mem::SeamBoundaryRow;
    use proxist_wal::InMemoryWal;
    use tempfile::NamedTempFile;
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
        ) -> anyhow::Result<Vec<Vec<u8>>> {
            Ok(Vec::new())
        }

        async fn seam_rows(
            &self,
            _tenant: &TenantId,
            _range: &QueryRange,
        ) -> anyhow::Result<Vec<SeamBoundaryRow>> {
            Ok(Vec::new())
        }

        async fn snapshot(
            &self,
            _shard_tracker: &proxist_core::ShardPersistenceTracker,
        ) -> anyhow::Result<Vec<u8>> {
            Ok(Vec::new())
        }
    }

    #[derive(Default, Clone)]
    struct MockClickhouseSink {
        flushed: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl ClickhouseSink for MockClickhouseSink {
        async fn flush_segment(&self, segment: &proxist_wal::WalSegment) -> anyhow::Result<()> {
            self.flushed.lock().await.push(segment.records.len());
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

    #[tokio::test]
    async fn ingest_pipeline_wal_clickhouse_and_memory() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let metadata = SqliteMetadataStore::connect(temp.path().to_str().unwrap()).await?;
        let wal: Arc<dyn WalWriter> = Arc::new(InMemoryWal::new());
        let hot_store = TestHotStore::default();
        let hot_store_arc: Arc<dyn HotColumnStore> = Arc::new(hot_store.clone());
        let mock_sink = MockClickhouseSink::default();
        let target = mock_sink.target();

        let service = IngestService::new(
            metadata.clone(),
            wal,
            hot_store_arc,
            Some((Arc::new(mock_sink.clone()), target.clone())),
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

        let flushed = mock_sink.flushed.lock().await.clone();
        assert_eq!(flushed, vec![1]);

        let status = service.clickhouse_status();
        assert!(status.enabled);
        assert_eq!(status.target.unwrap().table, target.table);
        assert!(status.last_flush.is_some());

        Ok(())
    }
}
