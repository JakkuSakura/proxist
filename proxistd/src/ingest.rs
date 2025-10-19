use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use anyhow::Result;
use proxist_api::{ClickhouseStatus, ClickhouseTarget, IngestBatchRequest};
use proxist_ch::ClickhouseSink;
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

        ClickhouseStatus {
            enabled: self.clickhouse.is_some(),
            target: self.clickhouse_target.clone(),
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
