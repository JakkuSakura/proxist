use std::sync::Arc;

use anyhow::Result;
use proxist_api::IngestBatchRequest;
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
}

impl IngestService {
    pub fn new(
        metadata: SqliteMetadataStore,
        wal: Arc<dyn WalWriter>,
        hot_store: Arc<dyn HotColumnStore>,
    ) -> Self {
        Self {
            metadata,
            wal,
            hot_store,
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
            let batch = self.wal.flush_segment().await?;
            tracing::debug!(id = %batch.id, "flushed WAL segment");
        }

        Ok(())
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
