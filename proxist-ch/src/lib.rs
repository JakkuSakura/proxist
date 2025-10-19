//! ClickHouse persistence adapter.

use async_trait::async_trait;
use proxist_core::{metadata::TenantId, watermark::PersistenceBatch};
use proxist_wal::WalSegment;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseConfig {
    pub endpoint: String,
    pub database: String,
    pub credentials_secret: String,
    pub insert_batch_rows: usize,
}

impl Default for ClickhouseConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8123".into(),
            database: "proxist".into(),
            credentials_secret: "clickhouse/default".into(),
            insert_batch_rows: 100_000,
        }
    }
}

#[async_trait]
pub trait ClickhouseSink: Send + Sync {
    async fn flush_segment(
        &self,
        tenant: &TenantId,
        batch: &PersistenceBatch,
        segment: &WalSegment,
    ) -> anyhow::Result<()>;
}
