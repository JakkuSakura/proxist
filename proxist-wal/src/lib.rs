//! Write-ahead log semantics for Proxist ingest durability.

use std::path::PathBuf;
use std::time::SystemTime;

use async_trait::async_trait;
use proxist_core::{metadata::TenantId, watermark::PersistenceBatch};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalOffset(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub tenant: TenantId,
    pub shard_id: String,
    pub symbol: String,
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSegment {
    pub base_offset: WalOffset,
    pub records: Vec<WalRecord>,
    pub written_at: SystemTime,
}

impl WalSegment {
    pub fn end_offset(&self) -> WalOffset {
        WalOffset(self.base_offset.0 + self.records.len() as u64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    pub directory: PathBuf,
    pub segment_bytes: usize,
    pub fsync_interval: std::time::Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./wal"),
            segment_bytes: 8 * 1024 * 1024,
            fsync_interval: std::time::Duration::from_millis(5),
        }
    }
}

#[async_trait]
pub trait WalWriter: Send + Sync {
    async fn append(&self, record: WalRecord) -> anyhow::Result<WalOffset>;
    async fn flush_segment(&self) -> anyhow::Result<PersistenceBatch>;
}

#[async_trait]
pub trait WalReader: Send + Sync {
    async fn replay(&self, from: WalOffset) -> anyhow::Result<Vec<WalRecord>>;
    async fn load_segments(&self) -> anyhow::Result<Vec<WalSegment>>;
}
