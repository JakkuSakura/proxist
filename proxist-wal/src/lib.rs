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

#[derive(Default, Debug)]
struct InMemoryWalState {
    records: Vec<WalRecord>,
    last_flush_index: usize,
}

#[derive(Debug)]
pub struct InMemoryWal {
    state: tokio::sync::Mutex<InMemoryWalState>,
    next_offset: std::sync::atomic::AtomicU64,
}

impl InMemoryWal {
    pub fn new() -> Self {
        Self {
            state: tokio::sync::Mutex::new(InMemoryWalState::default()),
            next_offset: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl WalWriter for InMemoryWal {
    async fn append(&self, record: WalRecord) -> anyhow::Result<WalOffset> {
        let mut state = self.state.lock().await;
        let offset = WalOffset(
            self.next_offset
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );
        state.records.push(record);
        Ok(offset)
    }

    async fn flush_segment(&self) -> anyhow::Result<PersistenceBatch> {
        let mut state = self.state.lock().await;
        if state.records.len() == state.last_flush_index {
            anyhow::bail!("no WAL records to flush");
        }

        let pending = &state.records[state.last_flush_index..];
        let start_ts = pending
            .first()
            .map(|record| record.timestamp)
            .ok_or_else(|| anyhow::anyhow!("empty pending segment"))?;
        let end_ts = pending
            .last()
            .map(|record| record.timestamp)
            .ok_or_else(|| anyhow::anyhow!("empty pending segment"))?;

        let batch_id = format!("batch-{}", state.last_flush_index);
        state.last_flush_index = state.records.len();

        Ok(PersistenceBatch::new(batch_id, start_ts, end_ts))
    }
}

#[async_trait]
impl WalReader for InMemoryWal {
    async fn replay(&self, from: WalOffset) -> anyhow::Result<Vec<WalRecord>> {
        let state = self.state.lock().await;
        let start = from.0 as usize;
        if start >= state.records.len() {
            return Ok(Vec::new());
        }
        Ok(state.records[start..].to_vec())
    }

    async fn load_segments(&self) -> anyhow::Result<Vec<WalSegment>> {
        let state = self.state.lock().await;
        if state.records.is_empty() {
            return Ok(Vec::new());
        }
        let segment = WalSegment {
            base_offset: WalOffset(0),
            records: state.records.clone(),
            written_at: SystemTime::now(),
        };
        Ok(vec![segment])
    }
}
