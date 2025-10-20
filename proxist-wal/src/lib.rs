//! Write-ahead log semantics for Proxist ingest durability.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::anyhow;
use async_trait::async_trait;
use proxist_core::{metadata::TenantId, watermark::PersistenceBatch};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};
use serde_json;

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
    pub file_prefix: String,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./wal"),
            segment_bytes: 8 * 1024 * 1024,
            fsync_interval: std::time::Duration::from_millis(5),
            file_prefix: "proxist-wal".into(),
        }
    }
}

#[async_trait]
pub trait WalWriter: Send + Sync {
    async fn append(&self, record: WalRecord) -> anyhow::Result<WalOffset>;
    async fn flush_segment(&self) -> anyhow::Result<WalSegment>;
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

    pub fn to_persistence_batch(&self) -> anyhow::Result<PersistenceBatch> {
        let start = self
            .records
            .first()
            .ok_or_else(|| anyhow!("empty WAL segment"))?
            .timestamp;
        let end = self
            .records
            .last()
            .ok_or_else(|| anyhow!("empty WAL segment"))?
            .timestamp;

        Ok(PersistenceBatch::new(
            format!("batch-{}", self.base_offset.0),
            start,
            end,
        ))
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

    async fn flush_segment(&self) -> anyhow::Result<WalSegment> {
        let mut state = self.state.lock().await;
        if state.records.len() == state.last_flush_index {
            anyhow::bail!("no WAL records to flush");
        }

        let base_offset = state.last_flush_index as u64;
        let records = state.records[state.last_flush_index..].to_vec();
        state.last_flush_index = state.records.len();

        Ok(WalSegment {
            base_offset: WalOffset(base_offset),
            records,
            written_at: SystemTime::now(),
        })
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

#[derive(Debug)]
pub struct FileWal {
    config: WalConfig,
    path: PathBuf,
    state: Mutex<FileWalState>,
}

#[derive(Debug)]
struct FileWalState {
    file: File,
    buffer: Vec<WalRecord>,
    buffer_start_offset: u64,
    next_offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WalDiskEntry {
    offset: u64,
    record: WalRecord,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotManifest {
    snapshot: String,
    last_offset: u64,
}

impl FileWal {
    pub async fn open(config: WalConfig) -> anyhow::Result<Self> {
        fs::create_dir_all(&config.directory).await?;

        let path = wal_path(&config);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await?;

        let mut state = FileWalState {
            file,
            buffer: Vec::new(),
            buffer_start_offset: 0,
            next_offset: 0,
        };

        state.next_offset = Self::load_last_offset(&path).await?;

        Ok(Self {
            config,
            path,
            state: Mutex::new(state),
        })
    }

    async fn load_last_offset(path: &Path) -> anyhow::Result<u64> {
        let file = OpenOptions::new().read(true).open(path).await?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut last_offset = 0_u64;
        loop {
            line.clear();
            let bytes = reader.read_line(&mut line).await?;
            if bytes == 0 {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }
            let entry: WalDiskEntry = serde_json::from_str(line.trim())?;
            last_offset = entry.offset + 1;
        }
        Ok(last_offset)
    }

    pub async fn create_snapshot(&self) -> anyhow::Result<PathBuf> {
        let state = self.state.lock().await;
        let snapshot_name = format!(
            "{}-snapshot-{}.json",
            self.config.file_prefix, state.next_offset
        );
        let snapshot_path = self.config.directory.join(&snapshot_name);
        fs::copy(&self.path, &snapshot_path).await?;

        let manifest = SnapshotManifest {
            snapshot: snapshot_name,
            last_offset: state.next_offset,
        };
        let manifest_path = manifest_path(&self.config);
        let manifest_json = serde_json::to_string(&manifest)?;
        fs::write(manifest_path, manifest_json).await?;
        Ok(snapshot_path)
    }

    pub async fn load_snapshot(&self) -> anyhow::Result<Option<WalSegment>> {
        let manifest_path = manifest_path(&self.config);
        let manifest_bytes = match fs::read(&manifest_path).await {
            Ok(bytes) => bytes,
            Err(_) => return Ok(None),
        };
        let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
        let snapshot_path = self.config.directory.join(&manifest.snapshot);
        let file = OpenOptions::new().read(true).open(&snapshot_path).await?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut records = Vec::new();
        while reader.read_line(&mut line).await? != 0 {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                line.clear();
                continue;
            }
            let entry: WalDiskEntry = serde_json::from_str(trimmed)?;
            records.push(entry.record);
            line.clear();
        }
        if records.is_empty() {
            return Ok(None);
        }
        Ok(Some(WalSegment {
            base_offset: WalOffset(0),
            records,
            written_at: SystemTime::now(),
        }))
    }
}

#[async_trait]
impl WalWriter for FileWal {
    async fn append(&self, record: WalRecord) -> anyhow::Result<WalOffset> {
        let mut guard = self.state.lock().await;
        let offset = guard.next_offset;
        let entry = WalDiskEntry {
            offset,
            record: record.clone(),
        };
        let json = serde_json::to_vec(&entry)?;

        guard.file.write_all(&json).await?;
        guard.file.write_all(b"\n").await?;
        guard.file.sync_data().await?;

        if guard.buffer.is_empty() {
            guard.buffer_start_offset = offset;
        }
        guard.buffer.push(record);
        guard.next_offset += 1;

        Ok(WalOffset(offset))
    }

    async fn flush_segment(&self) -> anyhow::Result<WalSegment> {
        let mut guard = self.state.lock().await;
        if guard.buffer.is_empty() {
            anyhow::bail!("no WAL records to flush");
        }

        let segment = WalSegment {
            base_offset: WalOffset(guard.buffer_start_offset),
            records: guard.buffer.clone(),
            written_at: SystemTime::now(),
        };

        guard.buffer.clear();
        guard.buffer_start_offset = guard.next_offset;

        Ok(segment)
    }
}

#[async_trait]
impl WalReader for FileWal {
    async fn replay(&self, from: WalOffset) -> anyhow::Result<Vec<WalRecord>> {
        let path = self.path.clone();
        let file = OpenOptions::new().read(true).open(&path).await?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut records = Vec::new();
        while reader.read_line(&mut line).await? != 0 {
            if line.trim().is_empty() {
                line.clear();
                continue;
            }
            let entry: WalDiskEntry = serde_json::from_str(line.trim())?;
            if entry.offset >= from.0 {
                records.push(entry.record);
            }
            line.clear();
        }
        Ok(records)
    }

    async fn load_segments(&self) -> anyhow::Result<Vec<WalSegment>> {
        let file = OpenOptions::new().read(true).open(&self.path).await?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut records = Vec::new();
        let mut first_offset = None;
        while reader.read_line(&mut line).await? != 0 {
            if line.trim().is_empty() {
                line.clear();
                continue;
            }
            let entry: WalDiskEntry = serde_json::from_str(line.trim())?;
            first_offset.get_or_insert(entry.offset);
            records.push(entry.record);
            line.clear();
        }
        if records.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![WalSegment {
            base_offset: WalOffset(first_offset.unwrap_or(0)),
            records,
            written_at: SystemTime::now(),
        }])
    }
}

fn wal_path(config: &WalConfig) -> PathBuf {
    config
        .directory
        .join(format!("{}-primary.log", config.file_prefix))
}

fn manifest_path(config: &WalConfig) -> PathBuf {
    config
        .directory
        .join(format!("{}-manifest.json", config.file_prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn file_wal_persists_and_replays() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let config = WalConfig {
            directory: dir.path().to_path_buf(),
            ..WalConfig::default()
        };

        let wal = FileWal::open(config.clone()).await?;
        wal.append(sample_record("AAPL", 1)).await?;
        wal.append(sample_record("AAPL", 2)).await?;
        let segment = wal.flush_segment().await?;
        assert_eq!(segment.records.len(), 2);

        drop(wal);

        let wal_reopen = FileWal::open(config).await?;
        let replayed = wal_reopen.replay(WalOffset(0)).await?;
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].seq, 1);
        assert_eq!(replayed[1].seq, 2);

        Ok(())
    }

    #[tokio::test]
    async fn file_wal_snapshot_and_load() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let mut config = WalConfig::default();
        config.directory = dir.path().to_path_buf();
        let wal = FileWal::open(config.clone()).await?;
        wal.append(sample_record("AAPL", 1)).await?;
        wal.append(sample_record("AAPL", 2)).await?;
        wal.flush_segment().await?;

        let snapshot_path = wal.create_snapshot().await?;
        assert!(snapshot_path.exists());

        let snapshot_segment = wal.load_snapshot().await?.expect("snapshot segment");
        assert_eq!(snapshot_segment.records.len(), 2);

        Ok(())
    }

    fn sample_record(symbol: &str, seq: u64) -> WalRecord {
        WalRecord {
            tenant: "alpha".into(),
            shard_id: "alpha::default".into(),
            symbol: symbol.into(),
            timestamp: SystemTime::now(),
            payload: vec![1, 2, 3],
            seq,
        }
    }
}
