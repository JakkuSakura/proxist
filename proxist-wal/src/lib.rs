//! Write-ahead log semantics for Proxist ingest durability.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use crc32fast::Hasher;
use proxist_core::{metadata::TenantId, watermark::PersistenceBatch};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};

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
    pub fsync_interval: Duration,
    pub file_prefix: String,
    pub snapshot_interval_segments: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./wal"),
            segment_bytes: 8 * 1024 * 1024,
            fsync_interval: Duration::from_millis(5),
            file_prefix: "proxist-wal".into(),
            snapshot_interval_segments: 64,
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
    buffer_bytes: usize,
    segments_since_snapshot: usize,
    last_sync: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalDiskEntry {
    offset: u64,
    checksum: u32,
    record: WalRecord,
}

impl WalDiskEntry {
    fn new(offset: u64, record: WalRecord) -> Self {
        let checksum = checksum_for_record(&record);
        Self {
            offset,
            checksum,
            record,
        }
    }

    fn verify(&self) -> anyhow::Result<()> {
        let expected = checksum_for_record(&self.record);
        if expected != self.checksum {
            anyhow::bail!("checksum mismatch at WAL offset {}", self.offset);
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotManifest {
    snapshot: String,
    last_offset: u64,
    created_at_millis: u64,
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

        let next_offset = Self::load_last_offset(&path).await?;
        let now = Instant::now();
        let last_sync = now.checked_sub(config.fsync_interval).unwrap_or(now);

        let state = FileWalState {
            file,
            buffer: Vec::new(),
            buffer_start_offset: next_offset,
            next_offset,
            buffer_bytes: 0,
            segments_since_snapshot: 0,
            last_sync,
        };

        Ok(Self {
            config,
            path,
            state: Mutex::new(state),
        })
    }

    async fn load_last_offset(path: &Path) -> anyhow::Result<u64> {
        let file = match OpenOptions::new().read(true).open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(0),
            Err(err) => return Err(err.into()),
        };

        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut last_offset = 0_u64;
        loop {
            line.clear();
            let bytes = reader.read_line(&mut line).await?;
            if bytes == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let entry: WalDiskEntry = serde_json::from_str(trimmed)
                .with_context(|| "decode WAL entry while loading last offset")?;
            entry
                .verify()
                .with_context(|| format!("verify WAL entry at offset {}", entry.offset))?;
            last_offset = entry.offset + 1;
        }
        Ok(last_offset)
    }

    pub async fn create_snapshot(&self) -> anyhow::Result<PathBuf> {
        let mut state = self.state.lock().await;
        let snapshot_name = format!(
            "{}-snapshot-{}.json",
            self.config.file_prefix, state.next_offset
        );
        let snapshot_path = self.config.directory.join(&snapshot_name);
        fs::copy(&self.path, &snapshot_path)
            .await
            .context("copy WAL into snapshot")?;

        let manifest = SnapshotManifest {
            snapshot: snapshot_name,
            last_offset: state.next_offset,
            created_at_millis: system_time_to_millis(SystemTime::now()),
        };
        let manifest_path = manifest_path(&self.config);
        let manifest_json = serde_json::to_string(&manifest)?;
        fs::write(manifest_path, manifest_json)
            .await
            .context("write WAL snapshot manifest")?;
        state.segments_since_snapshot = 0;
        Ok(snapshot_path)
    }

    pub async fn load_snapshot(&self) -> anyhow::Result<Option<WalSegment>> {
        let manifest_path = manifest_path(&self.config);
        let manifest_bytes = match fs::read(&manifest_path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
        let snapshot_path = self.config.directory.join(&manifest.snapshot);
        let entries = read_entries(&snapshot_path, 0).await?;
        if entries.is_empty() {
            return Ok(None);
        }
        let base_offset = entries.first().map(|entry| entry.offset).unwrap_or(0);
        let records = entries.into_iter().map(|entry| entry.record).collect();
        Ok(Some(WalSegment {
            base_offset: WalOffset(base_offset),
            records,
            written_at: SystemTime::now(),
        }))
    }

    pub async fn load_snapshot_and_replay(&self) -> anyhow::Result<Vec<WalRecord>> {
        let manifest_path = manifest_path(&self.config);
        let manifest_bytes = match fs::read(&manifest_path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(read_entries(&self.path, 0)
                    .await?
                    .into_iter()
                    .map(|entry| entry.record)
                    .collect())
            }
            Err(err) => return Err(err.into()),
        };

        let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
        let snapshot_path = self.config.directory.join(&manifest.snapshot);

        let mut records: Vec<WalRecord> = read_entries(&snapshot_path, 0)
            .await?
            .into_iter()
            .map(|entry| entry.record)
            .collect();

        let tail = read_entries(&self.path, manifest.last_offset)
            .await?
            .into_iter()
            .map(|entry| entry.record)
            .collect::<Vec<_>>();

        records.extend(tail);

        Ok(records)
    }
}

#[async_trait]
impl WalWriter for FileWal {
    async fn append(&self, record: WalRecord) -> anyhow::Result<WalOffset> {
        let mut guard = self.state.lock().await;
        let offset = guard.next_offset;
        let entry = WalDiskEntry::new(offset, record.clone());
        let line = serde_json::to_vec(&entry)?;

        guard.file.write_all(&line).await?;
        guard.file.write_all(b"\n").await?;

        if guard.buffer.is_empty() {
            guard.buffer_start_offset = offset;
        }
        guard.buffer.push(record);
        guard.buffer_bytes += line.len() + 1;
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

        guard
            .file
            .sync_data()
            .await
            .context("sync WAL data to disk")?;
        guard.last_sync = Instant::now();
        guard.buffer.clear();
        guard.buffer_start_offset = guard.next_offset;
        guard.buffer_bytes = 0;

        guard.segments_since_snapshot += 1;
        let snapshot_due = self.config.snapshot_interval_segments > 0
            && guard.segments_since_snapshot >= self.config.snapshot_interval_segments;

        drop(guard);

        if snapshot_due {
            self.create_snapshot().await?;
        }

        Ok(segment)
    }
}

#[async_trait]
impl WalReader for FileWal {
    async fn replay(&self, from: WalOffset) -> anyhow::Result<Vec<WalRecord>> {
        Ok(read_entries(&self.path, from.0)
            .await?
            .into_iter()
            .map(|entry| entry.record)
            .collect())
    }

    async fn load_segments(&self) -> anyhow::Result<Vec<WalSegment>> {
        let entries = read_entries(&self.path, 0).await?;
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let base_offset = entries.first().map(|entry| entry.offset).unwrap_or(0);
        let records = entries.into_iter().map(|entry| entry.record).collect();

        Ok(vec![WalSegment {
            base_offset: WalOffset(base_offset),
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

async fn read_entries(path: &Path, start_offset: u64) -> anyhow::Result<Vec<WalDiskEntry>> {
    let file = match OpenOptions::new().read(true).open(path).await {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };

    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut entries = Vec::new();

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let entry: WalDiskEntry = serde_json::from_str(trimmed)
            .with_context(|| format!("decode WAL entry from {}", path.display()))?;
        entry
            .verify()
            .with_context(|| format!("verify WAL entry at offset {}", entry.offset))?;
        if entry.offset >= start_offset {
            entries.push(entry);
        }
    }

    Ok(entries)
}

fn checksum_for_record(record: &WalRecord) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(record.tenant.as_bytes());
    hasher.update(record.shard_id.as_bytes());
    hasher.update(record.symbol.as_bytes());

    let micros: i128 = match record.timestamp.duration_since(UNIX_EPOCH) {
        Ok(dur) => dur.as_micros() as i128,
        Err(err) => -(err.duration().as_micros() as i128),
    };
    hasher.update(&micros.to_le_bytes());

    hasher.update(&record.seq.to_le_bytes());
    let len = record.payload.len() as u64;
    hasher.update(&len.to_le_bytes());
    hasher.update(&record.payload);

    hasher.finalize()
}

fn system_time_to_millis(ts: SystemTime) -> u64 {
    ts.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

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

    #[tokio::test]
    async fn file_wal_persists_and_replays() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let config = WalConfig {
            directory: dir.path().to_path_buf(),
            snapshot_interval_segments: 0,
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
        config.snapshot_interval_segments = 0;

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

    #[tokio::test]
    async fn file_wal_detects_checksum_corruption() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let config = WalConfig {
            directory: dir.path().to_path_buf(),
            snapshot_interval_segments: 0,
            ..WalConfig::default()
        };

        let wal = FileWal::open(config.clone()).await?;
        wal.append(sample_record("AAPL", 1)).await?;
        wal.flush_segment().await?;
        drop(wal);

        let wal_path = wal_path(&config);
        let mut contents = fs::read_to_string(&wal_path)?;
        // Flip a byte inside the payload to trigger checksum mismatch.
        if let Some(idx) = contents.find("1,2,3") {
            contents.replace_range(idx..idx + 1, "9");
        }
        fs::write(&wal_path, contents)?;

        match FileWal::open(config.clone()).await {
            Ok(wal_reopen) => {
                let replay_result = wal_reopen.replay(WalOffset(0)).await;
                assert!(replay_result.is_err());
            }
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("checksum mismatch") || msg.contains("verify WAL entry"),
                    "unexpected error message: {msg}"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn file_wal_load_snapshot_and_replay() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let mut config = WalConfig::default();
        config.directory = dir.path().to_path_buf();
        config.snapshot_interval_segments = 0;

        let wal = FileWal::open(config.clone()).await?;
        wal.append(sample_record("AAPL", 1)).await?;
        wal.flush_segment().await?;
        wal.create_snapshot().await?;

        wal.append(sample_record("AAPL", 2)).await?;
        wal.flush_segment().await?;
        drop(wal);

        let wal_reopen = FileWal::open(config).await?;
        let records = wal_reopen.load_snapshot_and_replay().await?;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].seq, 1);
        assert_eq!(records[1].seq, 2);

        Ok(())
    }
}
