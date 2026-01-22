use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};

use proxist_core::ingest::IngestRecord;

const MANIFEST_FILE: &str = "manifest.json";

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub segment_max_bytes: u64,
    pub snapshot_every_rows: u64,
    pub fsync: bool,
}

impl WalConfig {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            segment_max_bytes: 256 * 1024 * 1024,
            snapshot_every_rows: 5_000_000,
            fsync: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalSegmentMeta {
    file: String,
    start_seq: u64,
    end_seq: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalManifest {
    next_seq: u64,
    last_snapshot_seq: u64,
    last_snapshot_micros: Option<i64>,
    segments: Vec<WalSegmentMeta>,
}

impl WalManifest {
    fn new() -> Self {
        Self {
            next_seq: 1,
            last_snapshot_seq: 0,
            last_snapshot_micros: None,
            segments: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntry {
    seq: u64,
    record: IngestRecord,
}

#[derive(Debug, Clone)]
pub struct WalSnapshot {
    pub last_seq: u64,
    pub last_micros: Option<i64>,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct WalAppendResult {
    pub first_seq: u64,
    pub last_seq: u64,
    pub last_micros: Option<i64>,
    pub bytes_written: u64,
    pub rows_written: u64,
}

struct WalWriter {
    writer: BufWriter<File>,
    meta_index: usize,
}

struct WalState {
    manifest: WalManifest,
    writer: Option<WalWriter>,
    rows_since_snapshot: u64,
}

pub struct WalManager {
    config: WalConfig,
    state: Mutex<WalState>,
}

impl WalManager {
    pub fn open(config: WalConfig) -> anyhow::Result<Self> {
        fs::create_dir_all(&config.dir)
            .with_context(|| format!("create wal dir {:?}", config.dir))?;

        let manifest_path = config.dir.join(MANIFEST_FILE);
        let manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)
                .with_context(|| format!("read wal manifest {:?}", manifest_path))?;
            serde_json::from_slice(&bytes)
                .context("deserialize wal manifest")
                .unwrap_or_else(|_| WalManifest::new())
        } else {
            WalManifest::new()
        };

        let mut state = WalState {
            manifest,
            writer: None,
            rows_since_snapshot: 0,
        };

        state.manifest = rebuild_manifest_if_needed(&config, state.manifest)?;
        state.writer = Some(open_writer(&config, &mut state.manifest)?);

        Ok(Self {
            config,
            state: Mutex::new(state),
        })
    }

    pub fn backlog_bytes(&self) -> u64 {
        let guard = self.state.lock().expect("wal state lock poisoned");
        guard
            .manifest
            .segments
            .iter()
            .map(|seg| seg.bytes)
            .sum()
    }

    pub fn snapshot_due(&self) -> bool {
        let guard = self.state.lock().expect("wal state lock poisoned");
        guard.rows_since_snapshot >= self.config.snapshot_every_rows && guard.manifest.next_seq > 1
    }

    pub fn append_records(&self, records: &[IngestRecord]) -> anyhow::Result<WalAppendResult> {
        if records.is_empty() {
            return Ok(WalAppendResult {
                first_seq: 0,
                last_seq: 0,
                last_micros: None,
                bytes_written: 0,
                rows_written: 0,
            });
        }

        let mut guard = self.state.lock().expect("wal state lock poisoned");
        let mut bytes_written = 0_u64;
        let mut last_micros: Option<i64> = None;
        let first_seq = guard.manifest.next_seq;
        let mut last_seq = guard.manifest.next_seq;
        let mut writer = guard
            .writer
            .take()
            .ok_or_else(|| anyhow!("wal writer not initialized"))?;

        for record in records {
            let seq = guard.manifest.next_seq;
            let entry = WalEntry {
                seq,
                record: record.clone(),
            };
            let payload = bincode::serialize(&entry).context("serialize wal entry")?;
            let mut crc = Crc32::new();
            crc.update(&payload);
            let checksum = crc.finalize();
            let entry_len = payload.len() as u64;
            let header_len = 8_u64;

            ensure_writer_capacity(
                &self.config,
                &mut guard.manifest,
                Some(&mut writer),
                entry_len + header_len,
            )?;

            writer
                .writer
                .write_all(&(payload.len() as u32).to_le_bytes())?;
            writer.writer.write_all(&checksum.to_le_bytes())?;
            writer.writer.write_all(&payload)?;

            guard.manifest.next_seq = seq + 1;
            last_seq = seq;
            bytes_written += header_len + entry_len;

            let meta = guard
                .manifest
                .segments
                .get_mut(writer.meta_index)
                .ok_or_else(|| anyhow!("wal segment metadata missing"))?;
            meta.end_seq = seq;
            meta.bytes += header_len + entry_len;

            last_micros = Some(system_time_to_micros(record.timestamp));
        }

        writer.writer.flush()?;
        if self.config.fsync {
            writer
                .writer
                .get_ref()
                .sync_data()
                .context("fsync wal segment")?;
        }

        guard.rows_since_snapshot += records.len() as u64;
        write_manifest(&self.config.dir, &guard.manifest)?;
        guard.writer = Some(writer);

        Ok(WalAppendResult {
            first_seq,
            last_seq,
            last_micros,
            bytes_written,
            rows_written: records.len() as u64,
        })
    }

    pub fn maybe_snapshot(
        &self,
        snapshot_bytes: Vec<u8>,
        last_seq: u64,
        last_micros: Option<i64>,
    ) -> anyhow::Result<bool> {
        if last_seq == 0 {
            return Ok(false);
        }
        let mut guard = self.state.lock().expect("wal state lock poisoned");
        if guard.rows_since_snapshot < self.config.snapshot_every_rows {
            return Ok(false);
        }

        let snapshot_file = format!("snapshot-{}.json", last_seq);
        let snapshot_path = self.config.dir.join(&snapshot_file);
        write_atomic(&snapshot_path, &snapshot_bytes)
            .with_context(|| format!("write wal snapshot {:?}", snapshot_path))?;

        guard.manifest.last_snapshot_seq = last_seq;
        guard.manifest.last_snapshot_micros = last_micros;

        let mut retained = Vec::new();
        for seg in guard.manifest.segments.drain(..) {
            if seg.end_seq <= last_seq {
                let path = self.config.dir.join(&seg.file);
                let _ = fs::remove_file(path);
            } else {
                retained.push(seg);
            }
        }
        guard.manifest.segments = retained;
        guard.rows_since_snapshot = 0;

        if guard.manifest.segments.is_empty() {
            guard.writer = Some(open_writer(&self.config, &mut guard.manifest)?);
        }

        write_manifest(&self.config.dir, &guard.manifest)?;
        Ok(true)
    }

    pub fn load_snapshot(&self) -> anyhow::Result<Option<WalSnapshot>> {
        let guard = self.state.lock().expect("wal state lock poisoned");
        if guard.manifest.last_snapshot_seq == 0 {
            return Ok(None);
        }
        let snapshot_file = format!("snapshot-{}.json", guard.manifest.last_snapshot_seq);
        let snapshot_path = self.config.dir.join(&snapshot_file);
        if !snapshot_path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&snapshot_path)
            .with_context(|| format!("read wal snapshot {:?}", snapshot_path))?;
        Ok(Some(WalSnapshot {
            last_seq: guard.manifest.last_snapshot_seq,
            last_micros: guard.manifest.last_snapshot_micros,
            bytes,
        }))
    }

    pub fn replay_from(&self, after_seq: u64) -> anyhow::Result<Vec<IngestRecord>> {
        let guard = self.state.lock().expect("wal state lock poisoned");
        let mut segments = guard.manifest.segments.clone();
        drop(guard);
        segments.sort_by_key(|seg| seg.start_seq);

        let mut records = Vec::new();
        for seg in segments {
            let path = self.config.dir.join(&seg.file);
            if !path.exists() {
                continue;
            }
            let file = File::open(&path)
                .with_context(|| format!("open wal segment {:?}", path))?;
            let mut reader = BufReader::new(file);

            loop {
                let mut header = [0_u8; 8];
                if reader.read_exact(&mut header).is_err() {
                    break;
                }
                let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
                let checksum = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

                let mut payload = vec![0_u8; len];
                if reader.read_exact(&mut payload).is_err() {
                    break;
                }

                let mut crc = Crc32::new();
                crc.update(&payload);
                let actual = crc.finalize();
                if actual != checksum {
                    return Err(anyhow!(
                        "wal checksum mismatch in {:?}: expected={}, got={}",
                        path,
                        checksum,
                        actual
                    ));
                }

                let entry: WalEntry = bincode::deserialize(&payload)
                    .context("deserialize wal entry")?;
                if entry.seq > after_seq {
                    records.push(entry.record);
                }
            }
        }
        Ok(records)
    }
}

fn rebuild_manifest_if_needed(
    config: &WalConfig,
    mut manifest: WalManifest,
) -> anyhow::Result<WalManifest> {
    let needs_rebuild = manifest.segments.is_empty()
        || manifest
            .segments
            .iter()
            .any(|seg| !config.dir.join(&seg.file).exists());
    if !needs_rebuild {
        return Ok(manifest);
    }

    let mut segments = Vec::new();
    let mut next_seq = 1_u64;

    for entry in fs::read_dir(&config.dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if !name.starts_with("segment-") || !name.ends_with(".wal") {
            continue;
        }
        let start_seq = parse_segment_start(name).unwrap_or(0);
        if start_seq == 0 {
            continue;
        }
        let meta = scan_segment(&path, name.to_string(), start_seq)?;
        next_seq = next_seq.max(meta.end_seq.saturating_add(1));
        segments.push(meta);
    }

    segments.sort_by_key(|seg| seg.start_seq);

    manifest.segments = segments;
    manifest.next_seq = next_seq;
    write_manifest(&config.dir, &manifest)?;
    Ok(manifest)
}

fn parse_segment_start(name: &str) -> Option<u64> {
    let suffix = name.strip_prefix("segment-")?;
    let suffix = suffix.strip_suffix(".wal")?;
    suffix.parse::<u64>().ok()
}

fn scan_segment(path: &Path, name: String, start_seq: u64) -> anyhow::Result<WalSegmentMeta> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut end_seq = start_seq.saturating_sub(1);
    let mut bytes = 0_u64;

    loop {
        let mut header = [0_u8; 8];
        if reader.read_exact(&mut header).is_err() {
            break;
        }
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let checksum = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let mut payload = vec![0_u8; len];
        if reader.read_exact(&mut payload).is_err() {
            break;
        }
        let mut crc = Crc32::new();
        crc.update(&payload);
        let actual = crc.finalize();
        if actual != checksum {
            break;
        }
        let entry: WalEntry = bincode::deserialize(&payload)
            .context("deserialize wal entry during scan")?;
        end_seq = entry.seq;
        bytes += 8_u64 + len as u64;
    }

    Ok(WalSegmentMeta {
        file: name,
        start_seq,
        end_seq,
        bytes,
    })
}

fn open_writer(config: &WalConfig, manifest: &mut WalManifest) -> anyhow::Result<WalWriter> {
    let mut use_index = None;

    if let Some((idx, meta)) = manifest.segments.iter().enumerate().last() {
        let path = config.dir.join(&meta.file);
        let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if size < config.segment_max_bytes {
            use_index = Some(idx);
        }
    }

    if use_index.is_none() {
        let start_seq = manifest.next_seq;
        let filename = format!("segment-{}.wal", start_seq);
        manifest.segments.push(WalSegmentMeta {
            file: filename,
            start_seq,
            end_seq: start_seq.saturating_sub(1),
            bytes: 0,
        });
        use_index = Some(manifest.segments.len() - 1);
    }

    let index = use_index.expect("segment index should be set");
    let path = config.dir.join(&manifest.segments[index].file);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open wal segment {:?}", path))?;

    Ok(WalWriter {
        writer: BufWriter::new(file),
        meta_index: index,
    })
}

fn ensure_writer_capacity(
    config: &WalConfig,
    manifest: &mut WalManifest,
    writer: Option<&mut WalWriter>,
    incoming: u64,
) -> anyhow::Result<()> {
    let Some(writer) = writer else {
        return Ok(());
    };
    let meta = manifest
        .segments
        .get(writer.meta_index)
        .ok_or_else(|| anyhow!("wal segment metadata missing"))?;
    if meta.bytes + incoming <= config.segment_max_bytes {
        return Ok(());
    }

    writer.writer.flush()?;
    if config.fsync {
        writer
            .writer
            .get_ref()
            .sync_data()
            .context("fsync wal segment")?;
    }

    let start_seq = manifest.next_seq;
    let filename = format!("segment-{}.wal", start_seq);
    manifest.segments.push(WalSegmentMeta {
        file: filename,
        start_seq,
        end_seq: start_seq.saturating_sub(1),
        bytes: 0,
    });

    let new_index = manifest.segments.len() - 1;
    let path = config.dir.join(&manifest.segments[new_index].file);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open wal segment {:?}", path))?;
    writer.writer = BufWriter::new(file);
    writer.meta_index = new_index;

    Ok(())
}

fn write_manifest(dir: &Path, manifest: &WalManifest) -> anyhow::Result<()> {
    let path = dir.join(MANIFEST_FILE);
    let bytes = serde_json::to_vec_pretty(manifest).context("serialize wal manifest")?;
    write_atomic(&path, &bytes).with_context(|| format!("write wal manifest {:?}", path))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_data()?;
    }
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn system_time_to_micros(ts: SystemTime) -> i64 {
    ts.duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_micros() as i64)
        .unwrap_or_else(|err| {
            let dur = err.duration();
            -(dur.as_micros() as i64)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn record(tenant: &str, shard: &str, symbol: &str, seq: u64, ts: i64) -> IngestRecord {
        IngestRecord {
            tenant: tenant.into(),
            shard_id: shard.into(),
            symbol: symbol.into(),
            timestamp: UNIX_EPOCH + std::time::Duration::from_micros(ts as u64),
            payload: vec![1, 2, 3],
            seq,
        }
    }

    #[test]
    fn wal_append_and_replay() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            segment_max_bytes: 1024,
            snapshot_every_rows: 1000,
            fsync: false,
        };
        let wal = WalManager::open(config)?;
        let records = vec![record("alpha", "s1", "AAPL", 1, 1000)];
        let res = wal.append_records(&records)?;
        assert_eq!(res.rows_written, 1);

        let replayed = wal.replay_from(0)?;
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].tenant, "alpha");
        Ok(())
    }

    #[test]
    fn wal_snapshot_prunes_segments() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            segment_max_bytes: 256,
            snapshot_every_rows: 1,
            fsync: false,
        };
        let wal = WalManager::open(config)?;
        let records = vec![record("alpha", "s1", "AAPL", 1, 1000)];
        let res = wal.append_records(&records)?;
        let snapshot = b"[]".to_vec();
        let did = wal.maybe_snapshot(snapshot, res.last_seq, res.last_micros)?;
        assert!(did);
        let snapshot = wal.load_snapshot()?;
        assert!(snapshot.is_some());
        Ok(())
    }
}
