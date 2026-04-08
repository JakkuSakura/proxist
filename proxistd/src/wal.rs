use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::Result;
use crate::memstore::MemStore;
use crate::pxl::{
    decode_delete_payload, decode_put_payload, encode_delete_payload, encode_put_payload, Op,
};

const MAGIC: [u8; 2] = *b"PW";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 2 + 1 + 1 + 4 + 4;

#[derive(Debug)]
pub struct Wal {
    file: File,
    sync: bool,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>, mem: &mut MemStore, sync: bool) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            replay_wal(path, mem)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self { file, sync })
    }

    pub fn append_put(&mut self, table: &str, symbol: &str, ts: u64, value: &[u8]) -> Result<()> {
        let payload = encode_put_payload(table, symbol, ts, value)?;
        let checksum = checksum_u32(&payload);
        let mut header = [0u8; HEADER_LEN];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3] = Op::Put as u8;
        header[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[8..12].copy_from_slice(&checksum.to_le_bytes());
        self.file.write_all(&header)?;
        self.file.write_all(&payload)?;
        if self.sync {
            self.file.flush()?;
            self.file.sync_all()?;
        }
        Ok(())
    }

    pub fn append_delete(&mut self, table: &str, symbol: &str) -> Result<()> {
        let payload = encode_delete_payload(table, symbol)?;
        let checksum = checksum_u32(&payload);
        let mut header = [0u8; HEADER_LEN];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3] = Op::Delete as u8;
        header[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[8..12].copy_from_slice(&checksum.to_le_bytes());
        self.file.write_all(&header)?;
        self.file.write_all(&payload)?;
        if self.sync {
            self.file.flush()?;
            self.file.sync_all()?;
        }
        Ok(())
    }
}

fn replay_wal(path: &Path, mem: &mut MemStore) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut offset: u64 = 0;
    let mut truncate_at: Option<u64> = None;
    loop {
        let mut header = [0u8; HEADER_LEN];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
        if header[0..2] != MAGIC || header[2] != VERSION {
            truncate_at = Some(offset);
            break;
        }
        let op = Op::from_u8(header[3])?;
        let len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        let checksum = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let mut payload = vec![0u8; len];
        match reader.read_exact(&mut payload) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                truncate_at = Some(offset);
                break;
            }
            Err(err) => return Err(err.into()),
        }
        if checksum_u32(&payload) != checksum {
            truncate_at = Some(offset);
            break;
        }
        match op {
            Op::Put => {
                let (table, symbol, ts, value) = decode_put_payload(&payload)?;
                mem.put(table, symbol, ts, value.to_vec());
            }
            Op::Delete => {
                let (table, symbol) = decode_delete_payload(&payload)?;
                mem.delete(table, symbol);
            }
            _ => {}
        }
        offset += (HEADER_LEN + len) as u64;
    }
    if let Some(pos) = truncate_at {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(pos)?;
    }
    Ok(())
}

fn checksum_u32(bytes: &[u8]) -> u32 {
    let mut sum: u32 = 0;
    for b in bytes {
        sum = sum.wrapping_add(*b as u32);
    }
    sum
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::memstore::MemStore;
    use crate::pxl::encode_put_payload;

    use super::Wal;

    fn temp_wal_path(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        path.push(format!("proxistd_{label}_{nanos}.wal"));
        path
    }

    #[test]
    fn wal_replay_restores_puts() {
        let path = temp_wal_path("replay");
        let _ = fs::remove_file(&path);

        let mut mem = MemStore::new();
        {
            let mut wal = Wal::open(&path, &mut mem, true).expect("open wal");
            wal.append_put("ticks", "AAPL", 1, b"first")
                .expect("append first");
            wal.append_put("ticks", "AAPL", 2, b"second")
                .expect("append second");
        }

        let mut replayed = MemStore::new();
        let _wal = Wal::open(&path, &mut replayed, true).expect("replay wal");
        let row = replayed
            .get_last("ticks", "AAPL")
            .expect("replayed last row");
        assert_eq!(row.ts, 2);
        assert_eq!(row.value, b"second");

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_replay_truncates_corrupt_tail() {
        let path = temp_wal_path("truncate");
        let _ = fs::remove_file(&path);

        let mut mem = MemStore::new();
        {
            let mut wal = Wal::open(&path, &mut mem, true).expect("open wal");
            wal.append_put("ticks", "AAPL", 1, b"first")
                .expect("append first");
            wal.append_put("ticks", "AAPL", 2, b"second")
                .expect("append second");
        }

        let payload = encode_put_payload("ticks", "AAPL", 1, b"first").expect("payload");
        let entry_len = (super::HEADER_LEN + payload.len()) as u64;

        let original_len = fs::metadata(&path).expect("metadata").len();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open truncate")
            .set_len(original_len.saturating_sub(3))
            .expect("truncate tail");

        let mut replayed = MemStore::new();
        let _wal = Wal::open(&path, &mut replayed, true).expect("replay wal");
        let truncated_len = fs::metadata(&path).expect("metadata").len();
        assert_eq!(truncated_len, entry_len);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_replay_applies_deletes() {
        let path = temp_wal_path("delete");
        let _ = fs::remove_file(&path);

        let mut mem = MemStore::new();
        {
            let mut wal = Wal::open(&path, &mut mem, true).expect("open wal");
            wal.append_put("ticks", "AAPL", 1, b"first")
                .expect("append first");
            wal.append_put("ticks", "AAPL", 2, b"second")
                .expect("append second");
            wal.append_delete("ticks", "AAPL").expect("append delete");
        }

        let mut replayed = MemStore::new();
        let _wal = Wal::open(&path, &mut replayed, true).expect("replay wal");
        assert!(replayed.get_last("ticks", "AAPL").is_none());

        let _ = fs::remove_file(&path);
    }
}
