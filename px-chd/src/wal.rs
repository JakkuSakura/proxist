use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use pxd::pxl::Op;

const MAGIC: [u8; 2] = *b"PW";
const VERSION: u8 = 2;
const HEADER_LEN: usize = 2 + 1 + 1 + 4 + 4 + 8;

#[derive(Debug)]
pub struct WalRecord {
    pub op: Op,
    pub payload: Vec<u8>,
    pub lsn: u64,
}

#[derive(Debug)]
pub enum WalRead {
    NeedMore,
    Record(WalRecord),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("invalid data: {0}")]
    InvalidData(String),
}

pub struct WalTail {
    file: File,
}

impl WalTail {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn seek_to_lsn(&mut self, target: u64) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(0))?;
        loop {
            let pos = self.file.stream_position()?;
            match self.read_record()? {
                WalRead::NeedMore => {
                    self.file.seek(SeekFrom::Start(pos))?;
                    break;
                }
                WalRead::Record(record) => {
                    if record.lsn > target {
                        self.file.seek(SeekFrom::Start(pos))?;
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn read_next(&mut self) -> Result<WalRead, Error> {
        self.read_record()
    }

    fn read_record(&mut self) -> Result<WalRead, Error> {
        let start_pos = self.file.stream_position()?;
        let mut header = [0u8; HEADER_LEN];
        match read_exact_or_rewind(&mut self.file, &mut header, start_pos)? {
            ReadOutcome::NeedMore => return Ok(WalRead::NeedMore),
            ReadOutcome::Ok => {}
        }

        if header[0..2] != MAGIC || header[2] != VERSION {
            return Err(Error::Protocol("wal header mismatch".to_string()));
        }
        let op = Op::from_u8(header[3]).map_err(|_| Error::Protocol("unknown op".to_string()))?;
        let len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        let checksum = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let lsn = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);

        let mut payload = vec![0u8; len];
        match read_exact_or_rewind(&mut self.file, &mut payload, start_pos)? {
            ReadOutcome::NeedMore => return Ok(WalRead::NeedMore),
            ReadOutcome::Ok => {}
        }

        if checksum_u32(&payload) != checksum {
            return Err(Error::InvalidData("wal checksum mismatch".to_string()));
        }

        Ok(WalRead::Record(WalRecord { op, payload, lsn }))
    }
}

enum ReadOutcome {
    Ok,
    NeedMore,
}

fn read_exact_or_rewind(
    file: &mut File,
    buf: &mut [u8],
    rewind_pos: u64,
) -> Result<ReadOutcome, Error> {
    match file.read_exact(buf) {
        Ok(()) => Ok(ReadOutcome::Ok),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            file.seek(SeekFrom::Start(rewind_pos))?;
            Ok(ReadOutcome::NeedMore)
        }
        Err(err) => Err(err.into()),
    }
}

fn checksum_u32(bytes: &[u8]) -> u32 {
    let mut sum: u32 = 0;
    for b in bytes {
        sum = sum.wrapping_add(*b as u32);
    }
    sum
}
