use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::Result;
use crate::expr::Expr;
use crate::memstore::MemStore;
use crate::pxl::{
    decode_delete_payload, decode_insert_payload, decode_schema_payload, decode_update_payload,
    encode_delete_payload, encode_insert_payload, encode_schema_payload, encode_update_payload, Op,
};
use crate::types::Schema;

const MAGIC: [u8; 2] = *b"PW";
const VERSION: u8 = 2;
const HEADER_LEN: usize = 2 + 1 + 1 + 4 + 4 + 8;

#[derive(Debug)]
pub struct Wal {
    file: File,
    sync: bool,
    next_lsn: u64,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>, mem: &mut MemStore, sync: bool) -> Result<Self> {
        let path = path.as_ref();
        let last_lsn = if path.exists() {
            replay_wal(path, mem)?
        } else {
            0
        };
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            sync,
            next_lsn: last_lsn.saturating_add(1),
        })
    }

    pub fn append_create(&mut self, table: &str, schema: &Schema) -> Result<u64> {
        let payload = encode_schema_payload(table, schema)?;
        self.append_record(Op::Create, &payload)
    }

    pub fn append_insert(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[Vec<crate::types::Value>],
    ) -> Result<u64> {
        let payload = encode_insert_payload(table, columns, rows)?;
        self.append_record(Op::Insert, &payload)
    }

    pub fn append_update(
        &mut self,
        table: &str,
        assignments: &[(String, crate::types::Value)],
        filter: Option<&Expr>,
    ) -> Result<u64> {
        let payload = encode_update_payload(table, assignments, filter)?;
        self.append_record(Op::Update, &payload)
    }

    pub fn append_delete(&mut self, table: &str, filter: Option<&Expr>) -> Result<u64> {
        let payload = encode_delete_payload(table, filter)?;
        self.append_record(Op::Delete, &payload)
    }

    fn append_record(&mut self, op: Op, payload: &[u8]) -> Result<u64> {
        let lsn = self.next_lsn;
        let checksum = checksum_u32(payload);
        let mut header = [0u8; HEADER_LEN];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3] = op as u8;
        header[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[8..12].copy_from_slice(&checksum.to_le_bytes());
        header[12..20].copy_from_slice(&lsn.to_le_bytes());
        self.file.write_all(&header)?;
        self.file.write_all(payload)?;
        if self.sync {
            self.file.flush()?;
            self.file.sync_all()?;
        }
        self.next_lsn = self.next_lsn.saturating_add(1);
        Ok(lsn)
    }
}

fn replay_wal(path: &Path, mem: &mut MemStore) -> Result<u64> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut offset: u64 = 0;
    let mut truncate_at: Option<u64> = None;
    let mut last_lsn = 0u64;

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
        let lsn = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
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
            Op::Create => {
                let (table, schema) = decode_schema_payload(&payload)?;
                mem.create_table(&table, schema)?;
            }
            Op::Insert => {
                let (table, columns, rows) = decode_insert_payload(&payload)?;
                mem.insert(&table, &columns, &rows)?;
            }
            Op::Update => {
                let (table, assignments, filter) = decode_update_payload(&payload)?;
                mem.update(&table, &assignments, filter.as_ref())?;
            }
            Op::Delete => {
                let (table, filter) = decode_delete_payload(&payload)?;
                mem.delete(&table, filter.as_ref())?;
            }
            _ => {}
        }
        last_lsn = last_lsn.max(lsn);
        offset += (HEADER_LEN + len) as u64;
    }
    if let Some(pos) = truncate_at {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(pos)?;
    }
    Ok(last_lsn)
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

    use crate::expr::{BinaryOp, Expr};
    use crate::memstore::MemStore;
    use crate::types::{ColumnSpec, ColumnType, Schema, Value};

    use super::Wal;

    fn temp_wal_path(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        path.push(format!("pxd_{label}_{nanos}.wal"));
        path
    }

    fn ticks_schema() -> Schema {
        Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
                nullable: false,
            },
            ColumnSpec {
                name: "ts".to_string(),
                col_type: ColumnType::I64,
                nullable: false,
            },
            ColumnSpec {
                name: "price".to_string(),
                col_type: ColumnType::F64,
                nullable: false,
            },
        ])
        .expect("schema")
    }

    #[test]
    fn wal_replay_restores_inserts() {
        let path = temp_wal_path("replay");
        let _ = fs::remove_file(&path);

        let mut mem = MemStore::new();
        {
            let mut wal = Wal::open(&path, &mut mem, true).expect("open wal");
            wal.append_create("ticks", &ticks_schema())
                .expect("append create");
            wal.append_insert(
                "ticks",
                &["symbol".to_string(), "ts".to_string(), "price".to_string()],
                &[vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(1),
                    Value::F64(10.0),
                ]],
            )
            .expect("append insert");
        }

        let mut replayed = MemStore::new();
        let _wal = Wal::open(&path, &mut replayed, true).expect("replay wal");
        let plan = crate::query::QueryPlan {
            table: "ticks".to_string(),
            join: None,
            filter: Some(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column("symbol".to_string())),
                right: Box::new(Expr::Literal(Value::String("AAPL".to_string()))),
            }),
            group_by: Vec::new(),
            select: vec![crate::query::SelectItem {
                expr: crate::query::SelectExpr::Column("price".to_string()),
                alias: None,
            }],
        };
        let result = replayed.query(&plan).expect("query");
        assert_eq!(result.rows.len(), 1);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_replay_truncates_corrupt_tail() {
        let path = temp_wal_path("truncate");
        let _ = fs::remove_file(&path);

        let mut mem = MemStore::new();
        {
            let mut wal = Wal::open(&path, &mut mem, true).expect("open wal");
            wal.append_create("ticks", &ticks_schema())
                .expect("append create");
            wal.append_insert(
                "ticks",
                &["symbol".to_string(), "ts".to_string(), "price".to_string()],
                &[vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(1),
                    Value::F64(10.0),
                ]],
            )
            .expect("append insert");
        }

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
        assert!(truncated_len < original_len);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_replay_applies_deletes() {
        let path = temp_wal_path("delete");
        let _ = fs::remove_file(&path);

        let mut mem = MemStore::new();
        {
            let mut wal = Wal::open(&path, &mut mem, true).expect("open wal");
            wal.append_create("ticks", &ticks_schema())
                .expect("append create");
            wal.append_insert(
                "ticks",
                &["symbol".to_string(), "ts".to_string(), "price".to_string()],
                &[vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(1),
                    Value::F64(10.0),
                ]],
            )
            .expect("append insert");
            let filter = Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column("symbol".to_string())),
                right: Box::new(Expr::Literal(Value::String("AAPL".to_string()))),
            };
            wal.append_delete("ticks", Some(&filter))
                .expect("append delete");
        }

        let mut replayed = MemStore::new();
        let _wal = Wal::open(&path, &mut replayed, true).expect("replay wal");
        let plan = crate::query::QueryPlan {
            table: "ticks".to_string(),
            join: None,
            filter: None,
            group_by: Vec::new(),
            select: vec![crate::query::SelectItem {
                expr: crate::query::SelectExpr::Column("price".to_string()),
                alias: None,
            }],
        };
        let result = replayed.query(&plan).expect("query");
        assert!(result.rows.is_empty());

        let _ = fs::remove_file(&path);
    }
}
