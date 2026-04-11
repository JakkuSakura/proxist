use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::Result;
use crate::expr::Expr;
use crate::memstore::MemStore;
use crate::pxl::{
    decode_alter_add_column_payload, decode_alter_drop_column_payload,
    decode_alter_rename_column_payload, decode_alter_set_default_payload,
    decode_delete_payload, decode_drop_table_payload, decode_insert_payload,
    decode_rename_table_payload, decode_schema_payload, decode_update_payload,
    encode_alter_add_column_payload, encode_alter_drop_column_payload,
    encode_alter_rename_column_payload, encode_alter_set_default_payload,
    encode_delete_payload, encode_drop_table_payload, encode_insert_payload,
    encode_rename_table_payload, encode_schema_payload, encode_update_payload, Op,
};
use crate::types::{ColumnSpec, Schema, Value};

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

    pub fn append_alter_add_column(
        &mut self,
        table: &str,
        column: &ColumnSpec,
    ) -> Result<u64> {
        let payload = encode_alter_add_column_payload(table, column)?;
        self.append_record(Op::AlterAddColumn, &payload)
    }

    pub fn append_alter_drop_column(&mut self, table: &str, column: &str) -> Result<u64> {
        let payload = encode_alter_drop_column_payload(table, column)?;
        self.append_record(Op::AlterDropColumn, &payload)
    }

    pub fn append_alter_rename_column(
        &mut self,
        table: &str,
        from: &str,
        to: &str,
    ) -> Result<u64> {
        let payload = encode_alter_rename_column_payload(table, from, to)?;
        self.append_record(Op::AlterRenameColumn, &payload)
    }

    pub fn append_alter_set_default(
        &mut self,
        table: &str,
        column: &str,
        default: Option<&Value>,
    ) -> Result<u64> {
        let payload = encode_alter_set_default_payload(table, column, default)?;
        self.append_record(Op::AlterSetDefault, &payload)
    }

    pub fn append_drop_table(&mut self, table: &str) -> Result<u64> {
        let payload = encode_drop_table_payload(table)?;
        self.append_record(Op::DropTable, &payload)
    }

    pub fn append_rename_table(&mut self, from: &str, to: &str) -> Result<u64> {
        let payload = encode_rename_table_payload(from, to)?;
        self.append_record(Op::RenameTable, &payload)
    }

    pub fn checkpoint(&mut self, mem: &mut MemStore) -> Result<()> {
        mem.flush_to_store()?;
        self.truncate()
    }

    fn truncate(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        if self.sync {
            self.file.flush()?;
            self.file.sync_all()?;
        }
        self.next_lsn = 1;
        Ok(())
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
            Op::AlterAddColumn => {
                let (table, column) = decode_alter_add_column_payload(&payload)?;
                mem.alter_add_column(&table, column)?;
            }
            Op::AlterDropColumn => {
                let (table, column) = decode_alter_drop_column_payload(&payload)?;
                mem.alter_drop_column(&table, &column)?;
            }
            Op::AlterRenameColumn => {
                let (table, from, to) = decode_alter_rename_column_payload(&payload)?;
                mem.alter_rename_column(&table, &from, &to)?;
            }
            Op::AlterSetDefault => {
                let (table, column, default) = decode_alter_set_default_payload(&payload)?;
                mem.alter_set_default(&table, &column, default)?;
            }
            Op::DropTable => {
                let table = decode_drop_table_payload(&payload)?;
                mem.drop_table(&table)?;
            }
            Op::RenameTable => {
                let (from, to) = decode_rename_table_payload(&payload)?;
                mem.rename_table(&from, &to)?;
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
    use crate::pxl::{ColumnProjectExpr, ColumnProjectItem, ColumnQuery};
    use crate::storage::{SplayedStore, StoreConfig};
    use crate::types::{ColumnBlock, ColumnBlockData, ColumnSpec, ColumnType, Schema, Value};

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

    fn temp_root(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        path.push(format!("pxd_store_{label}_{nanos}"));
        path
    }

    fn ticks_schema() -> Schema {
        Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
                nullable: false,
                default: None,
            },
            ColumnSpec {
                name: "ts".to_string(),
                col_type: ColumnType::I64,
                nullable: false,
                default: None,
            },
            ColumnSpec {
                name: "price".to_string(),
                col_type: ColumnType::F64,
                nullable: false,
                default: None,
            },
        ])
        .expect("schema")
    }

    fn block_column_i64(block: &ColumnBlock, name: &str) -> Vec<i64> {
        let idx = block.schema.column_index(name).expect("column index");
        match &block.columns[idx].data {
            ColumnBlockData::I64(values) => values.clone(),
            _ => panic!("expected i64 column"),
        }
    }

    fn block_column_string(block: &ColumnBlock, name: &str) -> Vec<String> {
        let idx = block.schema.column_index(name).expect("column index");
        match &block.columns[idx].data {
            ColumnBlockData::String(values) => values.clone(),
            _ => panic!("expected string column"),
        }
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
        assert_eq!(replayed.table_row_count("ticks"), Some(1));

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
        assert_eq!(replayed.table_row_count("ticks"), Some(0));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_replay_applies_ddl() {
        let path = temp_wal_path("ddl");
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
            wal.append_alter_add_column(
                "ticks",
                &ColumnSpec {
                    name: "lot".to_string(),
                    col_type: ColumnType::I64,
                    nullable: false,
                    default: Some(Value::I64(100)),
                },
            )
            .expect("append alter add");
            wal.append_alter_set_default("ticks", "price", Some(&Value::I64(7)))
                .expect("append alter default");
            wal.append_alter_rename_column("ticks", "lot", "lots")
                .expect("append rename column");
            wal.append_alter_drop_column("ticks", "ts")
                .expect("append drop column");
            wal.append_rename_table("ticks", "trades")
                .expect("append rename table");

        }

        let mut replayed = MemStore::new();
        let _wal = Wal::open(&path, &mut replayed, true).expect("replay wal");

        let schema = replayed.table_schema("trades").expect("schema");
        assert!(schema.column_index("ts").is_none());
        assert!(schema.column_index("lot").is_none());
        assert!(schema.column_index("lots").is_some());
        let price_idx = schema.column_index("price").expect("price idx");
        assert_eq!(schema.columns()[price_idx].default, Some(Value::F64(7.0)));

        let query = ColumnQuery {
            table: "trades".to_string(),
            columns: vec!["lots".to_string()],
            filter: Vec::new(),
            project: vec![ColumnProjectItem {
                name: "lots".to_string(),
                expr: ColumnProjectExpr::Column(0),
            }],
        };
        let result = replayed.query_col(&query).expect("query_col");
        let lots = block_column_i64(&result, "lots");
        assert_eq!(lots[0], 100);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_checkpoint_flushes_to_store_and_truncates() {
        let root = temp_root("checkpoint");
        let cfg = StoreConfig {
            root: root.clone(),
            partition: "p".to_string(),
        };
        let store = SplayedStore::open(cfg.clone()).expect("open store");
        let mut mem = MemStore::with_store(store);
        let path = temp_wal_path("checkpoint");
        let _ = fs::remove_file(&path);

        {
            let mut wal = Wal::open(&path, &mut mem, true).expect("open wal");
            wal.append_create("ticks", &ticks_schema())
                .expect("append create");
            mem.create_table("ticks", ticks_schema()).expect("create");
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
            mem.insert(
                "ticks",
                &["symbol".to_string(), "ts".to_string(), "price".to_string()],
                &[vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(1),
                    Value::F64(10.0),
                ]],
            )
            .expect("insert");
            wal.checkpoint(&mut mem).expect("checkpoint");
        }

        let wal_len = fs::metadata(&path).expect("metadata").len();
        assert_eq!(wal_len, 0);

        let store = SplayedStore::open(cfg).expect("reopen store");
        let mut reloaded = MemStore::with_store(store);
        reloaded.load_from_store().expect("load");
        let query = ColumnQuery {
            table: "ticks".to_string(),
            columns: vec!["symbol".to_string()],
            filter: Vec::new(),
            project: vec![ColumnProjectItem {
                name: "symbol".to_string(),
                expr: ColumnProjectExpr::Column(0),
            }],
        };
        let result = reloaded.query_col(&query).expect("query_col");
        let symbols = block_column_string(&result, "symbol");
        assert_eq!(symbols[0], "AAPL");

        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir_all(root);
    }
}
