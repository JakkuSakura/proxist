use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::pxl::{decode_schema_payload, encode_schema_payload};
use crate::types::{ColumnSpec, ColumnType, Schema, Value};

const SYM_FILE: &str = "sym";

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub root: PathBuf,
    pub partition: String,
}

#[derive(Debug)]
pub struct SplayedStore {
    cfg: StoreConfig,
    sym: SymbolTable,
}

impl SplayedStore {
    pub fn open(cfg: StoreConfig) -> Result<Self> {
        fs::create_dir_all(&cfg.root)?;
        let sym_path = cfg.root.join(SYM_FILE);
        let sym = SymbolTable::open(&sym_path)?;
        Ok(Self { cfg, sym })
    }

    pub fn config(&self) -> &StoreConfig {
        &self.cfg
    }

    pub fn create_table(&mut self, table: &str, schema: &Schema) -> Result<()> {
        let table_dir = self.table_dir(table);
        fs::create_dir_all(&table_dir)?;
        self.write_schema(table, schema)?;
        for column in schema.columns() {
            self.ensure_column_files(&table_dir, &column.name)?;
            if matches!(column.col_type, ColumnType::String | ColumnType::Bytes) {
                self.ensure_offsets_file(&table_dir, &column.name)?;
            }
        }
        Ok(())
    }

    pub fn drop_table(&mut self, table: &str) -> Result<()> {
        let table_dir = self.table_dir(table);
        if table_dir.exists() {
            fs::remove_dir_all(&table_dir)?;
        }
        Ok(())
    }

    pub fn rename_table(&mut self, from: &str, to: &str) -> Result<()> {
        let from_dir = self.table_dir(from);
        let to_dir = self.table_dir(to);
        if !from_dir.exists() {
            return Err(Error::InvalidData(format!(
                "unknown table: {from}"
            )));
        }
        fs::create_dir_all(self.partition_dir())?;
        if to_dir.exists() {
            return Err(Error::InvalidData(format!(
                "target table already exists: {to}"
            )));
        }
        fs::rename(from_dir, to_dir)?;
        Ok(())
    }

    pub fn alter_add_column(&mut self, table: &str, column: &ColumnSpec) -> Result<()> {
        let mut schema = self.read_schema(table)?;
        schema.add_column(column.clone())?;
        self.write_schema(table, &schema)?;
        let table_dir = self.table_dir(table);
        self.ensure_column_files(&table_dir, &column.name)?;
        if matches!(column.col_type, ColumnType::String | ColumnType::Bytes) {
            self.ensure_offsets_file(&table_dir, &column.name)?;
        }
        Ok(())
    }

    pub fn backfill_column(
        &mut self,
        table: &str,
        column: &ColumnSpec,
        rows: usize,
        default: &Value,
    ) -> Result<()> {
        if rows == 0 {
            return Ok(());
        }
        let table_dir = self.table_dir(table);
        let col_path = table_dir.join(&column.name);
        let null_path = table_dir.join(format!("{}.n", column.name));
        let nulls_len = std::fs::metadata(&null_path).map(|m| m.len() as usize).unwrap_or(0);
        let mut nulls = MmapWrite::open(&null_path, nulls_len, 4096)?;
        let is_null = matches!(default, Value::Null);
        match column.col_type {
            ColumnType::I64 => {
                let used = (nulls_len) * std::mem::size_of::<i64>();
                let mut data = MmapWrite::open(&col_path, used, 4096)?;
                for _ in 0..rows {
                    nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                    let v = if is_null {
                        0i64
                    } else if let Value::I64(v) = default {
                        *v
                    } else {
                        0i64
                    };
                    data.append_bytes(&v.to_le_bytes())?;
                }
            }
            ColumnType::F64 => {
                let used = (nulls_len) * std::mem::size_of::<f64>();
                let mut data = MmapWrite::open(&col_path, used, 4096)?;
                for _ in 0..rows {
                    nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                    let v = if is_null {
                        0f64
                    } else if let Value::F64(v) = default {
                        *v
                    } else {
                        0f64
                    };
                    data.append_bytes(&v.to_le_bytes())?;
                }
            }
            ColumnType::Bool => {
                let used = nulls_len;
                let mut data = MmapWrite::open(&col_path, used, 4096)?;
                for _ in 0..rows {
                    nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                    let v = if is_null {
                        0u8
                    } else if let Value::Bool(v) = default {
                        *v as u8
                    } else {
                        0u8
                    };
                    data.append_bytes(&[v])?;
                }
            }
            ColumnType::Timestamp => {
                let used = (nulls_len) * std::mem::size_of::<i64>();
                let mut data = MmapWrite::open(&col_path, used, 4096)?;
                for _ in 0..rows {
                    nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                    let v = if is_null {
                        0i64
                    } else if let Value::Timestamp(v) = default {
                        *v
                    } else if let Value::I64(v) = default {
                        *v
                    } else {
                        0i64
                    };
                    data.append_bytes(&v.to_le_bytes())?;
                }
            }
            ColumnType::Symbol => {
                let used = (nulls_len) * std::mem::size_of::<u32>();
                let mut data = MmapWrite::open(&col_path, used, 4096)?;
                for _ in 0..rows {
                    nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                    let v = if is_null {
                        0u32
                    } else if let Value::String(ref s) = default {
                        self.sym.get_or_insert(s)?
                    } else {
                        0u32
                    };
                    data.append_bytes(&v.to_le_bytes())?;
                }
            }
            ColumnType::String | ColumnType::Bytes => {
                let offsets_path = table_dir.join(format!("{}.o", column.name));
                let offsets_len = std::fs::metadata(&offsets_path).map(|m| m.len() as usize).unwrap_or(0);
                let data_len = std::fs::metadata(&col_path).map(|m| m.len() as usize).unwrap_or(0);
                let mut offsets = MmapWrite::open(&offsets_path, offsets_len, 4096)?;
                let mut data = MmapWrite::open(&col_path, data_len, 4096)?;
                for _ in 0..rows {
                    nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                    let offset = data.len() as u64;
                    offsets.append_bytes(&offset.to_le_bytes())?;
                    let bytes = match default {
                        Value::String(v) => v.as_bytes(),
                        Value::Bytes(v) => v.as_slice(),
                        _ => &[],
                    };
                    let len = if is_null { 0u32 } else { bytes.len() as u32 };
                    data.append_bytes(&len.to_le_bytes())?;
                    if !is_null {
                        data.append_bytes(bytes)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn alter_drop_column(&mut self, table: &str, column: &str) -> Result<()> {
        let mut schema = self.read_schema(table)?;
        schema.remove_column(column)?;
        self.write_schema(table, &schema)?;
        let table_dir = self.table_dir(table);
        let col_path = table_dir.join(column);
        if col_path.exists() {
            let _ = fs::remove_file(&col_path);
        }
        let null_path = table_dir.join(format!("{}.n", column));
        if null_path.exists() {
            let _ = fs::remove_file(&null_path);
        }
        let offsets_path = table_dir.join(format!("{}.o", column));
        if offsets_path.exists() {
            let _ = fs::remove_file(&offsets_path);
        }
        Ok(())
    }

    pub fn alter_rename_column(
        &mut self,
        table: &str,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let mut schema = self.read_schema(table)?;
        schema.rename_column(from, to)?;
        self.write_schema(table, &schema)?;
        let table_dir = self.table_dir(table);
        let from_path = table_dir.join(from);
        let to_path = table_dir.join(to);
        if from_path.exists() {
            fs::rename(from_path, to_path)?;
        }
        let from_null = table_dir.join(format!("{}.n", from));
        let to_null = table_dir.join(format!("{}.n", to));
        if from_null.exists() {
            fs::rename(from_null, to_null)?;
        }
        let from_offsets = table_dir.join(format!("{}.o", from));
        let to_offsets = table_dir.join(format!("{}.o", to));
        if from_offsets.exists() {
            fs::rename(from_offsets, to_offsets)?;
        }
        Ok(())
    }

    pub fn alter_set_default(
        &mut self,
        table: &str,
        column: &str,
        default: Option<Value>,
    ) -> Result<()> {
        let mut schema = self.read_schema(table)?;
        let idx = schema
            .column_index(column)
            .ok_or_else(|| Error::InvalidData(format!("unknown column name: {column}")))?;
        schema.set_column_default(idx, default)?;
        self.write_schema(table, &schema)?;
        Ok(())
    }

    pub fn append_rows(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let schema = self.read_schema(table)?;
        let table_dir = self.table_dir(table);
        let mut value_pos = vec![None; schema.columns().len()];
        for (input_idx, name) in columns.iter().enumerate() {
            let idx = schema
                .column_index(name)
                .ok_or_else(|| Error::InvalidData(format!("unknown column: {name}")))?;
            value_pos[idx] = Some(input_idx);
        }
        let defaults = schema.default_row();
        for (col_idx, column) in schema.columns().iter().enumerate() {
            let col_path = table_dir.join(&column.name);
            let null_path = table_dir.join(format!("{}.n", column.name));
            let nulls_len = std::fs::metadata(&null_path).map(|m| m.len() as usize).unwrap_or(0);
            let mut nulls = MmapWrite::open(&null_path, nulls_len, 4096)?;
            let data_len = std::fs::metadata(&col_path).map(|m| m.len() as usize).unwrap_or(0);
            let default_value = defaults
                .get(col_idx)
                .unwrap_or(&Value::Null);
            match value_pos[col_idx] {
                Some(input_idx) => {
                    match column.col_type {
                        ColumnType::I64 => {
                            let used = nulls_len * std::mem::size_of::<i64>();
                            let mut data = MmapWrite::open(&col_path, used, 4096)?;
                            for row in rows {
                                let value = row
                                    .get(input_idx)
                                    .ok_or_else(|| {
                                        Error::InvalidData("row length mismatch".to_string())
                                    })?;
                                let is_null = matches!(value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::I64(v) = value { *v } else { 0 };
                                data.append_bytes(&v.to_le_bytes())?;
                            }
                        }
                        ColumnType::F64 => {
                            let used = nulls_len * std::mem::size_of::<f64>();
                            let mut data = MmapWrite::open(&col_path, used, 4096)?;
                            for row in rows {
                                let value = row
                                    .get(input_idx)
                                    .ok_or_else(|| {
                                        Error::InvalidData("row length mismatch".to_string())
                                    })?;
                                let is_null = matches!(value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::F64(v) = value { *v } else { 0.0 };
                                data.append_bytes(&v.to_le_bytes())?;
                            }
                        }
                        ColumnType::Bool => {
                            let mut data = MmapWrite::open(&col_path, data_len, 4096)?;
                            for row in rows {
                                let value = row
                                    .get(input_idx)
                                    .ok_or_else(|| {
                                        Error::InvalidData("row length mismatch".to_string())
                                    })?;
                                let is_null = matches!(value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::Bool(v) = value { *v as u8 } else { 0u8 };
                                data.append_bytes(&[v])?;
                            }
                        }
                        ColumnType::Timestamp => {
                            let used = nulls_len * std::mem::size_of::<i64>();
                            let mut data = MmapWrite::open(&col_path, used, 4096)?;
                            for row in rows {
                                let value = row
                                    .get(input_idx)
                                    .ok_or_else(|| {
                                        Error::InvalidData("row length mismatch".to_string())
                                    })?;
                                let is_null = matches!(value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::Timestamp(v) = value { *v } else { 0 };
                                data.append_bytes(&v.to_le_bytes())?;
                            }
                        }
                        ColumnType::Symbol => {
                            let used = nulls_len * std::mem::size_of::<u32>();
                            let mut data = MmapWrite::open(&col_path, used, 4096)?;
                            for row in rows {
                                let value = row
                                    .get(input_idx)
                                    .ok_or_else(|| {
                                        Error::InvalidData("row length mismatch".to_string())
                                    })?;
                                let is_null = matches!(value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::String(v) = value {
                                    self.sym.get_or_insert(v)?
                                } else {
                                    0u32
                                };
                                data.append_bytes(&v.to_le_bytes())?;
                            }
                        }
                        ColumnType::String | ColumnType::Bytes => {
                            let offsets_path = table_dir.join(format!("{}.o", column.name));
                            let offsets_len = std::fs::metadata(&offsets_path)
                                .map(|m| m.len() as usize)
                                .unwrap_or(0);
                            let mut offsets = MmapWrite::open(&offsets_path, offsets_len, 4096)?;
                            let mut data = MmapWrite::open(&col_path, data_len, 4096)?;
                            for row in rows {
                                let value = row
                                    .get(input_idx)
                                    .ok_or_else(|| {
                                        Error::InvalidData("row length mismatch".to_string())
                                    })?;
                                let is_null = matches!(value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let offset = data.len() as u64;
                                offsets.append_bytes(&offset.to_le_bytes())?;
                                let bytes = match value {
                                    Value::String(v) => v.as_bytes(),
                                    Value::Bytes(v) => v.as_slice(),
                                    _ => &[],
                                };
                                let len = if is_null { 0u32 } else { bytes.len() as u32 };
                                data.append_bytes(&len.to_le_bytes())?;
                                if !is_null {
                                    data.append_bytes(bytes)?;
                                }
                            }
                        }
                    }
                }
                None => {
                    match column.col_type {
                        ColumnType::I64 => {
                            let used = nulls_len * std::mem::size_of::<i64>();
                            let mut data = MmapWrite::open(&col_path, used, 4096)?;
                            for _ in rows {
                                let is_null = matches!(default_value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::I64(v) = default_value { *v } else { 0 };
                                data.append_bytes(&v.to_le_bytes())?;
                            }
                        }
                        ColumnType::F64 => {
                            let used = nulls_len * std::mem::size_of::<f64>();
                            let mut data = MmapWrite::open(&col_path, used, 4096)?;
                            for _ in rows {
                                let is_null = matches!(default_value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::F64(v) = default_value { *v } else { 0.0 };
                                data.append_bytes(&v.to_le_bytes())?;
                            }
                        }
                        ColumnType::Bool => {
                            let mut data = MmapWrite::open(&col_path, data_len, 4096)?;
                            for _ in rows {
                                let is_null = matches!(default_value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::Bool(v) = default_value {
                                    *v as u8
                                } else {
                                    0u8
                                };
                                data.append_bytes(&[v])?;
                            }
                        }
                        ColumnType::Timestamp => {
                            let used = nulls_len * std::mem::size_of::<i64>();
                            let mut data = MmapWrite::open(&col_path, used, 4096)?;
                            for _ in rows {
                                let is_null = matches!(default_value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::Timestamp(v) = default_value { *v } else { 0 };
                                data.append_bytes(&v.to_le_bytes())?;
                            }
                        }
                        ColumnType::Symbol => {
                            let used = nulls_len * std::mem::size_of::<u32>();
                            let mut data = MmapWrite::open(&col_path, used, 4096)?;
                            for _ in rows {
                                let is_null = matches!(default_value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let v = if let Value::String(v) = default_value {
                                    self.sym.get_or_insert(v)?
                                } else {
                                    0u32
                                };
                                data.append_bytes(&v.to_le_bytes())?;
                            }
                        }
                        ColumnType::String | ColumnType::Bytes => {
                            let offsets_path = table_dir.join(format!("{}.o", column.name));
                            let offsets_len = std::fs::metadata(&offsets_path)
                                .map(|m| m.len() as usize)
                                .unwrap_or(0);
                            let mut offsets = MmapWrite::open(&offsets_path, offsets_len, 4096)?;
                            let mut data = MmapWrite::open(&col_path, data_len, 4096)?;
                            for _ in rows {
                                let is_null = matches!(default_value, Value::Null);
                                nulls.append_bytes(&[if is_null { 1 } else { 0 }])?;
                                let offset = data.len() as u64;
                                offsets.append_bytes(&offset.to_le_bytes())?;
                                let bytes = match default_value {
                                    Value::String(v) => v.as_bytes(),
                                    Value::Bytes(v) => v.as_slice(),
                                    _ => &[],
                                };
                                let len = if is_null { 0u32 } else { bytes.len() as u32 };
                                data.append_bytes(&len.to_le_bytes())?;
                                if !is_null {
                                    data.append_bytes(bytes)?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn read_schema(&self, table: &str) -> Result<Schema> {
        let path = self.table_dir(table).join(".d");
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let (_table, schema) = decode_schema_payload(&buf)?;
        Ok(schema)
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        let partition = self.partition_dir();
        if !partition.exists() {
            return Ok(tables);
        }
        for entry in fs::read_dir(partition)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                if name.starts_with('.') {
                    continue;
                }
                tables.push(name.to_string());
            }
        }
        Ok(tables)
    }

    pub fn resolve_symbol(&self, id: u32) -> Option<&str> {
        self.sym.lookup(id)
    }

    pub fn symbol_id(&mut self, value: &str) -> Result<u32> {
        self.sym.get_or_insert(value)
    }

    pub fn symbol_values(&self) -> Vec<String> {
        self.sym.values().to_vec()
    }

    fn write_schema(&self, table: &str, schema: &Schema) -> Result<()> {
        let payload = encode_schema_payload(table, schema)?;
        let path = self.table_dir(table).join(".d");
        let mut file = File::create(path)?;
        file.write_all(&payload)?;
        Ok(())
    }

    fn write_value(&mut self, file: &mut File, ty: ColumnType, value: &Value) -> Result<()> {
        match (ty, value) {
            (ColumnType::I64, Value::I64(v)) => {
                file.write_all(&v.to_le_bytes())?;
            }
            (ColumnType::F64, Value::F64(v)) => {
                file.write_all(&v.to_le_bytes())?;
            }
            (ColumnType::Bool, Value::Bool(v)) => {
                file.write_all(&[*v as u8])?;
            }
            (ColumnType::String, Value::String(v)) => {
                self.write_bytes(file, v.as_bytes())?;
            }
            (ColumnType::Bytes, Value::Bytes(v)) => {
                self.write_bytes(file, v)?;
            }
            (ColumnType::Timestamp, Value::Timestamp(v)) => {
                file.write_all(&v.to_le_bytes())?;
            }
            (ColumnType::Symbol, Value::String(v)) => {
                let sym = self.sym.get_or_insert(v)?;
                file.write_all(&sym.to_le_bytes())?;
            }
            _ => {
                return Err(Error::InvalidData(
                    "column type mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn write_null_value(&self, file: &mut File, ty: ColumnType) -> Result<()> {
        match ty {
            ColumnType::I64 => file.write_all(&0i64.to_le_bytes())?,
            ColumnType::F64 => file.write_all(&0f64.to_le_bytes())?,
            ColumnType::Bool => file.write_all(&[0u8])?,
            ColumnType::String | ColumnType::Bytes => self.write_bytes(file, &[])?,
            ColumnType::Timestamp => file.write_all(&0i64.to_le_bytes())?,
            ColumnType::Symbol => file.write_all(&0u32.to_le_bytes())?,
        }
        Ok(())
    }

    fn write_bytes(&self, file: &mut File, bytes: &[u8]) -> Result<()> {
        let len = bytes.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(bytes)?;
        Ok(())
    }

    fn ensure_column_files(&self, table_dir: &Path, name: &str) -> Result<()> {
        let data = table_dir.join(name);
        if !data.exists() {
            let _ = File::create(data)?;
        }
        let nulls = table_dir.join(format!("{}.n", name));
        if !nulls.exists() {
            let _ = File::create(nulls)?;
        }
        Ok(())
    }

    fn ensure_offsets_file(&self, table_dir: &Path, name: &str) -> Result<()> {
        let offsets = table_dir.join(format!("{}.o", name));
        if !offsets.exists() {
            let _ = File::create(offsets)?;
        }
        Ok(())
    }

    pub fn partition_dir(&self) -> PathBuf {
        self.cfg.root.join(&self.cfg.partition)
    }

    pub fn table_dir(&self, table: &str) -> PathBuf {
        self.partition_dir().join(table)
    }
}

#[derive(Debug)]
struct SymbolTable {
    path: PathBuf,
    index: HashMap<String, u32>,
    values: Vec<String>,
}

impl SymbolTable {
    fn open(path: &Path) -> Result<Self> {
        let mut table = Self {
            path: path.to_path_buf(),
            index: HashMap::new(),
            values: Vec::new(),
        };
        if path.exists() {
            table.load()?;
        }
        Ok(table)
    }

    fn load(&mut self) -> Result<()> {
        let mut file = File::open(&self.path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut offset = 0usize;
        while offset + 4 <= buf.len() {
            let len = u32::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]) as usize;
            offset += 4;
            if offset + len > buf.len() {
                break;
            }
            let value = String::from_utf8_lossy(&buf[offset..offset + len]).to_string();
            let id = self.values.len() as u32;
            self.index.insert(value.clone(), id);
            self.values.push(value);
            offset += len;
        }
        Ok(())
    }

    fn get_or_insert(&mut self, value: &str) -> Result<u32> {
        if let Some(id) = self.index.get(value) {
            return Ok(*id);
        }
        let id = self.values.len() as u32;
        self.values.push(value.to_string());
        self.index.insert(value.to_string(), id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let len = value.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(value.as_bytes())?;
        Ok(id)
    }

    fn lookup(&self, id: u32) -> Option<&str> {
        self.values.get(id as usize).map(|value| value.as_str())
    }

    fn values(&self) -> &[String] {
        &self.values
    }
}

#[derive(Debug)]
pub struct MmapView {
    ptr: *const u8,
    len: usize,
    #[cfg(windows)]
    mapping: *mut std::ffi::c_void,
}

#[derive(Debug)]
pub struct MmapWrite {
    file: File,
    ptr: *mut u8,
    len: usize,
    cap: usize,
    #[cfg(windows)]
    mapping: *mut std::ffi::c_void,
}

impl MmapWrite {
    pub fn open(path: &Path, used_len: usize, min_cap: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let current = file.metadata()?.len() as usize;
        let mut cap = current.max(min_cap).max(used_len);
        if cap == 0 {
            cap = 4096;
        }
        if cap != current {
            file.set_len(cap as u64)?;
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let fd = file.as_raw_fd();
            let ptr = unsafe { mmap_read_write(fd, cap)? };
            Ok(Self {
                file,
                ptr,
                len: used_len,
                cap,
                #[cfg(windows)]
                mapping: std::ptr::null_mut(),
            })
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            let handle = file.as_raw_handle();
            let (ptr, mapping) = unsafe { mmap_read_write(handle, cap)? };
            Ok(Self {
                file,
                ptr,
                len: used_len,
                cap,
                mapping,
            })
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn ensure_capacity(&mut self, additional: usize) -> Result<()> {
        let needed = self.len.saturating_add(additional);
        if needed <= self.cap {
            return Ok(());
        }
        let mut new_cap = self.cap.max(4096);
        while new_cap < needed {
            new_cap = new_cap.saturating_mul(2);
        }
        self.remap(new_cap)
    }

    pub fn write_at(&mut self, offset: usize, bytes: &[u8]) -> Result<()> {
        if offset.saturating_add(bytes.len()) > self.cap {
            let additional = offset
                .saturating_add(bytes.len())
                .saturating_sub(self.len);
            self.ensure_capacity(additional)?;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.add(offset), bytes.len());
        }
        let end = offset.saturating_add(bytes.len());
        if end > self.len {
            self.len = end;
        }
        Ok(())
    }

    pub fn append_bytes(&mut self, bytes: &[u8]) -> Result<usize> {
        let offset = self.len;
        self.ensure_capacity(bytes.len())?;
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.add(self.len), bytes.len());
        }
        self.len += bytes.len();
        Ok(offset)
    }

    fn remap(&mut self, new_cap: usize) -> Result<()> {
        if new_cap == self.cap {
            return Ok(());
        }
        if new_cap < self.len {
            return Err(Error::InvalidData("mmap shrink below len".to_string()));
        }
        #[cfg(unix)]
        unsafe {
            let _ = munmap_region(self.ptr as *mut std::ffi::c_void, self.cap);
        }
        #[cfg(windows)]
        unsafe {
            let _ = unmap_view(self.ptr as *mut std::ffi::c_void);
            if !self.mapping.is_null() {
                let _ = close_handle(self.mapping);
            }
        }
        self.file.set_len(new_cap as u64)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let fd = self.file.as_raw_fd();
            let ptr = unsafe { mmap_read_write(fd, new_cap)? };
            self.ptr = ptr;
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            let handle = self.file.as_raw_handle();
            let (ptr, mapping) = unsafe { mmap_read_write(handle, new_cap)? };
            self.ptr = ptr;
            self.mapping = mapping;
        }
        self.cap = new_cap;
        Ok(())
    }
}

unsafe impl Send for MmapWrite {}
unsafe impl Sync for MmapWrite {}

impl Drop for MmapWrite {
    fn drop(&mut self) {
        if self.cap == 0 || self.ptr.is_null() {
            return;
        }
        #[cfg(unix)]
        unsafe {
            let _ = munmap_region(self.ptr as *mut std::ffi::c_void, self.cap);
        }
        #[cfg(windows)]
        unsafe {
            let _ = unmap_view(self.ptr as *mut std::ffi::c_void);
            if !self.mapping.is_null() {
                let _ = close_handle(self.mapping);
            }
        }
        if self.len < self.cap {
            let _ = self.file.set_len(self.len as u64);
        }
    }
}

impl MmapView {
    pub fn read_only(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(Self {
                ptr: std::ptr::null(),
                len: 0,
                #[cfg(windows)]
                mapping: std::ptr::null_mut(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let ptr = unsafe { mmap_read_only(fd, len)? };
            return Ok(Self { ptr, len });
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            let handle = file.as_raw_handle();
            let (ptr, mapping) = unsafe { mmap_read_only(handle, len)? };
            return Ok(Self { ptr, len, mapping });
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

unsafe impl Send for MmapView {}
unsafe impl Sync for MmapView {}

impl Drop for MmapView {
    fn drop(&mut self) {
        if self.len == 0 || self.ptr.is_null() {
            return;
        }
        #[cfg(unix)]
        unsafe {
            let _ = munmap_region(self.ptr as *mut std::ffi::c_void, self.len);
        }
        #[cfg(windows)]
        unsafe {
            let _ = unmap_view(self.ptr as *mut std::ffi::c_void);
            if !self.mapping.is_null() {
                let _ = close_handle(self.mapping);
            }
        }
    }
}

#[cfg(unix)]
unsafe fn mmap_read_only(fd: std::os::fd::RawFd, len: usize) -> Result<*const u8> {
    const PROT_READ: i32 = 0x1;
    const MAP_PRIVATE: i32 = 0x02;
    let ptr = mmap(
        std::ptr::null_mut(),
        len,
        PROT_READ,
        MAP_PRIVATE,
        fd,
        0,
    );
    if ptr == MAP_FAILED {
        return Err(Error::InvalidData("mmap failed".to_string()));
    }
    Ok(ptr as *const u8)
}

#[cfg(unix)]
const MAP_FAILED: *mut std::ffi::c_void = !0 as *mut std::ffi::c_void;

#[cfg(unix)]
unsafe fn mmap_read_write(fd: std::os::fd::RawFd, len: usize) -> Result<*mut u8> {
    const PROT_READ: i32 = 0x1;
    const PROT_WRITE: i32 = 0x2;
    const MAP_SHARED: i32 = 0x01;
    let ptr = mmap(
        std::ptr::null_mut(),
        len,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd,
        0,
    );
    if ptr == MAP_FAILED {
        return Err(Error::InvalidData("mmap failed".to_string()));
    }
    Ok(ptr as *mut u8)
}

#[cfg(unix)]
extern "C" {
    fn mmap(
        addr: *mut std::ffi::c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: std::os::fd::RawFd,
        offset: i64,
    ) -> *mut std::ffi::c_void;
    fn munmap(addr: *mut std::ffi::c_void, len: usize) -> i32;
}

#[cfg(unix)]
unsafe fn munmap_region(addr: *mut std::ffi::c_void, len: usize) -> Result<()> {
    if munmap(addr, len) != 0 {
        return Err(Error::InvalidData("munmap failed".to_string()));
    }
    Ok(())
}

#[cfg(windows)]
unsafe fn mmap_read_only(
    handle: std::os::windows::raw::HANDLE,
    len: usize,
) -> Result<(*const u8, *mut std::ffi::c_void)> {
    let mapping = CreateFileMappingW(handle, std::ptr::null_mut(), PAGE_READONLY, 0, 0, std::ptr::null());
    if mapping.is_null() {
        return Err(Error::InvalidData("CreateFileMappingW failed".to_string()));
    }
    let ptr = MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, len);
    if ptr.is_null() {
        let _ = CloseHandle(mapping);
        return Err(Error::InvalidData("MapViewOfFile failed".to_string()));
    }
    Ok((ptr as *const u8, mapping))
}

#[cfg(windows)]
unsafe fn unmap_view(ptr: *mut std::ffi::c_void) -> Result<()> {
    if UnmapViewOfFile(ptr) == 0 {
        return Err(Error::InvalidData("UnmapViewOfFile failed".to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnSpec, ColumnType, Schema, Value};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        path.push(format!("pxd_store_{label}_{nanos}"));
        path
    }

    #[test]
    fn backfill_column_writes_defaults() {
        let root = temp_root("backfill");
        let cfg = StoreConfig {
            root: root.clone(),
            partition: "p".to_string(),
        };
        let mut store = SplayedStore::open(cfg).expect("open");
        let schema = Schema::new(vec![ColumnSpec {
            name: "price".to_string(),
            col_type: ColumnType::F64,
            nullable: false,
            default: None,
        }])
        .expect("schema");
        store.create_table("ticks", &schema).expect("create");
        store
            .append_rows(
                "ticks",
                &["price".to_string()],
                &[
                    vec![Value::F64(1.0)],
                    vec![Value::F64(2.0)],
                ],
            )
            .expect("append");
        let spec = ColumnSpec {
            name: "lot".to_string(),
            col_type: ColumnType::I64,
            nullable: true,
            default: Some(Value::I64(100)),
        };
        store.alter_add_column("ticks", &spec).expect("alter");
        store
            .backfill_column("ticks", &spec, 2, &Value::I64(100))
            .expect("backfill");

        let col_path = store.table_dir("ticks").join("lot");
        let null_path = store.table_dir("ticks").join("lot.n");
        let col_bytes = fs::read(col_path).expect("read col");
        let null_bytes = fs::read(null_path).expect("read nulls");
        assert_eq!(null_bytes.len(), 2);
        assert_eq!(col_bytes.len(), 16);
        let first = i64::from_le_bytes([
            col_bytes[0],
            col_bytes[1],
            col_bytes[2],
            col_bytes[3],
            col_bytes[4],
            col_bytes[5],
            col_bytes[6],
            col_bytes[7],
        ]);
        assert_eq!(first, 100);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn symbol_table_persists() {
        let root = temp_root("sym");
        {
            let cfg = StoreConfig {
                root: root.clone(),
                partition: "p".to_string(),
            };
            let mut store = SplayedStore::open(cfg).expect("open");
            let schema = Schema::new(vec![ColumnSpec {
                name: "sym".to_string(),
                col_type: ColumnType::Symbol,
                nullable: false,
                default: None,
            }])
            .expect("schema");
            store.create_table("ticks", &schema).expect("create");
            store
                .append_rows(
                    "ticks",
                    &["sym".to_string()],
                    &[vec![Value::String("AAPL".to_string())]],
                )
                .expect("append");
        }

        let cfg = StoreConfig {
            root: root.clone(),
            partition: "p".to_string(),
        };
        let store = SplayedStore::open(cfg).expect("reopen");
        let col_path = store.table_dir("ticks").join("sym");
        let raw = fs::read(col_path).expect("read sym col");
        assert!(raw.len() >= 4);
        let id = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
        assert_eq!(store.resolve_symbol(id), Some("AAPL"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn mmap_view_reads_bytes() {
        let root = temp_root("mmap");
        let file_path = root.join("data");
        fs::create_dir_all(&root).expect("mkdir");
        fs::write(&file_path, &[1u8, 2u8, 3u8, 4u8]).expect("write");
        let view = MmapView::read_only(&file_path).expect("mmap");
        assert_eq!(view.as_slice(), &[1u8, 2u8, 3u8, 4u8]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn mmap_write_grows_and_persists_len() {
        let root = temp_root("mmapgrow");
        fs::create_dir_all(&root).expect("mkdir");
        let path = root.join("data.bin");
        {
            let mut view = MmapWrite::open(&path, 0, 16).expect("open");
            let payload = vec![7u8; 8192];
            view.append_bytes(&payload).expect("append");
            assert_eq!(view.len(), payload.len());
        }
        let meta = fs::metadata(&path).expect("meta");
        assert!(meta.len() as usize >= 8192);
        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(windows)]
unsafe fn close_handle(handle: *mut std::ffi::c_void) -> Result<()> {
    if CloseHandle(handle) == 0 {
        return Err(Error::InvalidData("CloseHandle failed".to_string()));
    }
    Ok(())
}

#[cfg(windows)]
type BOOL = i32;
#[cfg(windows)]
type DWORD = u32;

#[cfg(windows)]
const PAGE_READONLY: DWORD = 0x02;
#[cfg(windows)]
const PAGE_READWRITE: DWORD = 0x04;
#[cfg(windows)]
const FILE_MAP_READ: DWORD = 0x0004;
#[cfg(windows)]
const FILE_MAP_WRITE: DWORD = 0x0002;

#[cfg(windows)]
extern "system" {
    fn CreateFileMappingW(
        file: std::os::windows::raw::HANDLE,
        attrs: *mut std::ffi::c_void,
        protect: DWORD,
        max_size_high: DWORD,
        max_size_low: DWORD,
        name: *const u16,
    ) -> *mut std::ffi::c_void;
    fn MapViewOfFile(
        mapping: *mut std::ffi::c_void,
        access: DWORD,
        offset_high: DWORD,
        offset_low: DWORD,
        bytes: usize,
    ) -> *mut std::ffi::c_void;
    fn UnmapViewOfFile(address: *mut std::ffi::c_void) -> BOOL;
    fn CloseHandle(handle: *mut std::ffi::c_void) -> BOOL;
}

#[cfg(windows)]
unsafe fn mmap_read_write(
    handle: std::os::windows::raw::HANDLE,
    len: usize,
) -> Result<(*mut u8, *mut std::ffi::c_void)> {
    let mapping = CreateFileMappingW(handle, std::ptr::null_mut(), PAGE_READWRITE, 0, 0, std::ptr::null());
    if mapping.is_null() {
        return Err(Error::InvalidData("CreateFileMappingW failed".to_string()));
    }
    let ptr = MapViewOfFile(mapping, FILE_MAP_READ | FILE_MAP_WRITE, 0, 0, len);
    if ptr.is_null() {
        let _ = CloseHandle(mapping);
        return Err(Error::InvalidData("MapViewOfFile failed".to_string()));
    }
    Ok((ptr as *mut u8, mapping))
}
