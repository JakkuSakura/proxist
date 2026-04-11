use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, Write};
use std::mem::MaybeUninit;
use std::path::Path;
use std::slice;
use std::sync::{Arc, OnceLock};

use crate::error::{Error, Result};
use crate::expr::{compare_values, eval_predicate_at, ColumnAccess, Expr};
use crate::pxl::{ColumnInstr, ColumnProjectExpr, ColumnQuery};
use crate::storage::{MmapView, MmapWrite, SplayedStore};
use crate::types::{coerce_value, ColumnSpec, ColumnType, Row, Schema, Value, ValueRef};

#[derive(Debug)]
struct Table {
    schema: Schema,
    columns: Vec<ColumnData>,
    row_count: usize,
    symbols: SymbolInterner,
}

#[derive(Debug, Default)]
pub struct SymbolInterner {
    index: HashMap<String, u32>,
    values: Vec<Arc<str>>,
}

impl SymbolInterner {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            values: Vec::new(),
        }
    }

    fn from_values(values: Vec<String>) -> Self {
        let mut index = HashMap::with_capacity(values.len().saturating_mul(2));
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let arc: Arc<str> = Arc::from(value.as_str());
            let id = out.len() as u32;
            index.insert(value, id);
            out.push(arc);
        }
        Self { index, values: out }
    }

    fn resolve(&self, id: u32) -> Option<&str> {
        self.values.get(id as usize).map(|value| value.as_ref())
    }

    fn intern(&mut self, value: &str) -> u32 {
        if let Some(id) = self.index.get(value) {
            return *id;
        }
        let arc: Arc<str> = Arc::from(value);
        let id = self.values.len() as u32;
        self.values.push(arc);
        self.index.insert(value.to_string(), id);
        id
    }

    fn intern_with_id(&mut self, value: &str, id: u32) -> u32 {
        if let Some(existing) = self.index.get(value) {
            return *existing;
        }
        let arc: Arc<str> = Arc::from(value);
        if self.values.len() <= id as usize {
            self.values.resize_with(id as usize + 1, empty_symbol);
        }
        self.values[id as usize] = arc;
        self.index.insert(value.to_string(), id);
        id
    }
}

#[derive(Debug)]
struct ColumnData {
    ty: ColumnType,
    nulls: NullsStorage,
    data: ColumnStorage,
}

#[derive(Debug)]
enum ColumnStorage {
    I64(Vec<i64>),
    F64(Vec<f64>),
    Bool(Vec<u8>),
    String(Vec<String>),
    Bytes(Vec<Vec<u8>>),
    Timestamp(Vec<i64>),
    Symbol(Vec<u32>),
    MmapI64(MmapView),
    MmapF64(MmapView),
    MmapBool(MmapView),
    MmapTimestamp(MmapView),
    MmapI64W(MmapWrite),
    MmapF64W(MmapWrite),
    MmapBoolW(MmapWrite),
    MmapTimestampW(MmapWrite),
    MmapSymbolW(MmapWrite),
    MmapVarStringW { offsets: MmapWrite, data: MmapWrite },
    MmapVarBytesW { offsets: MmapWrite, data: MmapWrite },
}

#[derive(Debug)]
enum NullsStorage {
    Owned(Vec<u8>),
    Mapped(MmapView),
    Writable(MmapWrite),
}

impl NullsStorage {
    fn len(&self) -> usize {
        match self {
            NullsStorage::Owned(values) => values.len(),
            NullsStorage::Mapped(view) => view.len(),
            NullsStorage::Writable(view) => view.len(),
        }
    }

    fn get(&self, idx: usize) -> Option<u8> {
        match self {
            NullsStorage::Owned(values) => values.get(idx).copied(),
            NullsStorage::Mapped(view) => view.as_slice().get(idx).copied(),
            NullsStorage::Writable(view) => view.as_slice().get(idx).copied(),
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            NullsStorage::Owned(values) => values.as_slice(),
            NullsStorage::Mapped(view) => view.as_slice(),
            NullsStorage::Writable(view) => view.as_slice(),
        }
    }

    fn ensure_owned(&mut self) {
        match self {
            NullsStorage::Mapped(view) => {
                let mut values = Vec::with_capacity(view.len());
                values.extend_from_slice(view.as_slice());
                *self = NullsStorage::Owned(values);
            }
            NullsStorage::Writable(view) => {
                let mut values = Vec::with_capacity(view.len());
                values.extend_from_slice(view.as_slice());
                *self = NullsStorage::Owned(values);
            }
            _ => {}
        }
    }

    fn push(&mut self, value: u8) {
        match self {
            NullsStorage::Owned(values) => values.push(value),
            NullsStorage::Writable(view) => {
                let _ = view.append_bytes(&[value]);
            }
            NullsStorage::Mapped(_) => self.ensure_owned(),
        }
    }

    fn reserve(&mut self, additional: usize) {
        match self {
            NullsStorage::Owned(values) => values.reserve(additional),
            NullsStorage::Writable(view) => {
                let _ = view.ensure_capacity(additional);
            }
            NullsStorage::Mapped(_) => self.ensure_owned(),
        }
    }

    fn set(&mut self, idx: usize, value: u8) {
        match self {
            NullsStorage::Owned(values) => values[idx] = value,
            NullsStorage::Writable(view) => {
                let _ = view.write_at(idx, &[value]);
            }
            NullsStorage::Mapped(_) => self.ensure_owned(),
        }
    }
}

fn empty_symbol() -> Arc<str> {
    static EMPTY: OnceLock<Arc<str>> = OnceLock::new();
    EMPTY.get_or_init(|| Arc::from("")).clone()
}

impl ColumnData {
    fn new(ty: ColumnType) -> Self {
        let data = match ty {
            ColumnType::I64 => ColumnStorage::I64(Vec::new()),
            ColumnType::F64 => ColumnStorage::F64(Vec::new()),
            ColumnType::Bool => ColumnStorage::Bool(Vec::new()),
            ColumnType::String => ColumnStorage::String(Vec::new()),
            ColumnType::Bytes => ColumnStorage::Bytes(Vec::new()),
            ColumnType::Timestamp => ColumnStorage::Timestamp(Vec::new()),
            ColumnType::Symbol => ColumnStorage::Symbol(Vec::new()),
        };
        Self {
            ty,
            nulls: NullsStorage::Owned(Vec::new()),
            data,
        }
    }

    fn new_mmap(ty: ColumnType, data: ColumnStorage, nulls: NullsStorage) -> Self {
        Self { ty, nulls, data }
    }

    fn len(&self) -> usize {
        match &self.data {
            ColumnStorage::I64(values) => values.len(),
            ColumnStorage::F64(values) => values.len(),
            ColumnStorage::Bool(values) => values.len(),
            ColumnStorage::String(values) => values.len(),
            ColumnStorage::Bytes(values) => values.len(),
            ColumnStorage::Timestamp(values) => values.len(),
            ColumnStorage::Symbol(values) => values.len(),
            ColumnStorage::MmapI64(view) => view.len() / std::mem::size_of::<i64>(),
            ColumnStorage::MmapF64(view) => view.len() / std::mem::size_of::<f64>(),
            ColumnStorage::MmapBool(view) => view.len(),
            ColumnStorage::MmapTimestamp(view) => view.len() / std::mem::size_of::<i64>(),
            ColumnStorage::MmapI64W(view) => view.len() / std::mem::size_of::<i64>(),
            ColumnStorage::MmapF64W(view) => view.len() / std::mem::size_of::<f64>(),
            ColumnStorage::MmapBoolW(view) => view.len(),
            ColumnStorage::MmapTimestampW(view) => view.len() / std::mem::size_of::<i64>(),
            ColumnStorage::MmapSymbolW(view) => view.len() / std::mem::size_of::<u32>(),
            ColumnStorage::MmapVarStringW { offsets, .. } => offsets.len() / 8,
            ColumnStorage::MmapVarBytesW { offsets, .. } => offsets.len() / 8,
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.nulls.reserve(additional);
        match &mut self.data {
            ColumnStorage::I64(values) => values.reserve(additional),
            ColumnStorage::F64(values) => values.reserve(additional),
            ColumnStorage::Bool(values) => values.reserve(additional),
            ColumnStorage::String(values) => values.reserve(additional),
            ColumnStorage::Bytes(values) => values.reserve(additional),
            ColumnStorage::Timestamp(values) => values.reserve(additional),
            ColumnStorage::Symbol(values) => values.reserve(additional),
            ColumnStorage::MmapI64(_) => self.materialize(),
            ColumnStorage::MmapF64(_) => self.materialize(),
            ColumnStorage::MmapBool(_) => self.materialize(),
            ColumnStorage::MmapTimestamp(_) => self.materialize(),
            ColumnStorage::MmapI64W(view) => {
                let _ = view.ensure_capacity(additional * std::mem::size_of::<i64>());
            }
            ColumnStorage::MmapF64W(view) => {
                let _ = view.ensure_capacity(additional * std::mem::size_of::<f64>());
            }
            ColumnStorage::MmapBoolW(view) => {
                let _ = view.ensure_capacity(additional);
            }
            ColumnStorage::MmapTimestampW(view) => {
                let _ = view.ensure_capacity(additional * std::mem::size_of::<i64>());
            }
            ColumnStorage::MmapSymbolW(view) => {
                let _ = view.ensure_capacity(additional * std::mem::size_of::<u32>());
            }
            ColumnStorage::MmapVarStringW { offsets, .. } => {
                let _ = offsets.ensure_capacity(additional * 8);
            }
            ColumnStorage::MmapVarBytesW { offsets, .. } => {
                let _ = offsets.ensure_capacity(additional * 8);
            }
        }
    }

    fn fill_with_default(&mut self, default_value: &Value, rows: usize) -> Result<()> {
        if rows == 0 {
            return Ok(());
        }
        self.reserve(rows);
        for _ in 0..rows {
            let value = if default_value.is_null() {
                Value::Null
            } else {
                default_value.clone()
            };
            let value = coerce_value(value, self.ty)?;
            self.push_value(value)?;
        }
        Ok(())
    }

    fn push_value(&mut self, value: Value) -> Result<()> {
        self.ensure_owned_data();
        match (self.ty, value) {
            (_, Value::Null) => {
                self.nulls.push(1);
                self.push_default_value();
                Ok(())
            }
            (ColumnType::I64, Value::I64(v)) => {
                self.nulls.push(0);
                match &mut self.data {
                    ColumnStorage::I64(values) => values.push(v),
                    ColumnStorage::MmapI64W(view) => {
                        view.append_bytes(&v.to_le_bytes())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::F64, Value::F64(v)) => {
                self.nulls.push(0);
                match &mut self.data {
                    ColumnStorage::F64(values) => values.push(v),
                    ColumnStorage::MmapF64W(view) => {
                        view.append_bytes(&v.to_le_bytes())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::Bool, Value::Bool(v)) => {
                self.nulls.push(0);
                let byte = if v { 1 } else { 0 };
                match &mut self.data {
                    ColumnStorage::Bool(values) => values.push(byte),
                    ColumnStorage::MmapBoolW(view) => {
                        view.append_bytes(&[byte])?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::String, Value::String(v)) => {
                self.nulls.push(0);
                match &mut self.data {
                    ColumnStorage::String(values) => values.push(v),
                    ColumnStorage::MmapVarStringW { offsets, data } => {
                        let offset = data.len() as u64;
                        offsets.append_bytes(&offset.to_le_bytes())?;
                        let len = v.len() as u32;
                        data.append_bytes(&len.to_le_bytes())?;
                        data.append_bytes(v.as_bytes())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::Symbol, Value::String(_)) => Err(Error::InvalidData(
                "symbol requires interner".to_string(),
            )),
            (ColumnType::Bytes, Value::Bytes(v)) => {
                self.nulls.push(0);
                match &mut self.data {
                    ColumnStorage::Bytes(values) => values.push(v),
                    ColumnStorage::MmapVarBytesW { offsets, data } => {
                        let offset = data.len() as u64;
                        offsets.append_bytes(&offset.to_le_bytes())?;
                        let len = v.len() as u32;
                        data.append_bytes(&len.to_le_bytes())?;
                        data.append_bytes(&v)?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::Timestamp, Value::Timestamp(v)) => {
                self.nulls.push(0);
                match &mut self.data {
                    ColumnStorage::Timestamp(values) => values.push(v),
                    ColumnStorage::MmapTimestampW(view) => {
                        view.append_bytes(&v.to_le_bytes())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            _ => Err(Error::InvalidData("column type mismatch".to_string())),
        }
    }

    fn set_value(&mut self, idx: usize, value: Value) -> Result<()> {
        if idx >= self.len() {
            return Err(Error::InvalidData("column index out of range".to_string()));
        }
        self.ensure_owned_data();
        match (self.ty, value) {
            (_, Value::Null) => {
                self.nulls.set(idx, 1);
                self.set_default_value(idx);
                Ok(())
            }
            (ColumnType::I64, Value::I64(v)) => {
                self.nulls.set(idx, 0);
                match &mut self.data {
                    ColumnStorage::I64(values) => values[idx] = v,
                    ColumnStorage::MmapI64W(view) => {
                        let offset = idx * std::mem::size_of::<i64>();
                        view.write_at(offset, &v.to_le_bytes())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::F64, Value::F64(v)) => {
                self.nulls.set(idx, 0);
                match &mut self.data {
                    ColumnStorage::F64(values) => values[idx] = v,
                    ColumnStorage::MmapF64W(view) => {
                        let offset = idx * std::mem::size_of::<f64>();
                        view.write_at(offset, &v.to_le_bytes())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::Bool, Value::Bool(v)) => {
                self.nulls.set(idx, 0);
                let byte = if v { 1 } else { 0 };
                match &mut self.data {
                    ColumnStorage::Bool(values) => values[idx] = byte,
                    ColumnStorage::MmapBoolW(view) => {
                        view.write_at(idx, &[byte])?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::String, Value::String(v)) => {
                self.nulls.set(idx, 0);
                match &mut self.data {
                    ColumnStorage::String(values) => values[idx] = v,
                    ColumnStorage::MmapVarStringW { offsets, data } => {
                        let offset = data.len() as u64;
                        offsets.write_at(idx * 8, &offset.to_le_bytes())?;
                        let len = v.len() as u32;
                        data.append_bytes(&len.to_le_bytes())?;
                        data.append_bytes(v.as_bytes())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::Symbol, Value::String(_)) => Err(Error::InvalidData(
                "symbol requires interner".to_string(),
            )),
            (ColumnType::Bytes, Value::Bytes(v)) => {
                self.nulls.set(idx, 0);
                match &mut self.data {
                    ColumnStorage::Bytes(values) => values[idx] = v,
                    ColumnStorage::MmapVarBytesW { offsets, data } => {
                        let offset = data.len() as u64;
                        offsets.write_at(idx * 8, &offset.to_le_bytes())?;
                        let len = v.len() as u32;
                        data.append_bytes(&len.to_le_bytes())?;
                        data.append_bytes(&v)?;
                    }
                    _ => {}
                }
                Ok(())
            }
            (ColumnType::Timestamp, Value::Timestamp(v)) => {
                self.nulls.set(idx, 0);
                match &mut self.data {
                    ColumnStorage::Timestamp(values) => values[idx] = v,
                    ColumnStorage::MmapTimestampW(view) => {
                        let offset = idx * std::mem::size_of::<i64>();
                        view.write_at(offset, &v.to_le_bytes())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            _ => Err(Error::InvalidData("column type mismatch".to_string())),
        }
    }

    fn push_symbol_id(&mut self, id: u32) -> Result<()> {
        self.ensure_owned_data();
        self.nulls.push(0);
        match &mut self.data {
            ColumnStorage::Symbol(values) => values.push(id),
            ColumnStorage::MmapSymbolW(view) => {
                view.append_bytes(&id.to_le_bytes())?;
            }
            _ => {}
        }
        Ok(())
    }

    fn set_symbol_id(&mut self, idx: usize, id: u32) -> Result<()> {
        if idx >= self.len() {
            return Err(Error::InvalidData("column index out of range".to_string()));
        }
        self.ensure_owned_data();
        self.nulls.set(idx, 0);
        match &mut self.data {
            ColumnStorage::Symbol(values) => values[idx] = id,
            ColumnStorage::MmapSymbolW(view) => {
                let offset = idx * std::mem::size_of::<u32>();
                view.write_at(offset, &id.to_le_bytes())?;
            }
            _ => {}
        }
        Ok(())
    }

    fn push_default_value(&mut self) {
        match &mut self.data {
            ColumnStorage::I64(values) => values.push(0),
            ColumnStorage::F64(values) => values.push(0.0),
            ColumnStorage::Bool(values) => values.push(0),
            ColumnStorage::String(values) => values.push(String::new()),
            ColumnStorage::Bytes(values) => values.push(Vec::new()),
            ColumnStorage::Timestamp(values) => values.push(0),
            ColumnStorage::Symbol(values) => values.push(0),
            ColumnStorage::MmapI64(_) => self.materialize(),
            ColumnStorage::MmapF64(_) => self.materialize(),
            ColumnStorage::MmapBool(_) => self.materialize(),
            ColumnStorage::MmapTimestamp(_) => self.materialize(),
            ColumnStorage::MmapI64W(view) => {
                let _ = view.append_bytes(&0i64.to_le_bytes());
            }
            ColumnStorage::MmapF64W(view) => {
                let _ = view.append_bytes(&0f64.to_le_bytes());
            }
            ColumnStorage::MmapBoolW(view) => {
                let _ = view.append_bytes(&[0u8]);
            }
            ColumnStorage::MmapTimestampW(view) => {
                let _ = view.append_bytes(&0i64.to_le_bytes());
            }
            ColumnStorage::MmapSymbolW(view) => {
                let _ = view.append_bytes(&0u32.to_le_bytes());
            }
            ColumnStorage::MmapVarStringW { offsets, data } => {
                let offset = data.len() as u64;
                let _ = offsets.append_bytes(&offset.to_le_bytes());
                let _ = data.append_bytes(&0u32.to_le_bytes());
            }
            ColumnStorage::MmapVarBytesW { offsets, data } => {
                let offset = data.len() as u64;
                let _ = offsets.append_bytes(&offset.to_le_bytes());
                let _ = data.append_bytes(&0u32.to_le_bytes());
            }
        }
    }

    fn set_default_value(&mut self, idx: usize) {
        match &mut self.data {
            ColumnStorage::I64(values) => values[idx] = 0,
            ColumnStorage::F64(values) => values[idx] = 0.0,
            ColumnStorage::Bool(values) => values[idx] = 0,
            ColumnStorage::String(values) => values[idx].clear(),
            ColumnStorage::Bytes(values) => values[idx].clear(),
            ColumnStorage::Timestamp(values) => values[idx] = 0,
            ColumnStorage::Symbol(values) => values[idx] = 0,
            ColumnStorage::MmapI64(_) => self.materialize(),
            ColumnStorage::MmapF64(_) => self.materialize(),
            ColumnStorage::MmapBool(_) => self.materialize(),
            ColumnStorage::MmapTimestamp(_) => self.materialize(),
            ColumnStorage::MmapI64W(view) => {
                let offset = idx * std::mem::size_of::<i64>();
                let _ = view.write_at(offset, &0i64.to_le_bytes());
            }
            ColumnStorage::MmapF64W(view) => {
                let offset = idx * std::mem::size_of::<f64>();
                let _ = view.write_at(offset, &0f64.to_le_bytes());
            }
            ColumnStorage::MmapBoolW(view) => {
                let _ = view.write_at(idx, &[0u8]);
            }
            ColumnStorage::MmapTimestampW(view) => {
                let offset = idx * std::mem::size_of::<i64>();
                let _ = view.write_at(offset, &0i64.to_le_bytes());
            }
            ColumnStorage::MmapSymbolW(view) => {
                let offset = idx * std::mem::size_of::<u32>();
                let _ = view.write_at(offset, &0u32.to_le_bytes());
            }
            ColumnStorage::MmapVarStringW { offsets, data } => {
                let offset = data.len() as u64;
                let _ = offsets.write_at(idx * 8, &offset.to_le_bytes());
                let _ = data.append_bytes(&0u32.to_le_bytes());
            }
            ColumnStorage::MmapVarBytesW { offsets, data } => {
                let offset = data.len() as u64;
                let _ = offsets.write_at(idx * 8, &offset.to_le_bytes());
                let _ = data.append_bytes(&0u32.to_le_bytes());
            }
        }
    }

    fn ensure_owned_data(&mut self) {
        match self.data {
            ColumnStorage::MmapI64(_)
            | ColumnStorage::MmapF64(_)
            | ColumnStorage::MmapBool(_)
            | ColumnStorage::MmapTimestamp(_) => {
                self.nulls.ensure_owned();
                self.materialize();
            }
            _ => {}
        }
    }

    fn materialize(&mut self) {
        let new_data = match &self.data {
            ColumnStorage::MmapI64(view) => {
                let slice = mmap_as_slice::<i64>(view);
                ColumnStorage::I64(slice.to_vec())
            }
            ColumnStorage::MmapF64(view) => {
                let slice = mmap_as_slice::<f64>(view);
                ColumnStorage::F64(slice.to_vec())
            }
            ColumnStorage::MmapBool(view) => {
                let slice = view.as_slice();
                ColumnStorage::Bool(slice.to_vec())
            }
            ColumnStorage::MmapTimestamp(view) => {
                let slice = mmap_as_slice::<i64>(view);
                ColumnStorage::Timestamp(slice.to_vec())
            }
            _ => return,
        };
        self.data = new_data;
    }

    fn extend_with_default(&mut self, count: usize, default: &Value) -> Result<()> {
        for _ in 0..count {
            self.push_value(default.clone())?;
        }
        Ok(())
    }

    fn get_ref(&self, idx: usize) -> Option<ValueRef<'_>> {
        if idx >= self.len() {
            return None;
        }
        if self.nulls.get(idx).unwrap_or(0) == 1 {
            return Some(ValueRef::Null);
        }
        match &self.data {
            ColumnStorage::I64(values) => values.get(idx).copied().map(ValueRef::I64),
            ColumnStorage::F64(values) => values.get(idx).copied().map(ValueRef::F64),
            ColumnStorage::Bool(values) => values.get(idx).map(|v| ValueRef::Bool(*v != 0)),
            ColumnStorage::String(values) => values.get(idx).map(|v| ValueRef::String(v.as_str())),
            ColumnStorage::Bytes(values) => values.get(idx).map(|v| ValueRef::Bytes(v.as_slice())),
            ColumnStorage::Timestamp(values) => values.get(idx).copied().map(ValueRef::Timestamp),
            ColumnStorage::Symbol(_) => Some(ValueRef::Null),
            ColumnStorage::MmapI64(view) => mmap_as_slice::<i64>(view)
                .get(idx)
                .copied()
                .map(ValueRef::I64),
            ColumnStorage::MmapF64(view) => mmap_as_slice::<f64>(view)
                .get(idx)
                .copied()
                .map(ValueRef::F64),
            ColumnStorage::MmapBool(view) => view
                .as_slice()
                .get(idx)
                .copied()
                .map(|v| ValueRef::Bool(v != 0)),
            ColumnStorage::MmapTimestamp(view) => mmap_as_slice::<i64>(view)
                .get(idx)
                .copied()
                .map(ValueRef::Timestamp),
            ColumnStorage::MmapI64W(view) => mmap_write_as_slice::<i64>(view)
                .get(idx)
                .copied()
                .map(ValueRef::I64),
            ColumnStorage::MmapF64W(view) => mmap_write_as_slice::<f64>(view)
                .get(idx)
                .copied()
                .map(ValueRef::F64),
            ColumnStorage::MmapBoolW(view) => view
                .as_slice()
                .get(idx)
                .copied()
                .map(|v| ValueRef::Bool(v != 0)),
            ColumnStorage::MmapTimestampW(view) => mmap_write_as_slice::<i64>(view)
                .get(idx)
                .copied()
                .map(ValueRef::Timestamp),
            ColumnStorage::MmapSymbolW(_) => Some(ValueRef::Null),
            ColumnStorage::MmapVarStringW { offsets, data } => {
                let offsets = mmap_write_as_slice::<u64>(offsets);
                let data = data.as_slice();
                if idx >= offsets.len() {
                    return None;
                }
                let start = offsets[idx] as usize;
                if start + 4 > data.len() {
                    return Some(ValueRef::Null);
                }
                let len = u32::from_le_bytes([
                    data[start],
                    data[start + 1],
                    data[start + 2],
                    data[start + 3],
                ]) as usize;
                let end = start + 4 + len;
                if end > data.len() {
                    return Some(ValueRef::Null);
                }
                let bytes = &data[start + 4..end];
                let value = std::str::from_utf8(bytes).ok()?;
                Some(ValueRef::String(value))
            }
            ColumnStorage::MmapVarBytesW { offsets, data } => {
                let offsets = mmap_write_as_slice::<u64>(offsets);
                let data = data.as_slice();
                if idx >= offsets.len() {
                    return None;
                }
                let start = offsets[idx] as usize;
                if start + 4 > data.len() {
                    return Some(ValueRef::Null);
                }
                let len = u32::from_le_bytes([
                    data[start],
                    data[start + 1],
                    data[start + 2],
                    data[start + 3],
                ]) as usize;
                let end = start + 4 + len;
                if end > data.len() {
                    return Some(ValueRef::Null);
                }
                Some(ValueRef::Bytes(&data[start + 4..end]))
            }
        }
    }

    fn get_ref_with_symbols<'a>(
        &'a self,
        idx: usize,
        symbols: &'a SymbolInterner,
    ) -> Option<ValueRef<'a>> {
        if idx >= self.len() {
            return None;
        }
        if self.nulls.get(idx).unwrap_or(0) == 1 {
            return Some(ValueRef::Null);
        }
        match &self.data {
            ColumnStorage::Symbol(values) => {
                let id = *values.get(idx)?;
                let value = symbols.resolve(id).unwrap_or("");
                Some(ValueRef::String(value))
            }
            ColumnStorage::MmapSymbolW(values) => {
                let ids = mmap_write_as_slice::<u32>(values);
                let id = *ids.get(idx)?;
                let value = symbols.resolve(id).unwrap_or("");
                Some(ValueRef::String(value))
            }
            _ => self.get_ref(idx),
        }
    }

    fn take_rows(&self, indices: &[usize], symbols: &SymbolInterner) -> Result<Self> {
        let mut out = ColumnData::new(self.ty);
        out.reserve(indices.len());
        if self.ty == ColumnType::Symbol {
            if let ColumnStorage::Symbol(values) = &self.data {
                for idx in indices {
                    let is_null = self.nulls.get(*idx).unwrap_or(0) == 1;
                    if is_null {
                        out.nulls.push(1);
                        out.push_default_value();
                    } else {
                        let id = values.get(*idx).copied().unwrap_or(0);
                        out.push_symbol_id(id)?;
                    }
                }
                return Ok(out);
            }
        }
        for idx in indices {
            let value = if self.ty == ColumnType::Symbol {
                self.get_ref_with_symbols(*idx, symbols)
                    .unwrap_or(ValueRef::Null)
                    .to_value()
            } else {
                self.get_ref(*idx).unwrap_or(ValueRef::Null).to_value()
            };
            out.push_value(value)?;
        }
        Ok(out)
    }

    fn view(&self) -> ColumnView<'_> {
        match &self.data {
            ColumnStorage::I64(values) => ColumnView::I64(values.as_slice(), self.nulls.as_slice()),
            ColumnStorage::F64(values) => ColumnView::F64(values.as_slice(), self.nulls.as_slice()),
            ColumnStorage::Bool(values) => {
                ColumnView::Bool(values.as_slice(), self.nulls.as_slice())
            }
            ColumnStorage::String(values) => {
                ColumnView::String(values.as_slice(), self.nulls.as_slice())
            }
            ColumnStorage::Bytes(values) => {
                ColumnView::Bytes(values.as_slice(), self.nulls.as_slice())
            }
            ColumnStorage::Timestamp(values) => {
                ColumnView::Timestamp(values.as_slice(), self.nulls.as_slice())
            }
            ColumnStorage::Symbol(values) => {
                ColumnView::Symbol(values.as_slice(), self.nulls.as_slice(), None)
            }
            ColumnStorage::MmapI64(view) => {
                ColumnView::I64(mmap_as_slice::<i64>(view), self.nulls.as_slice())
            }
            ColumnStorage::MmapF64(view) => {
                ColumnView::F64(mmap_as_slice::<f64>(view), self.nulls.as_slice())
            }
            ColumnStorage::MmapBool(view) => {
                ColumnView::Bool(view.as_slice(), self.nulls.as_slice())
            }
            ColumnStorage::MmapTimestamp(view) => {
                ColumnView::Timestamp(mmap_as_slice::<i64>(view), self.nulls.as_slice())
            }
            ColumnStorage::MmapI64W(view) => {
                ColumnView::I64(mmap_write_as_slice::<i64>(view), self.nulls.as_slice())
            }
            ColumnStorage::MmapF64W(view) => {
                ColumnView::F64(mmap_write_as_slice::<f64>(view), self.nulls.as_slice())
            }
            ColumnStorage::MmapBoolW(view) => {
                ColumnView::Bool(view.as_slice(), self.nulls.as_slice())
            }
            ColumnStorage::MmapTimestampW(view) => {
                ColumnView::Timestamp(mmap_write_as_slice::<i64>(view), self.nulls.as_slice())
            }
            ColumnStorage::MmapSymbolW(view) => {
                ColumnView::Symbol(mmap_write_as_slice::<u32>(view), self.nulls.as_slice(), None)
            }
            ColumnStorage::MmapVarStringW { offsets, data } => ColumnView::VarString(
                mmap_write_as_slice::<u64>(offsets),
                data.as_slice(),
                self.nulls.as_slice(),
            ),
            ColumnStorage::MmapVarBytesW { offsets, data } => ColumnView::VarBytes(
                mmap_write_as_slice::<u64>(offsets),
                data.as_slice(),
                self.nulls.as_slice(),
            ),
        }
    }

    fn view_with_symbols<'a>(
        &'a self,
        symbols: &'a SymbolInterner,
    ) -> ColumnView<'a> {
        match &self.data {
            ColumnStorage::Symbol(values) => {
                ColumnView::Symbol(values.as_slice(), self.nulls.as_slice(), Some(symbols))
            }
            ColumnStorage::MmapSymbolW(values) => {
                ColumnView::Symbol(mmap_write_as_slice::<u32>(values), self.nulls.as_slice(), Some(symbols))
            }
            _ => self.view(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnView<'a> {
    I64(&'a [i64], &'a [u8]),
    F64(&'a [f64], &'a [u8]),
    Bool(&'a [u8], &'a [u8]),
    String(&'a [String], &'a [u8]),
    Symbol(&'a [u32], &'a [u8], Option<&'a SymbolInterner>),
    Bytes(&'a [Vec<u8>], &'a [u8]),
    Timestamp(&'a [i64], &'a [u8]),
    VarString(&'a [u64], &'a [u8], &'a [u8]),
    VarBytes(&'a [u64], &'a [u8], &'a [u8]),
}

impl ColumnView<'_> {
    pub fn len(&self) -> usize {
        match self {
            ColumnView::I64(values, _) => values.len(),
            ColumnView::F64(values, _) => values.len(),
            ColumnView::Bool(values, _) => values.len(),
            ColumnView::String(values, _) => values.len(),
            ColumnView::Symbol(values, _, _) => values.len(),
            ColumnView::Bytes(values, _) => values.len(),
            ColumnView::Timestamp(values, _) => values.len(),
            ColumnView::VarString(offsets, _, _) => offsets.len(),
            ColumnView::VarBytes(offsets, _, _) => offsets.len(),
        }
    }

    pub fn get_value(&self, idx: usize) -> Option<ValueRef<'_>> {
        match self {
            ColumnView::I64(values, nulls) => get_column_value(values, nulls, idx, ValueRef::I64),
            ColumnView::F64(values, nulls) => get_column_value(values, nulls, idx, ValueRef::F64),
            ColumnView::Bool(values, nulls) => {
                if idx >= values.len() {
                    return None;
                }
                if nulls.get(idx).copied().unwrap_or(0) == 1 {
                    return Some(ValueRef::Null);
                }
                Some(ValueRef::Bool(values[idx] != 0))
            }
            ColumnView::String(values, nulls) => {
                if idx >= values.len() {
                    return None;
                }
                if nulls.get(idx).copied().unwrap_or(0) == 1 {
                    return Some(ValueRef::Null);
                }
                Some(ValueRef::String(values[idx].as_str()))
            }
            ColumnView::Symbol(values, nulls, symbols) => {
                if idx >= values.len() {
                    return None;
                }
                if nulls.get(idx).copied().unwrap_or(0) == 1 {
                    return Some(ValueRef::Null);
                }
                let symbols = match symbols {
                    Some(sym) => *sym,
                    None => return Some(ValueRef::Null),
                };
                let value = symbols.resolve(values[idx]).unwrap_or("");
                Some(ValueRef::String(value))
            }
            ColumnView::Bytes(values, nulls) => {
                if idx >= values.len() {
                    return None;
                }
                if nulls.get(idx).copied().unwrap_or(0) == 1 {
                    return Some(ValueRef::Null);
                }
                Some(ValueRef::Bytes(values[idx].as_slice()))
            }
            ColumnView::Timestamp(values, nulls) => {
                get_column_value(values, nulls, idx, ValueRef::Timestamp)
            }
            ColumnView::VarString(offsets, data, nulls) => {
                if idx >= offsets.len() {
                    return None;
                }
                if nulls.get(idx).copied().unwrap_or(0) == 1 {
                    return Some(ValueRef::Null);
                }
                let start = offsets[idx] as usize;
                if start + 4 > data.len() {
                    return Some(ValueRef::Null);
                }
                let len = u32::from_le_bytes([
                    data[start],
                    data[start + 1],
                    data[start + 2],
                    data[start + 3],
                ]) as usize;
                let end = start + 4 + len;
                if end > data.len() {
                    return Some(ValueRef::Null);
                }
                let bytes = &data[start + 4..end];
                let value = std::str::from_utf8(bytes).ok()?;
                Some(ValueRef::String(value))
            }
            ColumnView::VarBytes(offsets, data, nulls) => {
                if idx >= offsets.len() {
                    return None;
                }
                if nulls.get(idx).copied().unwrap_or(0) == 1 {
                    return Some(ValueRef::Null);
                }
                let start = offsets[idx] as usize;
                if start + 4 > data.len() {
                    return Some(ValueRef::Null);
                }
                let len = u32::from_le_bytes([
                    data[start],
                    data[start + 1],
                    data[start + 2],
                    data[start + 3],
                ]) as usize;
                let end = start + 4 + len;
                if end > data.len() {
                    return Some(ValueRef::Null);
                }
                Some(ValueRef::Bytes(&data[start + 4..end]))
            }
        }
    }

    pub fn as_f64(&self) -> Option<&[f64]> {
        match self {
            ColumnView::F64(values, _) => Some(values),
            _ => None,
        }
    }

    pub fn nulls(&self) -> &[u8] {
        match self {
            ColumnView::I64(_, nulls) => nulls,
            ColumnView::F64(_, nulls) => nulls,
            ColumnView::Bool(_, nulls) => nulls,
            ColumnView::String(_, nulls) => nulls,
            ColumnView::Symbol(_, nulls, _) => nulls,
            ColumnView::Bytes(_, nulls) => nulls,
            ColumnView::Timestamp(_, nulls) => nulls,
            ColumnView::VarString(_, _, nulls) => nulls,
            ColumnView::VarBytes(_, _, nulls) => nulls,
        }
    }
}

fn mmap_as_slice<T>(view: &MmapView) -> &[T] {
    let bytes = view.as_slice();
    if bytes.is_empty() {
        return &[];
    }
    let size = std::mem::size_of::<T>();
    let align = std::mem::align_of::<T>();
    debug_assert!(size > 0);
    debug_assert!(bytes.len() % size == 0);
    let ptr = bytes.as_ptr() as usize;
    debug_assert!(ptr % align == 0);
    let len = bytes.len() / size;
    unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const T, len) }
}

fn mmap_write_as_slice<T>(view: &MmapWrite) -> &[T] {
    let bytes = view.as_slice();
    if bytes.is_empty() {
        return &[];
    }
    let size = std::mem::size_of::<T>();
    let align = std::mem::align_of::<T>();
    debug_assert!(size > 0);
    debug_assert!(bytes.len() % size == 0);
    let ptr = bytes.as_ptr() as usize;
    debug_assert!(ptr % align == 0);
    let len = bytes.len() / size;
    unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const T, len) }
}

fn write_numeric_slice<T: Copy>(file: &mut File, values: &[T]) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let byte_len = values.len() * std::mem::size_of::<T>();
    let bytes = unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) };
    file.write_all(bytes)?;
    Ok(())
}

fn write_string_values(file: &mut File, values: &[String]) -> Result<()> {
    for value in values {
        let len = value.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(value.as_bytes())?;
    }
    Ok(())
}

fn write_bytes_values(file: &mut File, values: &[Vec<u8>]) -> Result<()> {
    for value in values {
        let len = value.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(value)?;
    }
    Ok(())
}

fn write_string_values_with_offsets(
    data_file: &mut File,
    offsets_file: &mut File,
    values: &[String],
) -> Result<()> {
    let mut offset = 0u64;
    for value in values {
        offsets_file.write_all(&offset.to_le_bytes())?;
        let len = value.len() as u32;
        data_file.write_all(&len.to_le_bytes())?;
        data_file.write_all(value.as_bytes())?;
        offset = offset
            .checked_add(4)
            .and_then(|v| v.checked_add(value.len() as u64))
            .ok_or_else(|| Error::InvalidData("bytes length overflow".to_string()))?;
    }
    Ok(())
}

fn write_bytes_values_with_offsets(
    data_file: &mut File,
    offsets_file: &mut File,
    values: &[Vec<u8>],
) -> Result<()> {
    let mut offset = 0u64;
    for value in values {
        offsets_file.write_all(&offset.to_le_bytes())?;
        let len = value.len() as u32;
        data_file.write_all(&len.to_le_bytes())?;
        data_file.write_all(value)?;
        offset = offset
            .checked_add(4)
            .and_then(|v| v.checked_add(value.len() as u64))
            .ok_or_else(|| Error::InvalidData("bytes length overflow".to_string()))?;
    }
    Ok(())
}

fn get_column_value<'a, T: Copy, F: FnOnce(T) -> ValueRef<'a>>(
    values: &'a [T],
    nulls: &'a [u8],
    idx: usize,
    ctor: F,
) -> Option<ValueRef<'a>> {
    if idx >= values.len() {
        return None;
    }
    if nulls.get(idx).copied().unwrap_or(0) == 1 {
        return Some(ValueRef::Null);
    }
    Some(ctor(values[idx]))
}

struct TableAccess<'a> {
    table: &'a Table,
}

impl<'a> TableAccess<'a> {
    fn new(table: &'a Table) -> Self {
        Self { table }
    }
}

impl ColumnAccess for TableAccess<'_> {
    fn row_count(&self) -> usize {
        self.table.row_count
    }

    fn value_at(&self, col_idx: usize, row_idx: usize) -> Option<ValueRef<'_>> {
        let column = self.table.columns.get(col_idx)?;
        if column.ty == ColumnType::Symbol {
            column.get_ref_with_symbols(row_idx, &self.table.symbols)
        } else {
            column.get_ref(row_idx)
        }
    }
}


fn resolve_column_map(schema: &Schema, columns: &[String]) -> Result<Vec<usize>> {
    let mut map = Vec::with_capacity(columns.len());
    for name in columns {
        let idx = schema
            .column_index(name)
            .ok_or_else(|| Error::InvalidData(format!("unknown column {name}")))?;
        map.push(idx);
    }
    Ok(map)
}

enum ColStackValue<'a> {
    Value(ValueRef<'a>),
    Bool(bool),
}

fn eval_column_filter<A: ColumnAccess + ?Sized>(
    instrs: &[ColumnInstr],
    access: &A,
    col_map: &[usize],
    row_idx: usize,
) -> Result<bool> {
    let mut stack: Vec<ColStackValue<'_>> = Vec::with_capacity(instrs.len());
    for instr in instrs {
        match instr {
            ColumnInstr::PushCol(idx) => {
                let idx = *idx as usize;
                if idx >= col_map.len() {
                    return Err(Error::InvalidData("filter column out of range".to_string()));
                }
                let schema_idx = col_map[idx];
                let value = access.value_at(schema_idx, row_idx).unwrap_or(ValueRef::Null);
                stack.push(ColStackValue::Value(value));
            }
            ColumnInstr::PushLit(value) => {
                stack.push(ColStackValue::Value(ValueRef::from(value)));
            }
            ColumnInstr::Cmp(op) => {
                let right = match stack.pop() {
                    Some(ColStackValue::Value(v)) => v,
                    _ => {
                        return Err(Error::InvalidData(
                            "filter expects value on stack".to_string(),
                        ))
                    }
                };
                let left = match stack.pop() {
                    Some(ColStackValue::Value(v)) => v,
                    _ => {
                        return Err(Error::InvalidData(
                            "filter expects value on stack".to_string(),
                        ))
                    }
                };
                let passed = compare_values(op, &left, &right)?;
                stack.push(ColStackValue::Bool(passed));
            }
            ColumnInstr::And => {
                let right = match stack.pop() {
                    Some(ColStackValue::Bool(v)) => v,
                    _ => {
                        return Err(Error::InvalidData(
                            "filter expects bool on stack".to_string(),
                        ))
                    }
                };
                let left = match stack.pop() {
                    Some(ColStackValue::Bool(v)) => v,
                    _ => {
                        return Err(Error::InvalidData(
                            "filter expects bool on stack".to_string(),
                        ))
                    }
                };
                stack.push(ColStackValue::Bool(left && right));
            }
            ColumnInstr::Or => {
                let right = match stack.pop() {
                    Some(ColStackValue::Bool(v)) => v,
                    _ => {
                        return Err(Error::InvalidData(
                            "filter expects bool on stack".to_string(),
                        ))
                    }
                };
                let left = match stack.pop() {
                    Some(ColStackValue::Bool(v)) => v,
                    _ => {
                        return Err(Error::InvalidData(
                            "filter expects bool on stack".to_string(),
                        ))
                    }
                };
                stack.push(ColStackValue::Bool(left || right));
            }
            ColumnInstr::Not => {
                let value = match stack.pop() {
                    Some(ColStackValue::Bool(v)) => v,
                    _ => {
                        return Err(Error::InvalidData(
                            "filter expects bool on stack".to_string(),
                        ))
                    }
                };
                stack.push(ColStackValue::Bool(!value));
            }
        }
    }
    match stack.pop() {
        Some(ColStackValue::Bool(v)) => Ok(v),
        _ => Err(Error::InvalidData("filter did not resolve to bool".to_string())),
    }
}

const INLINE_KEY_CAP: usize = 4;

struct KeyBuf {
    len: usize,
    inline: [MaybeUninit<Value>; INLINE_KEY_CAP],
    heap: Option<Vec<Value>>,
}

impl KeyBuf {
    fn new_empty() -> Self {
        Self {
            len: 0,
            inline: unsafe { MaybeUninit::uninit().assume_init() },
            heap: None,
        }
    }

    fn from_indices<A: ColumnAccess + ?Sized>(
        access: &A,
        row_idx: usize,
        indices: &[usize],
    ) -> Self {
        if indices.is_empty() {
            return Self::new_empty();
        }
        let len = indices.len();
        if len <= INLINE_KEY_CAP {
            let mut inline: [MaybeUninit<Value>; INLINE_KEY_CAP] =
                unsafe { MaybeUninit::uninit().assume_init() };
            for (pos, idx) in indices.iter().enumerate() {
                let value = access
                    .value_at(*idx, row_idx)
                    .unwrap_or(ValueRef::Null)
                    .to_value();
                inline[pos].write(value);
            }
            Self {
                len,
                inline,
                heap: None,
            }
        } else {
            let mut values = Vec::with_capacity(len);
            for idx in indices {
                values.push(
                    access
                        .value_at(*idx, row_idx)
                        .unwrap_or(ValueRef::Null)
                        .to_value(),
                );
            }
            Self {
                len,
                inline: unsafe { MaybeUninit::uninit().assume_init() },
                heap: Some(values),
            }
        }
    }

    fn from_values(values: &[Value]) -> Self {
        if values.is_empty() {
            return Self::new_empty();
        }
        let len = values.len();
        if len <= INLINE_KEY_CAP {
            let mut inline: [MaybeUninit<Value>; INLINE_KEY_CAP] =
                unsafe { MaybeUninit::uninit().assume_init() };
            for (pos, value) in values.iter().enumerate() {
                inline[pos].write(value.clone());
            }
            Self {
                len,
                inline,
                heap: None,
            }
        } else {
            Self {
                len,
                inline: unsafe { MaybeUninit::uninit().assume_init() },
                heap: Some(values.to_vec()),
            }
        }
    }

    fn as_slice(&self) -> &[Value] {
        match &self.heap {
            Some(values) => values.as_slice(),
            None => unsafe {
                slice::from_raw_parts(self.inline.as_ptr() as *const Value, self.len)
            },
        }
    }

    fn get(&self, idx: usize) -> Option<&Value> {
        self.as_slice().get(idx)
    }
}

impl Clone for KeyBuf {
    fn clone(&self) -> Self {
        Self::from_values(self.as_slice())
    }
}

impl Drop for KeyBuf {
    fn drop(&mut self) {
        if self.heap.is_some() {
            return;
        }
        for idx in 0..self.len {
            unsafe {
                self.inline[idx].assume_init_drop();
            }
        }
    }
}

impl PartialEq for KeyBuf {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for KeyBuf {}

impl Hash for KeyBuf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

#[derive(Debug, Default)]
pub struct MemStore {
    tables: HashMap<String, Table>,
    store: Option<SplayedStore>,
    scratch_col_indices: Vec<usize>,
    scratch_value_pos: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct ResultSet {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            store: None,
            scratch_col_indices: Vec::new(),
            scratch_value_pos: Vec::new(),
        }
    }

    pub fn with_store(store: SplayedStore) -> Self {
        Self {
            tables: HashMap::new(),
            store: Some(store),
            scratch_col_indices: Vec::new(),
            scratch_value_pos: Vec::new(),
        }
    }

    pub fn load_from_store(&mut self) -> Result<()> {
        let Some(store) = self.store.as_ref() else {
            return Ok(());
        };
        let tables = store.list_tables()?;
        for table_name in tables {
            let schema = store.read_schema(&table_name)?;
            let mut columns = Vec::with_capacity(schema.columns().len());
            let symbols = SymbolInterner::from_values(store.symbol_values());
            let mut row_count: Option<usize> = None;
            for column in schema.columns() {
                let (col_data, col_rows) =
                    load_column_from_store(store, &table_name, column)?;
                if let Some(existing) = row_count {
                    if existing != col_rows {
                        return Err(Error::InvalidData(format!(
                            "row count mismatch in {table_name}.{name}: {existing} vs {col_rows}",
                            name = column.name
                        )));
                    }
                } else {
                    row_count = Some(col_rows);
                }
                columns.push(col_data);
            }
            self.tables.insert(
                table_name,
                Table {
                    schema,
                    columns,
                    row_count: row_count.unwrap_or(0),
                    symbols,
                },
            );
        }
        Ok(())
    }

    pub fn flush_to_store(&mut self) -> Result<()> {
        if self.store.is_none() {
            return Ok(());
        };
        let table_names: Vec<String> = self.tables.keys().cloned().collect();
        for table_name in table_names {
            self.flush_table_to_store(&table_name)?;
        }
        Ok(())
    }

    fn flush_table_to_store(&mut self, table_name: &str) -> Result<()> {
        let Some(store) = self.store.as_mut() else {
            return Ok(());
        };
        let Some(table) = self.tables.get(table_name) else {
            return Ok(());
        };
        store.drop_table(table_name)?;
        store.create_table(table_name, &table.schema)?;
        if table.row_count == 0 {
            return Ok(());
        }
        let table_dir = store.table_dir(table_name);
        for (idx, spec) in table.schema.columns().iter().enumerate() {
            let column = table
                .columns
                .get(idx)
                .ok_or_else(|| Error::InvalidData("column data mismatch".to_string()))?;
            let data_path = table_dir.join(&spec.name);
            let null_path = table_dir.join(format!("{}.n", spec.name));
            let offsets_path = table_dir.join(format!("{}.o", spec.name));
            let mut data_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&data_path)?;
            let mut null_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&null_path)?;
            let nulls = column.nulls.as_slice();
            null_file.write_all(nulls)?;
            match (&column.data, spec.col_type) {
                (ColumnStorage::I64(values), ColumnType::I64) => {
                    write_numeric_slice(&mut data_file, values)?;
                }
                (ColumnStorage::MmapI64(view), ColumnType::I64) => {
                    write_numeric_slice(&mut data_file, mmap_as_slice::<i64>(view))?;
                }
                (ColumnStorage::MmapI64W(view), ColumnType::I64) => {
                    write_numeric_slice(&mut data_file, mmap_write_as_slice::<i64>(view))?;
                }
                (ColumnStorage::F64(values), ColumnType::F64) => {
                    write_numeric_slice(&mut data_file, values)?;
                }
                (ColumnStorage::MmapF64(view), ColumnType::F64) => {
                    write_numeric_slice(&mut data_file, mmap_as_slice::<f64>(view))?;
                }
                (ColumnStorage::MmapF64W(view), ColumnType::F64) => {
                    write_numeric_slice(&mut data_file, mmap_write_as_slice::<f64>(view))?;
                }
                (ColumnStorage::Bool(values), ColumnType::Bool) => {
                    data_file.write_all(values)?;
                }
                (ColumnStorage::MmapBool(view), ColumnType::Bool) => {
                    data_file.write_all(view.as_slice())?;
                }
                (ColumnStorage::MmapBoolW(view), ColumnType::Bool) => {
                    data_file.write_all(view.as_slice())?;
                }
                (ColumnStorage::Timestamp(values), ColumnType::Timestamp) => {
                    write_numeric_slice(&mut data_file, values)?;
                }
                (ColumnStorage::MmapTimestamp(view), ColumnType::Timestamp) => {
                    write_numeric_slice(&mut data_file, mmap_as_slice::<i64>(view))?;
                }
                (ColumnStorage::MmapTimestampW(view), ColumnType::Timestamp) => {
                    write_numeric_slice(&mut data_file, mmap_write_as_slice::<i64>(view))?;
                }
                (ColumnStorage::String(values), ColumnType::String) => {
                    let mut offsets_file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&offsets_path)?;
                    write_string_values_with_offsets(&mut data_file, &mut offsets_file, values)?;
                }
                (ColumnStorage::Bytes(values), ColumnType::Bytes) => {
                    let mut offsets_file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&offsets_path)?;
                    write_bytes_values_with_offsets(&mut data_file, &mut offsets_file, values)?;
                }
                (ColumnStorage::MmapVarStringW { offsets, data }, ColumnType::String) => {
                    data_file.write_all(data.as_slice())?;
                    let mut offsets_file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&offsets_path)?;
                    offsets_file.write_all(offsets.as_slice())?;
                }
                (ColumnStorage::MmapVarBytesW { offsets, data }, ColumnType::Bytes) => {
                    data_file.write_all(data.as_slice())?;
                    let mut offsets_file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&offsets_path)?;
                    offsets_file.write_all(offsets.as_slice())?;
                }
                (ColumnStorage::Symbol(values), ColumnType::Symbol) => {
                    for (row_idx, value) in values.iter().enumerate() {
                        if nulls.get(row_idx).copied().unwrap_or(0) == 1 {
                            data_file.write_all(&0u32.to_le_bytes())?;
                        } else {
                            data_file.write_all(&value.to_le_bytes())?;
                        }
                    }
                }
                (ColumnStorage::MmapSymbolW(values), ColumnType::Symbol) => {
                    let ids = mmap_write_as_slice::<u32>(values);
                    for (row_idx, value) in ids.iter().enumerate() {
                        if nulls.get(row_idx).copied().unwrap_or(0) == 1 {
                            data_file.write_all(&0u32.to_le_bytes())?;
                        } else {
                            data_file.write_all(&value.to_le_bytes())?;
                        }
                    }
                }
                _ => {
                    return Err(Error::InvalidData("column type mismatch".to_string()));
                }
            }
        }
        Ok(())
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(Error::InvalidData(format!("table already exists: {name}")));
        }
        if let Some(store) = self.store.as_mut() {
            store.create_table(name, &schema)?;
        }
        self.tables.insert(
            name.to_string(),
            Table {
                schema,
                columns: Vec::new(),
                row_count: 0,
                symbols: SymbolInterner::new(),
            },
        );
        if let Some(table) = self.tables.get_mut(name) {
            if let Some(store) = self.store.as_ref() {
                let mut columns = Vec::with_capacity(table.schema.columns().len());
                for col in table.schema.columns() {
                    columns.push(init_mmap_column_data(store, name, col)?);
                }
                table.columns = columns;
            } else {
                table.columns = table
                    .schema
                    .columns()
                    .iter()
                    .map(|col| ColumnData::new(col.col_type))
                    .collect();
            }
        }
        Ok(())
    }

    pub fn alter_add_column(&mut self, table: &str, column: ColumnSpec) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {table}")))?;
        let col_type = column.col_type;
        let default_value = column.default.clone().unwrap_or(Value::Null);
        table.schema.add_column(column)?;
        let mut col = ColumnData::new(col_type);
        col.fill_with_default(&default_value, table.row_count)?;
        table.columns.push(col);
        Ok(())
    }

    pub fn alter_drop_column(&mut self, table: &str, column: &str) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {table}")))?;
        let idx = table.schema.remove_column(column)?;
        if idx < table.columns.len() {
            table.columns.remove(idx);
        }
        Ok(())
    }

    pub fn alter_rename_column(&mut self, table: &str, from: &str, to: &str) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {table}")))?;
        table.schema.rename_column(from, to)?;
        Ok(())
    }

    pub fn alter_set_default(
        &mut self,
        table: &str,
        column: &str,
        default: Option<Value>,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {table}")))?;
        let idx = table
            .schema
            .column_index(column)
            .ok_or_else(|| Error::InvalidData(format!("unknown column name: {column}")))?;
        table.schema.set_column_default(idx, default)?;
        Ok(())
    }

    pub fn drop_table(&mut self, table: &str) -> Result<()> {
        if self.tables.remove(table).is_none() {
            return Err(Error::InvalidData(format!("table not found: {table}")));
        }
        Ok(())
    }

    pub fn rename_table(&mut self, from: &str, to: &str) -> Result<()> {
        if from == to {
            return Ok(());
        }
        if self.tables.contains_key(to) {
            return Err(Error::InvalidData(format!("table already exists: {to}")));
        }
        let table = self
            .tables
            .remove(from)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {from}")))?;
        self.tables.insert(to.to_string(), table);
        Ok(())
    }

    pub fn insert(&mut self, table: &str, columns: &[String], rows: &[Vec<Value>]) -> Result<()> {
        let table_name = table;
        if columns.is_empty() {
            return Err(Error::InvalidData("insert requires column list".to_string()));
        }
        if !self.tables.contains_key(table_name) {
            if let Some(store) = self.store.as_mut() {
                let mut specs = Vec::with_capacity(columns.len());
                for (col_pos, name) in columns.iter().enumerate() {
                    let col_type = infer_column_type_from_rows(rows, col_pos);
                    specs.push(ColumnSpec {
                        name: name.clone(),
                        col_type,
                        nullable: true,
                        default: None,
                    });
                }
                let schema = Schema::new(specs)?;
                store.create_table(table_name, &schema)?;
                let mut columns_data = Vec::with_capacity(schema.columns().len());
                for col in schema.columns() {
                    columns_data.push(init_mmap_column_data(store, table_name, col)?);
                }
                let symbols = SymbolInterner::from_values(store.symbol_values());
                self.tables.insert(
                    table_name.to_string(),
                    Table {
                        schema,
                        columns: columns_data,
                        row_count: 0,
                        symbols,
                    },
                );
            } else {
                self.tables.insert(
                    table_name.to_string(),
                    Table {
                        schema: Schema::new(Vec::new()).expect("empty schema"),
                        columns: Vec::new(),
                        row_count: 0,
                        symbols: SymbolInterner::new(),
                    },
                );
            }
        }
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::InvalidData("table not found".to_string()))?;

        self.scratch_col_indices.clear();
        self.scratch_col_indices.reserve(columns.len());
        for (col_pos, name) in columns.iter().enumerate() {
            let idx = match table.schema.column_index(name) {
                Some(idx) => idx,
                None => {
                    let col_type = infer_column_type_from_rows(rows, col_pos);
                    let spec = ColumnSpec {
                        name: name.clone(),
                        col_type,
                        nullable: true,
                        default: None,
                    };
                    table.schema.add_column(spec.clone())?;
                    let idx = table.schema.columns().len() - 1;
                    if let Some(store) = self.store.as_mut() {
                        store.alter_add_column(table_name, &spec)?;
                        let column = init_mmap_column_data(store, table_name, &spec)?;
                        table.columns.push(column);
                    } else {
                        let mut column = ColumnData::new(col_type);
                        column.extend_with_default(table.row_count, &Value::Null)?;
                        table.columns.push(column);
                    }
                    idx
                }
            };
            self.scratch_col_indices.push(idx);
        }

        let schema_len = table.schema.columns().len();
        self.scratch_value_pos.clear();
        self.scratch_value_pos.resize(schema_len, usize::MAX);
        for (value_idx, col_idx) in self.scratch_col_indices.iter().enumerate() {
            if let Some(pos) = self.scratch_value_pos.get_mut(*col_idx) {
                *pos = value_idx;
            }
        }
        let defaults = table.schema.default_row();
        for column in &mut table.columns {
            column.reserve(rows.len());
        }
        for values in rows {
            if values.len() != columns.len() {
                return Err(Error::InvalidData("insert row length mismatch".to_string()));
            }
        }

        let schema_columns = table.schema.columns();
        for (col_idx, column) in table.columns.iter_mut().enumerate() {
            let col_type = schema_columns[col_idx].col_type;
            let default_value = defaults.get(col_idx).unwrap_or(&Value::Null);
            let value_pos = self.scratch_value_pos.get(col_idx).copied().unwrap_or(usize::MAX);
            if value_pos != usize::MAX {
                for values in rows {
                    let raw_value = values.get(value_pos).unwrap_or(&Value::Null);
                    let value = if raw_value.is_null() {
                        Value::Null
                    } else if raw_value.column_type() == col_type {
                        raw_value.clone()
                    } else {
                        coerce_value(raw_value.clone(), col_type)?
                    };
                    if col_type == ColumnType::Symbol {
                        if value.is_null() {
                            column.push_value(Value::Null)?;
                        } else if let Value::String(symbol) = value {
                            let id = if let Some(store) = self.store.as_mut() {
                                let id = store.symbol_id(&symbol)?;
                                table.symbols.intern_with_id(&symbol, id)
                            } else {
                                table.symbols.intern(&symbol)
                            };
                            column.push_symbol_id(id)?;
                        } else {
                            return Err(Error::InvalidData("symbol expects string".to_string()));
                        }
                    } else {
                        column.push_value(value)?;
                    }
                }
            } else if col_type == ColumnType::Symbol {
                for _ in 0..rows.len() {
                    let value = if default_value.is_null() {
                        Value::Null
                    } else {
                        coerce_value(default_value.clone(), col_type)?
                    };
                    if value.is_null() {
                        column.push_value(Value::Null)?;
                    } else if let Value::String(symbol) = value {
                        let id = if let Some(store) = self.store.as_mut() {
                            let id = store.symbol_id(&symbol)?;
                            table.symbols.intern_with_id(&symbol, id)
                        } else {
                            table.symbols.intern(&symbol)
                        };
                        column.push_symbol_id(id)?;
                    } else {
                        return Err(Error::InvalidData("symbol expects string".to_string()));
                    }
                }
            } else {
                column.fill_with_default(default_value, rows.len())?;
            }
        }
        table.row_count += rows.len();
        Ok(())
    }

    pub fn table_schema(&self, name: &str) -> Option<Schema> {
        self.tables.get(name).map(|table| table.schema.clone())
    }

    pub fn table_row_count(&self, name: &str) -> Option<usize> {
        self.tables.get(name).map(|table| table.row_count)
    }

    pub fn table_row(&self, name: &str, index: usize) -> Option<Row> {
        self.tables.get(name).and_then(|table| table_row(table, index))
    }

    pub fn table_rows(&self, name: &str) -> Option<Vec<Row>> {
        self.tables.get(name).map(table_rows)
    }

    pub fn update(
        &mut self,
        table: &str,
        assignments: &[(String, Value)],
        filter: Option<&Expr>,
    ) -> Result<u64> {
        let Some(table) = self.tables.get_mut(table) else {
            return Ok(0);
        };

        let mut assign_indices = Vec::with_capacity(assignments.len());
        for (name, value) in assignments {
            let idx = match table.schema.column_index(name) {
                Some(idx) => idx,
                None => {
                    let col_type = value.column_type();
                    table.schema.add_column(ColumnSpec {
                        name: name.clone(),
                        col_type,
                        nullable: true,
                        default: None,
                    })?;
                    let idx = table.schema.columns().len() - 1;
                    let mut column = ColumnData::new(col_type);
                    column.extend_with_default(table.row_count, &Value::Null)?;
                    table.columns.push(column);
                    idx
                }
            };
            assign_indices.push((idx, value.clone()));
        }

        let mut updated = 0u64;
        let schema = table.schema.clone();
        for row_idx in 0..table.row_count {
            if let Some(expr) = filter {
                let access = TableAccess::new(&*table);
                if !eval_predicate_at(expr, &access, &schema, row_idx)? {
                    continue;
                }
            }
            let (columns, symbols) = (&mut table.columns, &mut table.symbols);
            for (idx, value) in &assign_indices {
                if let Some(column) = columns.get_mut(*idx) {
                    let col_type = schema.columns()[*idx].col_type;
                    let mut normalized = value.clone();
                    if !normalized.is_null() && normalized.column_type() != col_type {
                        normalized = coerce_value(normalized, col_type)?;
                    }
                    if col_type == ColumnType::Symbol {
                        if normalized.is_null() {
                            column.set_value(row_idx, Value::Null)?;
                        } else if let Value::String(symbol) = normalized {
                            let id = if let Some(store) = self.store.as_mut() {
                                let id = store.symbol_id(&symbol)?;
                                symbols.intern_with_id(&symbol, id)
                            } else {
                                symbols.intern(&symbol)
                            };
                            column.set_symbol_id(row_idx, id)?;
                        } else {
                            return Err(Error::InvalidData("symbol expects string".to_string()));
                        }
                    } else {
                        column.set_value(row_idx, normalized)?;
                    }
                }
            }
            updated += 1;
        }
        Ok(updated)
    }

    pub fn delete(&mut self, table: &str, filter: Option<&Expr>) -> Result<u64> {
        let Some(table) = self.tables.get_mut(table) else {
            return Ok(0);
        };
        if filter.is_none() {
            let count = table.row_count as u64;
            for column in &mut table.columns {
                *column = ColumnData::new(column.ty);
            }
            table.row_count = 0;
            return Ok(count);
        }
        let expr = filter.expect("filter checked");
        let access = TableAccess::new(table);
        let mut keep_indices = Vec::with_capacity(table.row_count);
        for row_idx in 0..table.row_count {
            match eval_predicate_at(expr, &access, &table.schema, row_idx) {
                Ok(true) => {}
                Ok(false) => keep_indices.push(row_idx),
                Err(_) => keep_indices.push(row_idx),
            }
        }
        let removed = (table.row_count - keep_indices.len()) as u64;
        for column in &mut table.columns {
            *column = column.take_rows(&keep_indices, &table.symbols)?;
        }
        table.row_count = keep_indices.len();
        Ok(removed)
    }


    pub fn query_col(&self, query: &ColumnQuery) -> Result<ResultSet> {
        let table = self
            .tables
            .get(&query.table)
            .ok_or_else(|| Error::InvalidData("table not found".to_string()))?;
        let schema = &table.schema;
        let col_map = resolve_column_map(schema, &query.columns)?;
        let access = TableAccess::new(table);
        let mut row_indices = Vec::new();
        if query.filter.is_empty() {
            row_indices.extend(0..table.row_count);
        } else {
            for row_idx in 0..table.row_count {
                if eval_column_filter(&query.filter, &access, &col_map, row_idx)? {
                    row_indices.push(row_idx);
                }
            }
        }

        let mut out_schema = Vec::with_capacity(query.project.len());
        for item in &query.project {
            let (name, col_type) = match &item.expr {
                ColumnProjectExpr::Column(idx) => {
                    let idx = *idx as usize;
                    if idx >= col_map.len() {
                        return Err(Error::InvalidData("project column out of range".to_string()));
                    }
                    let schema_idx = col_map[idx];
                    let col_type = schema.columns()[schema_idx].col_type;
                    (item.name.clone(), col_type)
                }
                ColumnProjectExpr::Literal(value) => (item.name.clone(), value.column_type()),
            };
            out_schema.push(ColumnSpec {
                name,
                col_type,
                nullable: true,
                default: None,
            });
        }
        let result_schema = Schema::new(out_schema)?;
        let mut rows = Vec::with_capacity(row_indices.len());
        for row_idx in row_indices {
            let mut values = Vec::with_capacity(query.project.len());
            for item in &query.project {
                let value = match &item.expr {
                    ColumnProjectExpr::Column(idx) => {
                        let idx = *idx as usize;
                        if idx >= col_map.len() {
                            return Err(Error::InvalidData("project column out of range".to_string()));
                        }
                        let schema_idx = col_map[idx];
                        access
                            .value_at(schema_idx, row_idx)
                            .unwrap_or(ValueRef::Null)
                            .to_value()
                    }
                    ColumnProjectExpr::Literal(value) => value.clone(),
                };
                values.push(value);
            }
            rows.push(Row { values });
        }
        Ok(ResultSet {
            schema: result_schema,
            rows,
        })
    }

}

fn load_column_from_store(
    store: &SplayedStore,
    table: &str,
    column: &ColumnSpec,
) -> Result<(ColumnData, usize)> {
    let table_dir = store.table_dir(table);
    let data_path = table_dir.join(&column.name);
    let nulls_path = table_dir.join(format!("{}.n", column.name));
    let nulls_len = std::fs::metadata(&nulls_path)?.len() as usize;
    let row_count = nulls_len;
    let nulls = NullsStorage::Writable(MmapWrite::open(&nulls_path, nulls_len, 4096)?);

    let data = match column.col_type {
        ColumnType::I64 => {
            let used = row_count * std::mem::size_of::<i64>();
            ColumnStorage::MmapI64W(MmapWrite::open(&data_path, used, 4096)?)
        }
        ColumnType::F64 => {
            let used = row_count * std::mem::size_of::<f64>();
            ColumnStorage::MmapF64W(MmapWrite::open(&data_path, used, 4096)?)
        }
        ColumnType::Bool => {
            let used = row_count;
            ColumnStorage::MmapBoolW(MmapWrite::open(&data_path, used, 4096)?)
        }
        ColumnType::Timestamp => {
            let used = row_count * std::mem::size_of::<i64>();
            ColumnStorage::MmapTimestampW(MmapWrite::open(&data_path, used, 4096)?)
        }
        ColumnType::Symbol => {
            let used = row_count * std::mem::size_of::<u32>();
            ColumnStorage::MmapSymbolW(MmapWrite::open(&data_path, used, 4096)?)
        }
        ColumnType::String => {
            let offsets_path = table_dir.join(format!("{}.o", column.name));
            let (offsets_len, data_used) =
                load_varlen_offsets(&data_path, &offsets_path, row_count)?;
            let offsets = MmapWrite::open(&offsets_path, offsets_len, 4096)?;
            let data = MmapWrite::open(&data_path, data_used, 4096)?;
            ColumnStorage::MmapVarStringW { offsets, data }
        }
        ColumnType::Bytes => {
            let offsets_path = table_dir.join(format!("{}.o", column.name));
            let (offsets_len, data_used) =
                load_varlen_offsets(&data_path, &offsets_path, row_count)?;
            let offsets = MmapWrite::open(&offsets_path, offsets_len, 4096)?;
            let data = MmapWrite::open(&data_path, data_used, 4096)?;
            ColumnStorage::MmapVarBytesW { offsets, data }
        }
    };
    Ok((ColumnData::new_mmap(column.col_type, data, nulls), row_count))
}

fn init_mmap_column_data(
    store: &SplayedStore,
    table: &str,
    column: &ColumnSpec,
) -> Result<ColumnData> {
    let table_dir = store.table_dir(table);
    let data_path = table_dir.join(&column.name);
    let nulls_path = table_dir.join(format!("{}.n", column.name));
    let nulls = NullsStorage::Writable(MmapWrite::open(&nulls_path, 0, 4096)?);
    let data = match column.col_type {
        ColumnType::I64 => ColumnStorage::MmapI64W(MmapWrite::open(&data_path, 0, 4096)?),
        ColumnType::F64 => ColumnStorage::MmapF64W(MmapWrite::open(&data_path, 0, 4096)?),
        ColumnType::Bool => ColumnStorage::MmapBoolW(MmapWrite::open(&data_path, 0, 4096)?),
        ColumnType::Timestamp => {
            ColumnStorage::MmapTimestampW(MmapWrite::open(&data_path, 0, 4096)?)
        }
        ColumnType::Symbol => ColumnStorage::MmapSymbolW(MmapWrite::open(&data_path, 0, 4096)?),
        ColumnType::String => {
            let offsets_path = table_dir.join(format!("{}.o", column.name));
            let offsets = MmapWrite::open(&offsets_path, 0, 4096)?;
            let data = MmapWrite::open(&data_path, 0, 4096)?;
            ColumnStorage::MmapVarStringW { offsets, data }
        }
        ColumnType::Bytes => {
            let offsets_path = table_dir.join(format!("{}.o", column.name));
            let offsets = MmapWrite::open(&offsets_path, 0, 4096)?;
            let data = MmapWrite::open(&data_path, 0, 4096)?;
            ColumnStorage::MmapVarBytesW { offsets, data }
        }
    };
    Ok(ColumnData::new_mmap(column.col_type, data, nulls))
}

fn load_varlen_offsets(
    data_path: &Path,
    offsets_path: &Path,
    row_count: usize,
) -> Result<(usize, usize)> {
    if offsets_path.exists() {
        let file_len = std::fs::metadata(offsets_path)?.len() as usize;
        let offsets_len = row_count * 8;
        if file_len < offsets_len {
            return Err(Error::InvalidData("offsets truncated".to_string()));
        }
        if row_count == 0 {
            return Ok((offsets_len, 0));
        }
        let mut offsets_file = File::open(offsets_path)?;
        offsets_file.seek(std::io::SeekFrom::Start((row_count as u64 - 1) * 8))?;
        let mut last_buf = [0u8; 8];
        offsets_file.read_exact(&mut last_buf)?;
        let last_offset = u64::from_le_bytes(last_buf) as usize;
        let mut data_file = File::open(data_path)?;
        let data_len = data_file.metadata()?.len() as usize;
        data_file.seek(std::io::SeekFrom::Start(last_offset as u64))?;
        let mut len_buf = [0u8; 4];
        data_file.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let data_used = last_offset
            .checked_add(4)
            .and_then(|value| value.checked_add(len))
            .ok_or_else(|| Error::InvalidData("bytes length overflow".to_string()))?;
        if data_used > data_len {
            return Err(Error::InvalidData("data truncated".to_string()));
        }
        return Ok((offsets_len, data_used));
    }

    let mut data_file = File::open(data_path)?;
    let data_len = data_file.metadata()?.len() as usize;
    let mut offsets_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(offsets_path)?;
    let mut offset = 0usize;
    for _ in 0..row_count {
        offsets_file.write_all(&(offset as u64).to_le_bytes())?;
        if offset + 4 > data_len {
            return Err(Error::InvalidData("bytes column truncated".to_string()));
        }
        let mut len_buf = [0u8; 4];
        data_file.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        offset = offset
            .checked_add(4)
            .and_then(|value| value.checked_add(len))
            .ok_or_else(|| Error::InvalidData("bytes length overflow".to_string()))?;
        if offset > data_len {
            return Err(Error::InvalidData("bytes column truncated".to_string()));
        }
        if len > 0 {
            data_file.seek(std::io::SeekFrom::Current(len as i64))?;
        }
    }
    Ok((row_count * 8, offset))
}

fn infer_column_type_from_rows(rows: &[Vec<Value>], col_pos: usize) -> ColumnType {
    for row in rows {
        if let Some(value) = row.get(col_pos) {
            if !value.is_null() {
                return value.column_type();
            }
        }
    }
    ColumnType::String
}

fn table_row(table: &Table, index: usize) -> Option<Row> {
    if index >= table.row_count {
        return None;
    }
    let mut values = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        let value = if column.ty == ColumnType::Symbol {
            column
                .get_ref_with_symbols(index, &table.symbols)
                .unwrap_or(ValueRef::Null)
                .to_value()
        } else {
            column.get_ref(index).unwrap_or(ValueRef::Null).to_value()
        };
        values.push(value);
    }
    Some(Row { values })
}

fn table_rows(table: &Table) -> Vec<Row> {
    let mut rows = Vec::with_capacity(table.row_count);
    for idx in 0..table.row_count {
        if let Some(row) = table_row(table, idx) {
            rows.push(row);
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::{load_varlen_offsets, MemStore};
    use crate::expr::{BinaryOp, Expr};
    use crate::pxl::{ColumnInstr, ColumnProjectExpr, ColumnProjectItem, ColumnQuery};
    use crate::storage::{SplayedStore, StoreConfig};
    use crate::types::{ColumnSpec, ColumnType, Schema, Value};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        path.push(format!("pxd_{name}_{nanos}"));
        path
    }

    #[test]
    fn insert_and_select_roundtrip() {
        let mut mem = MemStore::new();
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
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
        .expect("schema");
        mem.create_table("ticks", schema).expect("create");
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[vec![Value::String("AAPL".to_string()), Value::F64(10.0)]],
        )
        .expect("insert");

        let query = ColumnQuery {
            table: "ticks".to_string(),
            columns: vec!["symbol".to_string(), "price".to_string()],
            filter: vec![
                ColumnInstr::PushCol(0),
                ColumnInstr::PushLit(Value::String("AAPL".to_string())),
                ColumnInstr::Cmp(BinaryOp::Eq),
            ],
            project: vec![ColumnProjectItem {
                name: "price".to_string(),
                expr: ColumnProjectExpr::Column(1),
            }],
        };
        let result = mem.query_col(&query).expect("query_col");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::F64(10.0));
    }

    #[test]
    fn query_col_executes_filter_and_project() {
        let mut mem = MemStore::new();
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
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
        .expect("schema");
        mem.create_table("ticks", schema).expect("create");
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(12.0)],
            ],
        )
        .expect("insert");

        let query = ColumnQuery {
            table: "ticks".to_string(),
            columns: vec!["symbol".to_string(), "price".to_string()],
            filter: vec![
                ColumnInstr::PushCol(0),
                ColumnInstr::PushLit(Value::String("AAPL".to_string())),
                ColumnInstr::Cmp(BinaryOp::Eq),
            ],
            project: vec![ColumnProjectItem {
                name: "price".to_string(),
                expr: ColumnProjectExpr::Column(1),
            }],
        };
        let result = mem.query_col(&query).expect("query_col");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::F64(10.0));
    }

    #[test]
    fn table_accessors_expose_rows() {
        let mut mem = MemStore::new();
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(12.0)],
            ],
        )
        .expect("insert");

        let schema = mem.table_schema("ticks").expect("schema");
        let price_idx = schema.column_index("price").expect("price idx");
        assert_eq!(mem.table_row_count("ticks"), Some(2));

        let rows = mem.table_rows("ticks").expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[price_idx], Value::F64(10.0));

        let row = mem.table_row("ticks", 1).expect("row");
        assert_eq!(row.values[price_idx], Value::F64(12.0));
    }

    #[test]
    fn update_adds_column_and_filters() {
        let mut mem = MemStore::new();
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(5.0)],
            ],
        )
        .expect("insert");

        let filter = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column("symbol".to_string())),
            right: Box::new(Expr::Literal(Value::String("AAPL".to_string()))),
        };
        let updated = mem
            .update(
                "ticks",
                &[("notes".to_string(), Value::String("hot".to_string()))],
                Some(&filter),
            )
            .expect("update");
        assert_eq!(updated, 1);
        let rows = mem.table_rows("ticks").expect("rows");
        let notes_idx = mem
            .table_schema("ticks")
            .expect("schema")
            .column_index("notes")
            .expect("notes idx");
        let notes: Vec<Value> = rows.into_iter().map(|row| row.values[notes_idx].clone()).collect();
        assert_eq!(notes.iter().filter(|v| **v == Value::String("hot".to_string())).count(), 1);
    }

    #[test]
    fn delete_with_filter_removes_matches() {
        let mut mem = MemStore::new();
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(5.0)],
                vec![Value::String("AAPL".to_string()), Value::F64(12.0)],
            ],
        )
        .expect("insert");

        let filter = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column("symbol".to_string())),
            right: Box::new(Expr::Literal(Value::String("MSFT".to_string()))),
        };
        let removed = mem.delete("ticks", Some(&filter)).expect("delete");
        assert_eq!(removed, 1);
        assert_eq!(mem.table_row_count("ticks"), Some(2));
    }

    #[test]
    fn alter_column_add_rename_drop() {
        let mut mem = MemStore::new();
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
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
        .expect("schema");
        mem.create_table("ticks", schema).expect("create");
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[vec![Value::String("AAPL".to_string()), Value::F64(10.0)]],
        )
        .expect("insert");

        mem.alter_add_column(
            "ticks",
            ColumnSpec {
                name: "lot".to_string(),
                col_type: ColumnType::I64,
                nullable: false,
                default: Some(Value::I64(100)),
            },
        )
        .expect("alter add");
        let schema = mem.table_schema("ticks").expect("schema");
        let lot_idx = schema.column_index("lot").expect("lot idx");
        let row = mem.table_row("ticks", 0).expect("row");
        assert_eq!(row.values[lot_idx], Value::I64(100));

        mem.alter_rename_column("ticks", "lot", "lots")
            .expect("rename");
        let schema = mem.table_schema("ticks").expect("schema");
        assert!(schema.column_index("lot").is_none());
        assert!(schema.column_index("lots").is_some());

        mem.alter_drop_column("ticks", "price")
            .expect("drop column");
        let schema = mem.table_schema("ticks").expect("schema");
        assert!(schema.column_index("price").is_none());
        let row = mem.table_row("ticks", 0).expect("row");
        assert_eq!(row.values.len(), schema.columns().len());
    }

    #[test]
    fn alter_set_default_applies_on_insert() {
        let mut mem = MemStore::new();
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
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
        .expect("schema");
        mem.create_table("ticks", schema).expect("create");
        mem.alter_set_default("ticks", "price", Some(Value::I64(7)))
            .expect("set default");
        mem.insert(
            "ticks",
            &["symbol".to_string()],
            &[vec![Value::String("AAPL".to_string())]],
        )
        .expect("insert");
        let schema = mem.table_schema("ticks").expect("schema");
        let price_idx = schema.column_index("price").expect("price idx");
        let row = mem.table_row("ticks", 0).expect("row");
        assert_eq!(row.values[price_idx], Value::F64(7.0));
    }

    #[test]
    fn update_flushes_to_store() {
        let root = temp_root("update_flush");
        let cfg = StoreConfig {
            root: root.clone(),
            partition: "2026.04.11".to_string(),
        };
        let store = SplayedStore::open(cfg.clone()).expect("open store");
        let mut mem = MemStore::with_store(store);

        let columns = vec!["sym".to_string(), "price".to_string()];
        let rows = vec![
            vec![Value::String("A".to_string()), Value::I64(1)],
            vec![Value::Null, Value::I64(2)],
        ];
        mem.insert("ticks", &columns, &rows).expect("insert");
        mem.update(
            "ticks",
            &[("price".to_string(), Value::I64(3))],
            None,
        )
        .expect("update");

        drop(mem);
        let store = SplayedStore::open(cfg).expect("reopen store");
        let mut reloaded = MemStore::with_store(store);
        reloaded.load_from_store().expect("load");

        let row0 = reloaded.table_row("ticks", 0).expect("row0");
        let row1 = reloaded.table_row("ticks", 1).expect("row1");
        assert_eq!(row0.values[0], Value::String("A".to_string()));
        assert_eq!(row1.values[0], Value::Null);
        assert_eq!(row0.values[1], Value::I64(3));
        assert_eq!(row1.values[1], Value::I64(3));
    }

    #[test]
    fn symbol_column_flushes_to_store() {
        let root = temp_root("symbol_flush");
        let cfg = StoreConfig {
            root: root.clone(),
            partition: "2026.04.11".to_string(),
        };
        let store = SplayedStore::open(cfg.clone()).expect("open store");
        let mut mem = MemStore::with_store(store);
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "sym".to_string(),
                col_type: ColumnType::Symbol,
                nullable: false,
                default: None,
            },
            ColumnSpec {
                name: "price".to_string(),
                col_type: ColumnType::I64,
                nullable: false,
                default: None,
            },
        ])
        .expect("schema");
        mem.create_table("ticks", schema).expect("create");
        mem.insert(
            "ticks",
            &["sym".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::I64(1)],
                vec![Value::String("MSFT".to_string()), Value::I64(2)],
            ],
        )
        .expect("insert");
        mem.flush_to_store().expect("flush");

        let store = SplayedStore::open(cfg).expect("reopen store");
        let mut reloaded = MemStore::with_store(store);
        reloaded.load_from_store().expect("load");
        let row0 = reloaded.table_row("ticks", 0).expect("row0");
        let row1 = reloaded.table_row("ticks", 1).expect("row1");
        assert_eq!(row0.values[0], Value::String("AAPL".to_string()));
        assert_eq!(row1.values[0], Value::String("MSFT".to_string()));
    }

    #[test]
    fn load_varlen_offsets_rebuilds_file() {
        let root = temp_root("offsets");
        fs::create_dir_all(&root).expect("mkdir");
        let data_path = root.join("data");
        let offsets_path = root.join("data.o");
        let mut data = Vec::new();
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"abc");
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(b"z");
        fs::write(&data_path, &data).expect("write");

        let (offsets_len, data_used) =
            load_varlen_offsets(&data_path, &offsets_path, 2).expect("offsets");
        assert_eq!(offsets_len, 16);
        assert_eq!(data_used, data.len());
        let offsets = fs::read(&offsets_path).expect("read offsets");
        assert_eq!(offsets.len(), 16);
        let first = u64::from_le_bytes([
            offsets[0],
            offsets[1],
            offsets[2],
            offsets[3],
            offsets[4],
            offsets[5],
            offsets[6],
            offsets[7],
        ]);
        let second = u64::from_le_bytes([
            offsets[8],
            offsets[9],
            offsets[10],
            offsets[11],
            offsets[12],
            offsets[13],
            offsets[14],
            offsets[15],
        ]);
        assert_eq!(first, 0);
        assert_eq!(second, 7);
        let _ = fs::remove_dir_all(root);
    }
}
