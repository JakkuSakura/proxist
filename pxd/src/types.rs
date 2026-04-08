use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ColumnType {
    I64 = 1,
    F64 = 2,
    Bool = 3,
    String = 4,
    Bytes = 5,
    Timestamp = 6,
}

impl ColumnType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(ColumnType::I64),
            2 => Ok(ColumnType::F64),
            3 => Ok(ColumnType::Bool),
            4 => Ok(ColumnType::String),
            5 => Ok(ColumnType::Bytes),
            6 => Ok(ColumnType::Timestamp),
            _ => Err(Error::Protocol("unknown column type")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<ColumnSpec>,
    index: HashMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnSpec>) -> Result<Self> {
        let mut index = HashMap::with_capacity(columns.len() * 2);
        for (idx, column) in columns.iter().enumerate() {
            let key = column.name.to_ascii_lowercase();
            if index.contains_key(&key) {
                return Err(Error::InvalidData(format!(
                    "duplicate column name: {}",
                    column.name
                )));
            }
            index.insert(key, idx);
        }
        Ok(Self { columns, index })
    }

    pub fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.index.get(&name.to_ascii_lowercase()).copied()
    }

    pub fn add_column(&mut self, column: ColumnSpec) -> Result<()> {
        let key = column.name.to_ascii_lowercase();
        if self.index.contains_key(&key) {
            return Err(Error::InvalidData(format!(
                "duplicate column name: {}",
                column.name
            )));
        }
        let idx = self.columns.len();
        self.columns.push(column);
        self.index.insert(key, idx);
        Ok(())
    }

    pub fn with_aliases(mut self, aliases: &[(String, usize)]) -> Self {
        for (alias, idx) in aliases {
            self.index
                .entry(alias.to_ascii_lowercase())
                .or_insert(*idx);
        }
        self
    }

    pub fn merge_with_prefix(
        left: (&str, &Schema),
        right: (&str, &Schema),
    ) -> Result<Self> {
        let mut columns = Vec::with_capacity(left.1.columns.len() + right.1.columns.len());
        for col in &left.1.columns {
            columns.push(ColumnSpec {
                name: format!("{}.{}", left.0, col.name),
                col_type: col.col_type,
                nullable: col.nullable,
            });
        }
        for col in &right.1.columns {
            columns.push(ColumnSpec {
                name: format!("{}.{}", right.0, col.name),
                col_type: col.col_type,
                nullable: col.nullable,
            });
        }
        let mut schema = Schema::new(columns)?;
        let mut aliases = Vec::new();
        for (idx, col) in left.1.columns.iter().enumerate() {
            aliases.push((col.name.clone(), idx));
        }
        let right_offset = left.1.columns.len();
        for (idx, col) in right.1.columns.iter().enumerate() {
            aliases.push((col.name.clone(), right_offset + idx));
        }
        schema = schema.with_aliases(&aliases);
        Ok(schema)
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0u8.hash(state),
            Value::I64(v) => {
                1u8.hash(state);
                v.hash(state);
            }
            Value::F64(v) => {
                2u8.hash(state);
                v.to_bits().hash(state);
            }
            Value::Bool(v) => {
                3u8.hash(state);
                v.hash(state);
            }
            Value::String(v) => {
                4u8.hash(state);
                v.hash(state);
            }
            Value::Bytes(v) => {
                5u8.hash(state);
                v.hash(state);
            }
            Value::Timestamp(v) => {
                6u8.hash(state);
                v.hash(state);
            }
        }
    }
}

impl Value {
    pub fn column_type(&self) -> ColumnType {
        match self {
            Value::Null => ColumnType::String,
            Value::I64(_) => ColumnType::I64,
            Value::F64(_) => ColumnType::F64,
            Value::Bool(_) => ColumnType::Bool,
            Value::String(_) => ColumnType::String,
            Value::Bytes(_) => ColumnType::Bytes,
            Value::Timestamp(_) => ColumnType::Timestamp,
        }
    }

    pub fn cmp_for_order(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(a), Value::I64(b)) => Some(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
            (Value::F64(a), Value::F64(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::I64(v) => Some(*v),
            Value::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F64(v) => Some(*v),
            Value::I64(v) => Some(*v as f64),
            Value::Timestamp(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

pub fn coerce_value(value: Value, target: ColumnType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match (value, target) {
        (Value::I64(v), ColumnType::I64) => Ok(Value::I64(v)),
        (Value::I64(v), ColumnType::Timestamp) => Ok(Value::Timestamp(v)),
        (Value::I64(v), ColumnType::F64) => Ok(Value::F64(v as f64)),
        (Value::F64(v), ColumnType::F64) => Ok(Value::F64(v)),
        (Value::F64(v), ColumnType::I64) => Ok(Value::I64(v as i64)),
        (Value::Bool(v), ColumnType::Bool) => Ok(Value::Bool(v)),
        (Value::String(v), ColumnType::String) => Ok(Value::String(v)),
        (Value::Bytes(v), ColumnType::Bytes) => Ok(Value::Bytes(v)),
        (Value::Timestamp(v), ColumnType::Timestamp) => Ok(Value::Timestamp(v)),
        (Value::Timestamp(v), ColumnType::I64) => Ok(Value::I64(v)),
        (Value::Timestamp(v), ColumnType::F64) => Ok(Value::F64(v as f64)),
        (Value::String(v), ColumnType::Bytes) => Ok(Value::Bytes(v.into_bytes())),
        (value, target) => Err(Error::InvalidData(format!(
            "value type mismatch: {value:?} -> {target:?}"
        ))),
    }
}

pub fn infer_column_type(values: &[Value]) -> ColumnType {
    for value in values {
        if !value.is_null() {
            return value.column_type();
        }
    }
    ColumnType::String
}
