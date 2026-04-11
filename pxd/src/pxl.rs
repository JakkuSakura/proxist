use std::io::{Read, Write};

use crate::error::{Error, Result};
use crate::expr::{BinaryOp, Expr};
use crate::types::{ColumnSpec, ColumnType, Schema, Value};

const MAGIC: [u8; 2] = *b"PX";
const VERSION: u8 = 2;
const HEADER_LEN: usize = 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Ping = 1,
    Pong = 2,
    Error = 3,
    Create = 10,
    Insert = 11,
    Update = 12,
    Delete = 13,
    Result = 15,
    Schema = 16,
    AlterAddColumn = 17,
    AlterDropColumn = 18,
    AlterRenameColumn = 19,
    AlterSetDefault = 20,
    DropTable = 21,
    RenameTable = 22,
    QueryCol = 24,
}

impl Op {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Op::Ping),
            2 => Ok(Op::Pong),
            3 => Ok(Op::Error),
            10 => Ok(Op::Create),
            11 => Ok(Op::Insert),
            12 => Ok(Op::Update),
            13 => Ok(Op::Delete),
            15 => Ok(Op::Result),
            16 => Ok(Op::Schema),
            17 => Ok(Op::AlterAddColumn),
            18 => Ok(Op::AlterDropColumn),
            19 => Ok(Op::AlterRenameColumn),
            20 => Ok(Op::AlterSetDefault),
            21 => Ok(Op::DropTable),
            22 => Ok(Op::RenameTable),
            24 => Ok(Op::QueryCol),
            _ => Err(Error::Protocol("unknown op")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub filter: Vec<ColumnInstr>,
    pub project: Vec<ColumnProjectItem>,
}

#[derive(Debug, Clone)]
pub enum ColumnInstr {
    PushCol(u16),
    PushLit(Value),
    Cmp(BinaryOp),
    And,
    Or,
    Not,
}

#[derive(Debug, Clone)]
pub struct ColumnProjectItem {
    pub name: String,
    pub expr: ColumnProjectExpr,
}

#[derive(Debug, Clone)]
pub enum ColumnProjectExpr {
    Column(u16),
    Literal(Value),
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub flags: u8,
    pub req_id: u32,
    pub op: Op,
    pub payload: Vec<u8>,
}

pub fn encode_frame(frame: &Frame) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + frame.payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(frame.flags);
    out.push(frame.op as u8);
    out.push(0);
    out.extend_from_slice(&frame.req_id.to_le_bytes());
    out.extend_from_slice(&(frame.payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&frame.payload);
    out
}

pub fn read_frame<R: Read>(reader: &mut R) -> Result<Option<Frame>> {
    let mut header = [0u8; HEADER_LEN];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    if header[0..2] != MAGIC || header[2] != VERSION {
        return Err(Error::Protocol("invalid frame header"));
    }
    let flags = header[3];
    let op = Op::from_u8(header[4])?;
    let req_id = u32::from_le_bytes([header[6], header[7], header[8], header[9]]);
    let len = u32::from_le_bytes([header[10], header[11], header[12], header[13]]) as usize;
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload)?;
    Ok(Some(Frame {
        flags,
        req_id,
        op,
        payload,
    }))
}

pub fn write_frame<W: Write>(writer: &mut W, frame: &Frame) -> Result<()> {
    let bytes = encode_frame(frame);
    writer.write_all(&bytes)?;
    Ok(())
}

pub fn encode_error_payload(message: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, message)?;
    Ok(out)
}

pub fn encode_schema_payload(table: &str, schema: &Schema) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_schema_with_defaults(&mut out, schema)?;
    Ok(out)
}

pub fn decode_schema_payload(bytes: &[u8]) -> Result<(String, Schema)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let schema = read_schema_with_defaults(&mut cursor)?;
    Ok((table, schema))
}

pub fn encode_alter_add_column_payload(table: &str, column: &ColumnSpec) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_column_spec(&mut out, column)?;
    Ok(out)
}

pub fn decode_alter_add_column_payload(bytes: &[u8]) -> Result<(String, ColumnSpec)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let column = read_column_spec(&mut cursor)?;
    Ok((table, column))
}

pub fn encode_alter_drop_column_payload(table: &str, column: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_string(&mut out, column)?;
    Ok(out)
}

pub fn decode_alter_drop_column_payload(bytes: &[u8]) -> Result<(String, String)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let column = read_string(&mut cursor)?;
    Ok((table, column))
}

pub fn encode_alter_rename_column_payload(
    table: &str,
    from: &str,
    to: &str,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_string(&mut out, from)?;
    write_string(&mut out, to)?;
    Ok(out)
}

pub fn decode_alter_rename_column_payload(bytes: &[u8]) -> Result<(String, String, String)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let from = read_string(&mut cursor)?;
    let to = read_string(&mut cursor)?;
    Ok((table, from, to))
}

pub fn encode_alter_set_default_payload(
    table: &str,
    column: &str,
    default: Option<&Value>,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_string(&mut out, column)?;
    write_default_value(&mut out, default)?;
    Ok(out)
}

pub fn decode_alter_set_default_payload(bytes: &[u8]) -> Result<(String, String, Option<Value>)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let column = read_string(&mut cursor)?;
    let default = read_default_value(&mut cursor)?;
    Ok((table, column, default))
}

pub fn encode_drop_table_payload(table: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    Ok(out)
}

pub fn decode_drop_table_payload(bytes: &[u8]) -> Result<String> {
    let mut cursor = Cursor::new(bytes);
    read_string(&mut cursor)
}

pub fn encode_rename_table_payload(from: &str, to: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, from)?;
    write_string(&mut out, to)?;
    Ok(out)
}

pub fn decode_rename_table_payload(bytes: &[u8]) -> Result<(String, String)> {
    let mut cursor = Cursor::new(bytes);
    let from = read_string(&mut cursor)?;
    let to = read_string(&mut cursor)?;
    Ok((from, to))
}

pub fn encode_insert_payload(
    table: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_string_list(&mut out, columns)?;
    write_u32(&mut out, rows.len() as u32)?;
    for row in rows {
        if row.len() != columns.len() {
            return Err(Error::InvalidData("row column count mismatch".to_string()));
        }
        write_values(&mut out, row)?;
    }
    Ok(out)
}

pub fn decode_insert_payload(bytes: &[u8]) -> Result<(String, Vec<String>, Vec<Vec<Value>>)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let columns = read_string_list(&mut cursor)?;
    let row_count = read_u32(&mut cursor)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        rows.push(read_values(&mut cursor, columns.len())?);
    }
    Ok((table, columns, rows))
}

pub fn encode_update_payload(
    table: &str,
    assignments: &[(String, Value)],
    filter: Option<&Expr>,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_u32(&mut out, assignments.len() as u32)?;
    for (col, value) in assignments {
        write_string(&mut out, col)?;
        write_value(&mut out, value)?;
    }
    write_expr_option(&mut out, filter)?;
    Ok(out)
}

pub fn decode_update_payload(
    bytes: &[u8],
) -> Result<(String, Vec<(String, Value)>, Option<Expr>)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let count = read_u32(&mut cursor)? as usize;
    let mut assignments = Vec::with_capacity(count);
    for _ in 0..count {
        let col = read_string(&mut cursor)?;
        let value = read_value(&mut cursor)?;
        assignments.push((col, value));
    }
    let filter = read_expr_option(&mut cursor)?;
    Ok((table, assignments, filter))
}

pub fn encode_delete_payload(table: &str, filter: Option<&Expr>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_expr_option(&mut out, filter)?;
    Ok(out)
}

pub fn decode_delete_payload(bytes: &[u8]) -> Result<(String, Option<Expr>)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let filter = read_expr_option(&mut cursor)?;
    Ok((table, filter))
}

pub fn encode_query_col_payload(query: &ColumnQuery) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_column_query_payload(&mut out, query)?;
    Ok(out)
}

pub fn decode_query_col_payload(bytes: &[u8]) -> Result<ColumnQuery> {
    let mut cursor = Cursor::new(bytes);
    read_column_query_payload(&mut cursor)
}

fn write_column_instr(out: &mut Vec<u8>, instr: &ColumnInstr) -> Result<()> {
    match instr {
        ColumnInstr::PushCol(idx) => {
            out.push(1);
            write_u16(out, *idx)
        }
        ColumnInstr::PushLit(value) => {
            out.push(2);
            write_value(out, value)
        }
        ColumnInstr::Cmp(op) => {
            out.push(3);
            out.push(*op as u8);
            Ok(())
        }
        ColumnInstr::And => {
            out.push(4);
            Ok(())
        }
        ColumnInstr::Or => {
            out.push(5);
            Ok(())
        }
        ColumnInstr::Not => {
            out.push(6);
            Ok(())
        }
    }
}

fn read_column_instr(cursor: &mut Cursor<'_>) -> Result<ColumnInstr> {
    let tag = read_byte(cursor)?;
    match tag {
        1 => Ok(ColumnInstr::PushCol(read_u16(cursor)?)),
        2 => Ok(ColumnInstr::PushLit(read_value(cursor)?)),
        3 => Ok(ColumnInstr::Cmp(BinaryOp::from_u8(read_byte(cursor)?)?)),
        4 => Ok(ColumnInstr::And),
        5 => Ok(ColumnInstr::Or),
        6 => Ok(ColumnInstr::Not),
        _ => Err(Error::Protocol("unknown column instr tag")),
    }
}

fn write_project_expr(out: &mut Vec<u8>, expr: &ColumnProjectExpr) -> Result<()> {
    match expr {
        ColumnProjectExpr::Column(idx) => {
            out.push(1);
            write_u16(out, *idx)
        }
        ColumnProjectExpr::Literal(value) => {
            out.push(2);
            write_value(out, value)
        }
    }
}

fn read_project_expr(cursor: &mut Cursor<'_>) -> Result<ColumnProjectExpr> {
    let tag = read_byte(cursor)?;
    match tag {
        1 => Ok(ColumnProjectExpr::Column(read_u16(cursor)?)),
        2 => Ok(ColumnProjectExpr::Literal(read_value(cursor)?)),
        _ => Err(Error::Protocol("unknown project expr tag")),
    }
}

fn write_column_query_payload(out: &mut Vec<u8>, query: &ColumnQuery) -> Result<()> {
    write_string(out, &query.table)?;
    write_u32(out, query.columns.len() as u32)?;
    for name in &query.columns {
        write_string(out, name)?;
    }
    write_u32(out, query.filter.len() as u32)?;
    for instr in &query.filter {
        write_column_instr(out, instr)?;
    }
    write_u32(out, query.project.len() as u32)?;
    for item in &query.project {
        write_string(out, &item.name)?;
        write_project_expr(out, &item.expr)?;
    }
    Ok(())
}

fn read_column_query_payload(cursor: &mut Cursor<'_>) -> Result<ColumnQuery> {
    let table = read_string(cursor)?;
    let col_count = read_u32(cursor)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        columns.push(read_string(cursor)?);
    }
    let filter_count = read_u32(cursor)? as usize;
    let mut filter = Vec::with_capacity(filter_count);
    for _ in 0..filter_count {
        filter.push(read_column_instr(cursor)?);
    }
    let proj_count = read_u32(cursor)? as usize;
    let mut project = Vec::with_capacity(proj_count);
    for _ in 0..proj_count {
        let name = read_string(cursor)?;
        let expr = read_project_expr(cursor)?;
        project.push(ColumnProjectItem { name, expr });
    }
    Ok(ColumnQuery {
        table,
        columns,
        filter,
        project,
    })
}

pub fn encode_scalar_payload(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_value(&mut out, value)?;
    Ok(out)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.pos + len > self.bytes.len() {
            return Err(Error::Protocol("payload truncated"));
        }
        let start = self.pos;
        let end = start + len;
        self.pos = end;
        Ok(&self.bytes[start..end])
    }
}

fn write_u32(out: &mut Vec<u8>, value: u32) -> Result<()> {
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_u32(cursor: &mut Cursor<'_>) -> Result<u32> {
    let bytes = cursor.take(4)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn write_u16(out: &mut Vec<u8>, value: u16) -> Result<()> {
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_u16(cursor: &mut Cursor<'_>) -> Result<u16> {
    let bytes = cursor.take(2)?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn write_i64(out: &mut Vec<u8>, value: i64) -> Result<()> {
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_i64(cursor: &mut Cursor<'_>) -> Result<i64> {
    let bytes = cursor.take(8)?;
    Ok(i64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn write_f64(out: &mut Vec<u8>, value: f64) -> Result<()> {
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_f64(cursor: &mut Cursor<'_>) -> Result<f64> {
    let bytes = cursor.take(8)?;
    Ok(f64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    write_u32(out, bytes.len() as u32)?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_string(cursor: &mut Cursor<'_>) -> Result<String> {
    let len = read_u32(cursor)? as usize;
    let bytes = cursor.take(len)?;
    let value = std::str::from_utf8(bytes)
        .map_err(|_| Error::InvalidData("invalid utf8".to_string()))?;
    Ok(value.to_string())
}

fn write_string_list(out: &mut Vec<u8>, values: &[String]) -> Result<()> {
    write_u32(out, values.len() as u32)?;
    for value in values {
        write_string(out, value)?;
    }
    Ok(())
}

fn read_string_list(cursor: &mut Cursor<'_>) -> Result<Vec<String>> {
    let count = read_u32(cursor)? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_string(cursor)?);
    }
    Ok(values)
}

fn write_schema(out: &mut Vec<u8>, schema: &Schema) -> Result<()> {
    write_u32(out, schema.columns().len() as u32)?;
    for col in schema.columns() {
        write_string(out, &col.name)?;
        out.push(col.col_type as u8);
        out.push(col.nullable as u8);
    }
    Ok(())
}

fn read_schema(cursor: &mut Cursor<'_>) -> Result<Schema> {
    let count = read_u32(cursor)? as usize;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let name = read_string(cursor)?;
        let col_type = ColumnType::from_u8(read_byte(cursor)?)?;
        let nullable = read_byte(cursor)? != 0;
        columns.push(ColumnSpec {
            name,
            col_type,
            nullable,
            default: None,
        });
    }
    Schema::new(columns)
}

fn write_schema_with_defaults(out: &mut Vec<u8>, schema: &Schema) -> Result<()> {
    write_schema(out, schema)?;
    write_schema_defaults(out, schema)?;
    Ok(())
}

fn write_schema_defaults(out: &mut Vec<u8>, schema: &Schema) -> Result<()> {
    out.push(1);
    write_u32(out, schema.columns().len() as u32)?;
    for col in schema.columns() {
        write_default_value(out, col.default.as_ref())?;
    }
    Ok(())
}

fn read_schema_with_defaults(cursor: &mut Cursor<'_>) -> Result<Schema> {
    let mut schema = read_schema(cursor)?;
    if cursor.pos >= cursor.bytes.len() {
        return Ok(schema);
    }
    let marker = read_byte(cursor)?;
    if marker == 0 {
        return Ok(schema);
    }
    if marker != 1 {
        return Err(Error::Protocol("unknown schema defaults marker"));
    }
    let count = read_u32(cursor)? as usize;
    if count != schema.columns().len() {
        return Err(Error::InvalidData(
            "schema defaults count mismatch".to_string(),
        ));
    }
    for idx in 0..count {
        let default = read_default_value(cursor)?;
        schema.set_column_default(idx, default)?;
    }
    Ok(schema)
}

fn write_column_spec(out: &mut Vec<u8>, column: &ColumnSpec) -> Result<()> {
    write_string(out, &column.name)?;
    out.push(column.col_type as u8);
    out.push(column.nullable as u8);
    write_default_value(out, column.default.as_ref())?;
    Ok(())
}

fn read_column_spec(cursor: &mut Cursor<'_>) -> Result<ColumnSpec> {
    let name = read_string(cursor)?;
    let col_type = ColumnType::from_u8(read_byte(cursor)?)?;
    let nullable = read_byte(cursor)? != 0;
    let default = read_default_value(cursor)?;
    Ok(ColumnSpec {
        name,
        col_type,
        nullable,
        default,
    })
}

fn write_default_value(out: &mut Vec<u8>, default: Option<&Value>) -> Result<()> {
    match default {
        Some(value) => {
            out.push(1);
            write_value(out, value)?;
        }
        None => {
            out.push(0);
        }
    }
    Ok(())
}

fn read_default_value(cursor: &mut Cursor<'_>) -> Result<Option<Value>> {
    let tag = read_byte(cursor)?;
    match tag {
        0 => Ok(None),
        1 => Ok(Some(read_value(cursor)?)),
        _ => Err(Error::Protocol("unknown default tag")),
    }
}

fn write_values(out: &mut Vec<u8>, values: &[Value]) -> Result<()> {
    for value in values {
        write_value(out, value)?;
    }
    Ok(())
}

fn read_values(cursor: &mut Cursor<'_>, count: usize) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_value(cursor)?);
    }
    Ok(values)
}

fn write_value(out: &mut Vec<u8>, value: &Value) -> Result<()> {
    match value {
        Value::Null => out.push(0),
        Value::I64(v) => {
            out.push(1);
            write_i64(out, *v)?;
        }
        Value::F64(v) => {
            out.push(2);
            write_f64(out, *v)?;
        }
        Value::Bool(v) => {
            out.push(3);
            out.push(*v as u8);
        }
        Value::String(v) => {
            out.push(4);
            write_string(out, v)?;
        }
        Value::Bytes(v) => {
            out.push(5);
            write_u32(out, v.len() as u32)?;
            out.extend_from_slice(v);
        }
        Value::Timestamp(v) => {
            out.push(6);
            write_i64(out, *v)?;
        }
    }
    Ok(())
}

fn read_value(cursor: &mut Cursor<'_>) -> Result<Value> {
    let tag = read_byte(cursor)?;
    match tag {
        0 => Ok(Value::Null),
        1 => Ok(Value::I64(read_i64(cursor)?)),
        2 => Ok(Value::F64(read_f64(cursor)?)),
        3 => Ok(Value::Bool(read_byte(cursor)? != 0)),
        4 => Ok(Value::String(read_string(cursor)?)),
        5 => {
            let len = read_u32(cursor)? as usize;
            let bytes = cursor.take(len)?.to_vec();
            Ok(Value::Bytes(bytes))
        }
        6 => Ok(Value::Timestamp(read_i64(cursor)?)),
        _ => Err(Error::Protocol("unknown value tag")),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{
        decode_alter_add_column_payload, decode_alter_drop_column_payload,
        decode_alter_rename_column_payload, decode_alter_set_default_payload,
        decode_delete_payload, decode_insert_payload, decode_query_col_payload,
        decode_schema_payload, decode_update_payload,
        encode_alter_add_column_payload, encode_alter_drop_column_payload,
        encode_alter_rename_column_payload, encode_alter_set_default_payload,
        encode_delete_payload, encode_insert_payload, encode_query_col_payload,
        encode_schema_payload, encode_update_payload,
        ColumnInstr, ColumnProjectExpr, ColumnProjectItem, ColumnQuery, Frame, Op, read_frame,
        write_frame,
    };
    use crate::expr::{BinaryOp, Expr};
    use crate::types::{ColumnSpec, ColumnType, Schema, Value};

    #[test]
    fn frame_roundtrip() {
        let frame = Frame {
            flags: 1,
            req_id: 42,
            op: Op::Ping,
            payload: vec![1, 2, 3],
        };
        let mut out = Vec::new();
        write_frame(&mut out, &frame).expect("write");
        let mut cursor = Cursor::new(out);
        let decoded = read_frame(&mut cursor).expect("read").expect("frame");
        assert_eq!(decoded.flags, 1);
        assert_eq!(decoded.req_id, 42);
        assert_eq!(decoded.op, Op::Ping);
        assert_eq!(decoded.payload, vec![1, 2, 3]);
    }

    #[test]
    fn schema_payload_roundtrip() {
        let schema = Schema::new(vec![
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
        ])
        .expect("schema");
        let payload = encode_schema_payload("ticks", &schema).expect("encode");
        let (table, decoded) = decode_schema_payload(&payload).expect("decode");
        assert_eq!(table, "ticks");
        assert_eq!(decoded.columns().len(), 2);
    }

    #[test]
    fn schema_defaults_roundtrip() {
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
                nullable: false,
                default: Some(Value::String("AAPL".to_string())),
            },
            ColumnSpec {
                name: "price".to_string(),
                col_type: ColumnType::F64,
                nullable: true,
                default: Some(Value::Null),
            },
        ])
        .expect("schema");
        let payload = encode_schema_payload("ticks", &schema).expect("encode");
        let (_, decoded) = decode_schema_payload(&payload).expect("decode");
        assert_eq!(
            decoded.columns()[0].default,
            Some(Value::String("AAPL".to_string()))
        );
        assert_eq!(decoded.columns()[1].default, Some(Value::Null));
    }

    #[test]
    fn insert_update_delete_roundtrip() {
        let payload = encode_insert_payload(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[vec![Value::String("AAPL".to_string()), Value::F64(10.0)]],
        )
        .expect("encode insert");
        let (table, cols, rows) = decode_insert_payload(&payload).expect("decode insert");
        assert_eq!(table, "ticks");
        assert_eq!(cols, vec!["symbol", "price"]);
        assert_eq!(rows.len(), 1);

        let filter = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column("symbol".to_string())),
            right: Box::new(Expr::Literal(Value::String("AAPL".to_string()))),
        };
        let update = encode_update_payload(
            "ticks",
            &[("price".to_string(), Value::F64(12.5))],
            Some(&filter),
        )
        .expect("encode update");
        let (table, assigns, decoded_filter) =
            decode_update_payload(&update).expect("decode update");
        assert_eq!(table, "ticks");
        assert_eq!(assigns.len(), 1);
        assert!(decoded_filter.is_some());

        let delete = encode_delete_payload("ticks", Some(&filter)).expect("encode delete");
        let (table, decoded_filter) = decode_delete_payload(&delete).expect("decode delete");
        assert_eq!(table, "ticks");
        assert!(decoded_filter.is_some());
    }

    #[test]
    fn query_col_payload_roundtrip() {
        let query = ColumnQuery {
            table: "ticks".to_string(),
            columns: vec!["symbol".to_string(), "price".to_string()],
            filter: vec![
                ColumnInstr::PushCol(0),
                ColumnInstr::PushLit(Value::String("AAPL".to_string())),
                ColumnInstr::Cmp(BinaryOp::Eq),
            ],
            project: vec![
                ColumnProjectItem {
                    name: "symbol".to_string(),
                    expr: ColumnProjectExpr::Column(0),
                },
                ColumnProjectItem {
                    name: "price".to_string(),
                    expr: ColumnProjectExpr::Column(1),
                },
            ],
        };
        let payload = encode_query_col_payload(&query).expect("encode");
        let decoded = decode_query_col_payload(&payload).expect("decode");
        assert_eq!(decoded.table, "ticks");
        assert_eq!(decoded.columns.len(), 2);
        assert_eq!(decoded.project.len(), 2);
        assert_eq!(decoded.filter.len(), 3);
    }

    #[test]
    fn ddl_payload_roundtrip() {
        let col = ColumnSpec {
            name: "qty".to_string(),
            col_type: ColumnType::I64,
            nullable: false,
            default: Some(Value::I64(1)),
        };
        let add = encode_alter_add_column_payload("ticks", &col).expect("encode add");
        let (table, decoded) = decode_alter_add_column_payload(&add).expect("decode add");
        assert_eq!(table, "ticks");
        assert_eq!(decoded.name, "qty");
        assert_eq!(decoded.default, Some(Value::I64(1)));

        let drop = encode_alter_drop_column_payload("ticks", "old")
            .expect("encode drop");
        let (table, col) = decode_alter_drop_column_payload(&drop).expect("decode drop");
        assert_eq!(table, "ticks");
        assert_eq!(col, "old");

        let rename = encode_alter_rename_column_payload("ticks", "a", "b")
            .expect("encode rename");
        let (table, from, to) =
            decode_alter_rename_column_payload(&rename).expect("decode rename");
        assert_eq!(table, "ticks");
        assert_eq!(from, "a");
        assert_eq!(to, "b");

        let set_default = encode_alter_set_default_payload(
            "ticks",
            "qty",
            Some(&Value::I64(9)),
        )
        .expect("encode set default");
        let (table, col, default) =
            decode_alter_set_default_payload(&set_default).expect("decode set default");
        assert_eq!(table, "ticks");
        assert_eq!(col, "qty");
        assert_eq!(default, Some(Value::I64(9)));
    }

}

fn read_byte(cursor: &mut Cursor<'_>) -> Result<u8> {
    let bytes = cursor.take(1)?;
    Ok(bytes[0])
}

fn write_expr_option(out: &mut Vec<u8>, expr: Option<&Expr>) -> Result<()> {
    match expr {
        Some(expr) => {
            out.push(1);
            write_expr(out, expr)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn read_expr_option(cursor: &mut Cursor<'_>) -> Result<Option<Expr>> {
    let tag = read_byte(cursor)?;
    if tag == 0 {
        return Ok(None);
    }
    Ok(Some(read_expr(cursor)?))
}

fn write_expr(out: &mut Vec<u8>, expr: &Expr) -> Result<()> {
    match expr {
        Expr::Column(name) => {
            out.push(1);
            write_string(out, name)
        }
        Expr::Literal(value) => {
            out.push(2);
            write_value(out, value)
        }
        Expr::Binary { op, left, right } => {
            out.push(3);
            out.push(*op as u8);
            write_expr(out, left)?;
            write_expr(out, right)
        }
        Expr::And(left, right) => {
            out.push(4);
            write_expr(out, left)?;
            write_expr(out, right)
        }
        Expr::Or(left, right) => {
            out.push(5);
            write_expr(out, left)?;
            write_expr(out, right)
        }
        Expr::Not(inner) => {
            out.push(6);
            write_expr(out, inner)
        }
    }
}

fn read_expr(cursor: &mut Cursor<'_>) -> Result<Expr> {
    let tag = read_byte(cursor)?;
    match tag {
        1 => Ok(Expr::Column(read_string(cursor)?)),
        2 => Ok(Expr::Literal(read_value(cursor)?)),
        3 => {
            let op = BinaryOp::from_u8(read_byte(cursor)?)?;
            let left = Box::new(read_expr(cursor)?);
            let right = Box::new(read_expr(cursor)?);
            Ok(Expr::Binary { op, left, right })
        }
        4 => Ok(Expr::And(
            Box::new(read_expr(cursor)?),
            Box::new(read_expr(cursor)?),
        )),
        5 => Ok(Expr::Or(
            Box::new(read_expr(cursor)?),
            Box::new(read_expr(cursor)?),
        )),
        6 => Ok(Expr::Not(Box::new(read_expr(cursor)?))),
        _ => Err(Error::Protocol("unknown expr tag")),
    }
}
