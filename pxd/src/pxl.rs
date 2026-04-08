use std::io::{Read, Write};

use crate::error::{Error, Result};
use crate::expr::{BinaryOp, Expr};
use crate::query::{
    AggFunc, JoinSpec, JoinType, QueryPlan, SelectExpr, SelectItem, WindowBound,
    WindowExpr, WindowFrameUnit, WindowSpec,
};
use crate::types::{ColumnSpec, ColumnType, Row, Schema, Value};

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
    Query = 14,
    Result = 15,
    Schema = 16,
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
            14 => Ok(Op::Query),
            15 => Ok(Op::Result),
            16 => Ok(Op::Schema),
            _ => Err(Error::Protocol("unknown op")),
        }
    }
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

pub fn decode_error_payload(bytes: &[u8]) -> Result<String> {
    let mut cursor = Cursor::new(bytes);
    read_string(&mut cursor)
}

pub fn encode_schema_payload(table: &str, schema: &Schema) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, table)?;
    write_schema(&mut out, schema)?;
    Ok(out)
}

pub fn decode_schema_payload(bytes: &[u8]) -> Result<(String, Schema)> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let schema = read_schema(&mut cursor)?;
    Ok((table, schema))
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

pub fn encode_query_payload(plan: &QueryPlan) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_string(&mut out, &plan.table)?;
    write_join_option(&mut out, plan.join.as_ref())?;
    write_expr_option(&mut out, plan.filter.as_ref())?;
    write_string_list(&mut out, &plan.group_by)?;
    write_u32(&mut out, plan.select.len() as u32)?;
    for item in &plan.select {
        write_select_item(&mut out, item)?;
    }
    Ok(out)
}

pub fn decode_query_payload(bytes: &[u8]) -> Result<QueryPlan> {
    let mut cursor = Cursor::new(bytes);
    let table = read_string(&mut cursor)?;
    let join = read_join_option(&mut cursor)?;
    let filter = read_expr_option(&mut cursor)?;
    let group_by = read_string_list(&mut cursor)?;
    let select_count = read_u32(&mut cursor)? as usize;
    let mut select = Vec::with_capacity(select_count);
    for _ in 0..select_count {
        select.push(read_select_item(&mut cursor)?);
    }
    Ok(QueryPlan {
        table,
        join,
        filter,
        group_by,
        select,
    })
}

pub fn encode_result_payload(schema: &Schema, rows: &[Row]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_schema(&mut out, schema)?;
    write_u32(&mut out, rows.len() as u32)?;
    for row in rows {
        write_values(&mut out, &row.values)?;
    }
    Ok(out)
}

pub fn decode_result_payload(bytes: &[u8]) -> Result<(Schema, Vec<Row>)> {
    let mut cursor = Cursor::new(bytes);
    let schema = read_schema(&mut cursor)?;
    let row_count = read_u32(&mut cursor)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let values = read_values(&mut cursor, schema.columns().len())?;
        rows.push(Row { values });
    }
    Ok((schema, rows))
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

fn write_u64(out: &mut Vec<u8>, value: u64) -> Result<()> {
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_u64(cursor: &mut Cursor<'_>) -> Result<u64> {
    let bytes = cursor.take(8)?;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
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
        });
    }
    Schema::new(columns)
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
        decode_delete_payload, decode_insert_payload, decode_query_payload, decode_schema_payload,
        decode_update_payload, encode_delete_payload, encode_insert_payload, encode_query_payload,
        encode_schema_payload, encode_update_payload, read_frame, write_frame, Frame, Op,
    };
    use crate::expr::{BinaryOp, Expr};
    use crate::query::{
        AggFunc, AggregateExpr, JoinSpec, JoinType, QueryPlan, SelectExpr, SelectItem, WindowBound,
        WindowExpr, WindowFrameUnit, WindowSpec,
    };
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
            },
            ColumnSpec {
                name: "ts".to_string(),
                col_type: ColumnType::I64,
                nullable: false,
            },
        ])
        .expect("schema");
        let payload = encode_schema_payload("ticks", &schema).expect("encode");
        let (table, decoded) = decode_schema_payload(&payload).expect("decode");
        assert_eq!(table, "ticks");
        assert_eq!(decoded.columns().len(), 2);
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
    fn query_payload_roundtrip() {
        let plan = QueryPlan {
            table: "ticks".to_string(),
            join: Some(JoinSpec {
                join_type: JoinType::Inner,
                right_table: "quotes".to_string(),
                left_on: "symbol".to_string(),
                right_on: "symbol".to_string(),
                left_ts: None,
                right_ts: None,
            }),
            filter: Some(Expr::Binary {
                op: BinaryOp::GtEq,
                left: Box::new(Expr::Column("ts".to_string())),
                right: Box::new(Expr::Literal(Value::I64(10))),
            }),
            group_by: vec!["symbol".to_string()],
            select: vec![
                SelectItem {
                    expr: SelectExpr::Column("symbol".to_string()),
                    alias: None,
                },
                SelectItem {
                    expr: SelectExpr::Aggregate(AggregateExpr {
                        func: AggFunc::Avg,
                        arg: Expr::Column("price".to_string()),
                    }),
                    alias: Some("avg_price".to_string()),
                },
                SelectItem {
                    expr: SelectExpr::Window(WindowExpr {
                        func: AggFunc::Count,
                        arg: Expr::Column("price".to_string()),
                        spec: WindowSpec {
                            partition_by: vec!["symbol".to_string()],
                            order_by: "ts".to_string(),
                            unit: WindowFrameUnit::Rows,
                            start: WindowBound::Preceding,
                            start_value: Some(2),
                        },
                    }),
                    alias: Some("win_cnt".to_string()),
                },
            ],
        };

        let payload = encode_query_payload(&plan).expect("encode");
        let decoded = decode_query_payload(&payload).expect("decode");
        assert_eq!(decoded.table, "ticks");
        assert_eq!(decoded.join.as_ref().unwrap().right_table, "quotes");
        assert_eq!(decoded.group_by, vec!["symbol"]);
        assert_eq!(decoded.select.len(), 3);
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

fn write_select_item(out: &mut Vec<u8>, item: &SelectItem) -> Result<()> {
    write_select_expr(out, &item.expr)?;
    match &item.alias {
        Some(alias) => {
            out.push(1);
            write_string(out, alias)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn read_select_item(cursor: &mut Cursor<'_>) -> Result<SelectItem> {
    let expr = read_select_expr(cursor)?;
    let alias = if read_byte(cursor)? != 0 {
        Some(read_string(cursor)?)
    } else {
        None
    };
    Ok(SelectItem { expr, alias })
}

fn write_select_expr(out: &mut Vec<u8>, expr: &SelectExpr) -> Result<()> {
    match expr {
        SelectExpr::Column(name) => {
            out.push(1);
            write_string(out, name)
        }
        SelectExpr::Literal(value) => {
            out.push(2);
            write_value(out, value)
        }
        SelectExpr::Aggregate(agg) => {
            out.push(3);
            out.push(agg.func as u8);
            write_expr(out, &agg.arg)
        }
        SelectExpr::Window(window) => {
            out.push(4);
            out.push(window.func as u8);
            write_expr(out, &window.arg)?;
            write_window_spec(out, &window.spec)
        }
        SelectExpr::Wildcard => {
            out.push(5);
            Ok(())
        }
    }
}

fn read_select_expr(cursor: &mut Cursor<'_>) -> Result<SelectExpr> {
    let tag = read_byte(cursor)?;
    match tag {
        1 => Ok(SelectExpr::Column(read_string(cursor)?)),
        2 => Ok(SelectExpr::Literal(read_value(cursor)?)),
        3 => {
            let func = AggFunc::from_u8(read_byte(cursor)?)?;
            let arg = read_expr(cursor)?;
            Ok(SelectExpr::Aggregate(crate::query::AggregateExpr { func, arg }))
        }
        4 => {
            let func = AggFunc::from_u8(read_byte(cursor)?)?;
            let arg = read_expr(cursor)?;
            let spec = read_window_spec(cursor)?;
            Ok(SelectExpr::Window(WindowExpr { func, arg, spec }))
        }
        5 => Ok(SelectExpr::Wildcard),
        _ => Err(Error::Protocol("unknown select expr")),
    }
}

fn write_window_spec(out: &mut Vec<u8>, spec: &WindowSpec) -> Result<()> {
    write_string_list(out, &spec.partition_by)?;
    write_string(out, &spec.order_by)?;
    out.push(spec.unit as u8);
    out.push(spec.start as u8);
    match spec.start {
        WindowBound::Preceding => {
            write_u64(out, spec.start_value.unwrap_or(0))?;
        }
        _ => {}
    }
    Ok(())
}

fn read_window_spec(cursor: &mut Cursor<'_>) -> Result<WindowSpec> {
    let partition_by = read_string_list(cursor)?;
    let order_by = read_string(cursor)?;
    let unit = WindowFrameUnit::Rows;
    let bound = match read_byte(cursor)? {
        1 => WindowBound::UnboundedPreceding,
        2 => WindowBound::CurrentRow,
        3 => WindowBound::Preceding,
        _ => return Err(Error::Protocol("unknown window bound")),
    };
    let start_value = if bound == WindowBound::Preceding {
        Some(read_u64(cursor)?)
    } else {
        None
    };
    Ok(WindowSpec {
        partition_by,
        order_by,
        unit,
        start: bound,
        start_value,
    })
}

fn write_join_option(out: &mut Vec<u8>, join: Option<&JoinSpec>) -> Result<()> {
    match join {
        Some(join) => {
            out.push(1);
            out.push(join.join_type as u8);
            write_string(out, &join.right_table)?;
            write_string(out, &join.left_on)?;
            write_string(out, &join.right_on)?;
            match &join.left_ts {
                Some(value) => {
                    out.push(1);
                    write_string(out, value)?;
                }
                None => out.push(0),
            }
            match &join.right_ts {
                Some(value) => {
                    out.push(1);
                    write_string(out, value)?;
                }
                None => out.push(0),
            }
            Ok(())
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn read_join_option(cursor: &mut Cursor<'_>) -> Result<Option<JoinSpec>> {
    let tag = read_byte(cursor)?;
    if tag == 0 {
        return Ok(None);
    }
    let join_type = JoinType::from_u8(read_byte(cursor)?)?;
    let right_table = read_string(cursor)?;
    let left_on = read_string(cursor)?;
    let right_on = read_string(cursor)?;
    let left_ts = if read_byte(cursor)? != 0 {
        Some(read_string(cursor)?)
    } else {
        None
    };
    let right_ts = if read_byte(cursor)? != 0 {
        Some(read_string(cursor)?)
    } else {
        None
    };
    Ok(Some(JoinSpec {
        join_type,
        right_table,
        left_on,
        right_on,
        left_ts,
        right_ts,
    }))
}
