use pxd::expr::{BinaryOp, Expr};
use pxd::types::{ColumnType, Value};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unsupported value: {0}")]
    Unsupported(String),
    #[error("invalid data: {0}")]
    InvalidData(String),
}

pub fn quote_ident(name: &str) -> String {
    let escaped = name.replace('`', "``");
    format!("`{escaped}`")
}

pub fn qualify_table(database: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(database), quote_ident(table))
}

pub fn column_type_sql(col_type: ColumnType, nullable: bool) -> String {
    let base = match col_type {
        ColumnType::I64 => "Int64",
        ColumnType::F64 => "Float64",
        ColumnType::Bool => "UInt8",
        ColumnType::String => "String",
        ColumnType::Bytes => "String",
        ColumnType::Timestamp => "Int64",
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base.to_string()
    }
}

pub fn value_to_sql(value: &Value) -> Result<String, Error> {
    match value {
        Value::Null => Ok("NULL".to_string()),
        Value::I64(v) => Ok(v.to_string()),
        Value::F64(v) => {
            if v.is_finite() {
                Ok(v.to_string())
            } else {
                Err(Error::Unsupported("non-finite f64".to_string()))
            }
        }
        Value::Bool(v) => Ok(if *v { "1" } else { "0" }.to_string()),
        Value::String(v) => Ok(format!("'{}'", escape_string(v))),
        Value::Bytes(v) => Ok(format!("unhex('{}')", encode_hex(v))),
        Value::Timestamp(v) => Ok(v.to_string()),
    }
}

pub fn expr_to_sql(expr: &Expr) -> Result<String, Error> {
    match expr {
        Expr::Column(name) => Ok(quote_ident(name)),
        Expr::Literal(value) => value_to_sql(value),
        Expr::Binary { op, left, right } => {
            let left = expr_to_sql(left)?;
            let right = expr_to_sql(right)?;
            Ok(format!(
                "({left} {} {right})",
                binary_op_sql(*op)
            ))
        }
        Expr::And(left, right) => Ok(format!(
            "({} AND {})",
            expr_to_sql(left)?,
            expr_to_sql(right)?
        )),
        Expr::Or(left, right) => Ok(format!(
            "({} OR {})",
            expr_to_sql(left)?,
            expr_to_sql(right)?
        )),
        Expr::Not(inner) => Ok(format!("(NOT {})", expr_to_sql(inner)?)),
    }
}

pub fn insert_values_sql(
    database: &str,
    table: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<String, Error> {
    if rows.is_empty() {
        return Err(Error::InvalidData("empty insert batch".to_string()));
    }
    let mut column_parts = Vec::with_capacity(columns.len());
    for col in columns {
        column_parts.push(quote_ident(col));
    }

    let mut values_parts = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != columns.len() {
            return Err(Error::InvalidData(
                "row length does not match columns".to_string(),
            ));
        }
        let mut value_sql = Vec::with_capacity(row.len());
        for value in row {
            value_sql.push(value_to_sql(value)?);
        }
        values_parts.push(format!("({})", value_sql.join(", ")));
    }

    Ok(format!(
        "INSERT INTO {} ({}) VALUES {}",
        qualify_table(database, table),
        column_parts.join(", "),
        values_parts.join(", ")
    ))
}

fn binary_op_sql(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
    }
}

fn escape_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 8);
    for ch in value.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}
