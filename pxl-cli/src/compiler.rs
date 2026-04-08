use fp_core::query::SqlDialect;
use fp_core::sql_ast::{
    BinaryOperator, Expr, ObjectName, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use fp_prql::compile_prql;
use fp_sql::sql_ast::parse_sql_ast;
use proxistd::pxl::{
    encode_delete_payload, encode_frame, encode_get_payload, encode_put_payload, Frame, Op,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Sql,
    Prql,
}

pub fn compile_to_frame(input: &str, kind: InputKind, req_id: u32) -> Result<Vec<u8>, String> {
    let statement = match kind {
        InputKind::Sql => parse_single_sql(input)?,
        InputKind::Prql => {
            let compiled = compile_prql(input, Some(SqlDialect::Generic))
                .map_err(|err| format!("PRQL compile failed: {err}"))?;
            if compiled.statements.is_empty() {
                return Err("PRQL compile produced no statements".to_string());
            }
            if compiled.statements.len() != 1 {
                return Err("PRQL compile produced multiple statements".to_string());
            }
            parse_single_sql(&compiled.statements[0])?
        }
    };

    let frame = statement_to_frame(statement, req_id)?;
    Ok(encode_frame(&frame))
}

fn parse_single_sql(input: &str) -> Result<Statement, String> {
    let statements = parse_sql_ast(input, SqlDialect::Generic)
        .map_err(|err| format!("SQL parse failed: {err}"))?;
    if statements.is_empty() {
        return Err("SQL parse produced no statements".to_string());
    }
    if statements.len() != 1 {
        return Err("SQL contains multiple statements".to_string());
    }
    Ok(statements.into_iter().next().expect("statement"))
}

fn statement_to_frame(statement: Statement, req_id: u32) -> Result<Frame, String> {
    match statement {
        Statement::Insert {
            table_name,
            columns,
            source,
        } => compile_insert(table_name, columns, source, req_id),
        Statement::Query(query) => compile_select(*query, req_id),
        Statement::Delete { from, selection } => compile_delete(from, selection, req_id),
        _ => Err("only INSERT/SELECT/DELETE are supported".to_string()),
    }
}

fn compile_insert(
    table_name: ObjectName,
    columns: Vec<fp_core::sql_ast::Ident>,
    source: Option<Box<fp_core::sql_ast::Query>>,
    req_id: u32,
) -> Result<Frame, String> {
    if columns.len() != 3 {
        return Err("INSERT must specify columns (symbol, ts, value)".to_string());
    }
    let column_names: Vec<String> = columns.iter().map(|col| col.value.clone()).collect();
    let expected = ["symbol", "ts", "value"];
    for (idx, name) in column_names.iter().enumerate() {
        if !name.eq_ignore_ascii_case(expected[idx]) {
            return Err("INSERT columns must be (symbol, ts, value)".to_string());
        }
    }

    let source = source.ok_or_else(|| "INSERT must provide VALUES".to_string())?;
    let SetExpr::Values(values) = *source.body else {
        return Err("INSERT only supports VALUES source".to_string());
    };
    if values.rows.len() != 1 {
        return Err("INSERT must include exactly one row".to_string());
    }
    let row = &values.rows[0];
    if row.len() != 3 {
        return Err("INSERT row must have 3 values".to_string());
    }

    let symbol = expr_to_string(&row[0])?;
    let ts = expr_to_u64(&row[1])?;
    let value = expr_to_bytes(&row[2])?;
    let table = table_name.to_string();

    let payload = encode_put_payload(&table, &symbol, ts, &value)
        .map_err(|err| format!("encode put payload failed: {err}"))?;

    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::Put,
        payload,
    })
}

fn compile_select(query: fp_core::sql_ast::Query, req_id: u32) -> Result<Frame, String> {
    validate_query_shape(&query)?;
    let SetExpr::Select(select) = *query.body else {
        return Err("SELECT only supports simple SELECT query".to_string());
    };

    validate_select_shape(&select)?;
    let table = extract_table(&select.from)?;
    let symbol = extract_symbol_filter(select.selection)?;

    let payload = encode_get_payload(&table, &symbol)
        .map_err(|err| format!("encode get payload failed: {err}"))?;

    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::Get,
        payload,
    })
}

fn validate_query_shape(query: &fp_core::sql_ast::Query) -> Result<(), String> {
    if !query.order_by.is_empty() {
        return Err("ORDER BY is not supported".to_string());
    }
    if query.limit.is_some() {
        return Err("LIMIT is not supported".to_string());
    }
    if query.offset.is_some() {
        return Err("OFFSET is not supported".to_string());
    }
    if query.format.is_some() {
        return Err("FORMAT is not supported".to_string());
    }
    if query.sample_ratio.is_some() {
        return Err("SAMPLE is not supported".to_string());
    }
    Ok(())
}

fn validate_select_shape(select: &fp_core::sql_ast::Select) -> Result<(), String> {
    let group_by_empty = match &select.group_by {
        fp_core::sql_ast::GroupByExpr::Expressions(exprs) => exprs.is_empty(),
    };
    if !group_by_empty {
        return Err("GROUP BY is not supported".to_string());
    }
    if select.having.is_some() {
        return Err("HAVING is not supported".to_string());
    }
    if select.distinct.is_some() {
        return Err("DISTINCT is not supported".to_string());
    }
    Ok(())
}

fn compile_delete(
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    req_id: u32,
) -> Result<Frame, String> {
    let table = extract_table(&from)?;
    let symbol = extract_symbol_filter(selection)?;

    let payload = encode_delete_payload(&table, &symbol)
        .map_err(|err| format!("encode delete payload failed: {err}"))?;

    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::Delete,
        payload,
    })
}

fn extract_table(from: &[TableWithJoins]) -> Result<String, String> {
    if from.len() != 1 {
        return Err("query must reference exactly one table".to_string());
    }
    let table = &from[0];
    if !table.joins.is_empty() {
        return Err("joins are not supported".to_string());
    }
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err("only direct table references are supported".to_string()),
    }
}

fn extract_symbol_filter(selection: Option<Expr>) -> Result<String, String> {
    let selection = selection.ok_or_else(|| "WHERE symbol = 'X' is required".to_string())?;
    let mut expr = &selection;
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    let Expr::BinaryOp { left, op, right } = expr else {
        return Err("WHERE must be a simple equality".to_string());
    };
    if *op != BinaryOperator::Eq {
        return Err("WHERE must use =".to_string());
    }
    if !expr_is_symbol(left) {
        return Err("WHERE must filter by symbol".to_string());
    }
    expr_to_string(right)
}

fn expr_is_symbol(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("symbol"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("symbol"))
            .unwrap_or(false),
        Expr::Nested(inner) => expr_is_symbol(inner),
        _ => false,
    }
}

fn expr_to_string(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(value)) => Ok(value.clone()),
        Expr::Value(Value::DoubleQuotedString(value)) => Ok(value.clone()),
        Expr::Value(Value::UnquotedString(value)) => Ok(value.clone()),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| "expected string literal".to_string()),
        Expr::Nested(inner) => expr_to_string(inner),
        _ => Err("expected string literal".to_string()),
    }
}

fn expr_to_u64(expr: &Expr) -> Result<u64, String> {
    match expr {
        Expr::Value(Value::Number(value, _)) => value
            .parse::<u64>()
            .map_err(|_| "ts must be a positive integer".to_string()),
        Expr::Nested(inner) => expr_to_u64(inner),
        _ => Err("ts must be a numeric literal".to_string()),
    }
}

fn expr_to_bytes(expr: &Expr) -> Result<Vec<u8>, String> {
    match expr {
        Expr::Value(Value::Number(value, _)) => Ok(value.as_bytes().to_vec()),
        Expr::Value(Value::SingleQuotedString(value)) => Ok(value.as_bytes().to_vec()),
        Expr::Value(Value::DoubleQuotedString(value)) => Ok(value.as_bytes().to_vec()),
        Expr::Value(Value::UnquotedString(value)) => Ok(value.as_bytes().to_vec()),
        Expr::Nested(inner) => expr_to_bytes(inner),
        _ => Err("value must be a string or number literal".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proxistd::pxl::{decode_delete_payload, decode_get_payload, decode_put_payload, read_frame};
    use std::io::Cursor;

    fn decode_frame(bytes: Vec<u8>) -> Frame {
        let mut cursor = Cursor::new(bytes);
        read_frame(&mut cursor)
            .expect("read frame")
            .expect("frame present")
    }

    #[test]
    fn compile_insert_sql_to_put() {
        let sql = "INSERT INTO ticks (symbol, ts, value) VALUES ('AAPL', 1, 'v')";
        let bytes = compile_to_frame(sql, InputKind::Sql, 42).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Put);
        let (table, symbol, ts, value) = decode_put_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(symbol, "AAPL");
        assert_eq!(ts, 1);
        assert_eq!(value, b"v");
    }

    #[test]
    fn compile_select_sql_to_get() {
        let sql = "SELECT value FROM ticks WHERE symbol = 'AAPL'";
        let bytes = compile_to_frame(sql, InputKind::Sql, 7).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Get);
        let (table, symbol) = decode_get_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(symbol, "AAPL");
    }

    #[test]
    fn compile_delete_sql_to_delete() {
        let sql = "DELETE FROM ticks WHERE symbol = 'AAPL'";
        let bytes = compile_to_frame(sql, InputKind::Sql, 3).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Delete);
        let (table, symbol) = decode_delete_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(symbol, "AAPL");
    }

    #[test]
    fn compile_prql_to_get() {
        let prql = r#"
from ticks
| filter symbol == "AAPL"
| select {value}
"#;
        let bytes = compile_to_frame(prql, InputKind::Prql, 9).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Get);
        let (table, symbol) = decode_get_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(symbol, "AAPL");
    }
}
