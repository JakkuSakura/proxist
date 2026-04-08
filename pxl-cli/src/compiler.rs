use fp_core::query::SqlDialect;
use fp_core::sql_ast::{
    BinaryOperator, ColumnDef, DataType, Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr,
    JoinConstraint, JoinOperator, ObjectName, OrderByExpr, SetExpr, Statement, TableFactor,
    TableWithJoins, UnaryOperator, Value as SqlValue, WindowFrameBound, WindowFrameUnits,
    WindowType,
};
use fp_prql::compile_prql;
use fp_sql::sql_ast::parse_sql_ast;
use pxd::expr::{BinaryOp as PxlBinaryOp, Expr as PxlExpr};
use pxd::pxl::{
    encode_delete_payload, encode_frame, encode_insert_payload, encode_query_payload,
    encode_schema_payload, encode_update_payload, Frame, Op,
};
use pxd::query::{
    AggFunc, AggregateExpr, JoinSpec, JoinType, QueryPlan, SelectExpr, SelectItem, WindowBound,
    WindowExpr, WindowFrameUnit, WindowSpec,
};
use pxd::types::{ColumnSpec, ColumnType, Schema, Value as PxlValue};

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
        Statement::CreateTable { name, columns, query, .. } => {
            if query.is_some() {
                return Err("CREATE TABLE AS SELECT is not supported".to_string());
            }
            compile_create(name, columns, req_id)
        }
        Statement::Insert {
            table_name,
            columns,
            source,
        } => compile_insert(table_name, columns, source, req_id),
        Statement::Update {
            table,
            assignments,
            selection,
        } => compile_update(table, assignments, selection, req_id),
        Statement::Delete { from, selection } => compile_delete(from, selection, req_id),
        Statement::Query(query) => compile_select(*query, req_id),
        _ => Err("only CREATE TABLE/INSERT/UPDATE/DELETE/SELECT are supported".to_string()),
    }
}

fn compile_create(
    name: ObjectName,
    columns: Vec<ColumnDef>,
    req_id: u32,
) -> Result<Frame, String> {
    if columns.is_empty() {
        return Err("CREATE TABLE requires column definitions".to_string());
    }
    let table = name.to_string();
    let mut specs = Vec::with_capacity(columns.len());
    for col in columns {
        let col_type = map_data_type(&col.data_type)?;
        specs.push(ColumnSpec {
            name: col.name.value.clone(),
            col_type,
            nullable: true,
        });
    }
    let schema = Schema::new(specs).map_err(|err| format!("invalid schema: {err}"))?;
    let payload = encode_schema_payload(&table, &schema)
        .map_err(|err| format!("encode create payload failed: {err}"))?;
    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::Create,
        payload,
    })
}

fn compile_insert(
    table_name: ObjectName,
    columns: Vec<fp_core::sql_ast::Ident>,
    source: Option<Box<fp_core::sql_ast::Query>>,
    req_id: u32,
) -> Result<Frame, String> {
    if columns.is_empty() {
        return Err("INSERT must specify column list".to_string());
    }
    let column_names: Vec<String> = columns.iter().map(|col| col.value.clone()).collect();
    let source = source.ok_or_else(|| "INSERT must provide VALUES".to_string())?;
    let SetExpr::Values(values) = *source.body else {
        return Err("INSERT only supports VALUES source".to_string());
    };
    if values.rows.is_empty() {
        return Err("INSERT must include at least one row".to_string());
    }

    let mut rows = Vec::with_capacity(values.rows.len());
    for row in values.rows {
        if row.len() != column_names.len() {
            return Err("INSERT row column count mismatch".to_string());
        }
        let mut values = Vec::with_capacity(row.len());
        for expr in row {
            values.push(expr_to_literal_value(&expr)?);
        }
        rows.push(values);
    }

    let table = table_name.to_string();
    let payload = encode_insert_payload(&table, &column_names, &rows)
        .map_err(|err| format!("encode insert payload failed: {err}"))?;
    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::Insert,
        payload,
    })
}

fn compile_update(
    table: ObjectName,
    assignments: Vec<fp_core::sql_ast::Assignment>,
    selection: Option<Expr>,
    req_id: u32,
) -> Result<Frame, String> {
    if assignments.is_empty() {
        return Err("UPDATE requires assignments".to_string());
    }
    let mut out = Vec::with_capacity(assignments.len());
    for assign in assignments {
        let name = assign
            .id
            .last()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| "UPDATE assignment missing column".to_string())?;
        let value = expr_to_literal_value(&assign.value)?;
        out.push((name, value));
    }
    let filter = match selection.as_ref() {
        Some(expr) => Some(expr_to_predicate(expr)?),
        None => None,
    };

    let payload = encode_update_payload(&table.to_string(), &out, filter.as_ref())
        .map_err(|err| format!("encode update payload failed: {err}"))?;
    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::Update,
        payload,
    })
}

fn compile_select(query: fp_core::sql_ast::Query, req_id: u32) -> Result<Frame, String> {
    validate_query_shape(&query)?;
    let plan = build_query_plan(query)?;
    let payload = encode_query_payload(&plan)
        .map_err(|err| format!("encode query payload failed: {err}"))?;
    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::Query,
        payload,
    })
}

fn compile_delete(
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    req_id: u32,
) -> Result<Frame, String> {
    let (table, join) = extract_table_and_join(&from)?;
    if join.is_some() {
        return Err("DELETE does not support JOIN".to_string());
    }
    let filter = match selection.as_ref() {
        Some(expr) => Some(expr_to_predicate(expr)?),
        None => None,
    };
    let payload = encode_delete_payload(&table, filter.as_ref())
        .map_err(|err| format!("encode delete payload failed: {err}"))?;
    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::Delete,
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

fn build_query_plan(query: fp_core::sql_ast::Query) -> Result<QueryPlan, String> {
    let SetExpr::Select(select) = *query.body else {
        return Err("SELECT only supports simple SELECT query".to_string());
    };
    let select = *select;
    validate_select_shape(&select)?;

    let (table, join) = extract_table_and_join(&select.from)?;
    let filter = match select.selection.as_ref() {
        Some(expr) => Some(expr_to_predicate(expr)?),
        None => None,
    };
    let group_by = extract_group_by(&select.group_by)?;
    let projection = extract_projection(&select.projection)?;

    Ok(QueryPlan {
        table,
        join,
        filter,
        group_by,
        select: projection,
    })
}

fn validate_select_shape(select: &fp_core::sql_ast::Select) -> Result<(), String> {
    if select.having.is_some() {
        return Err("HAVING is not supported".to_string());
    }
    if select.distinct.is_some() {
        return Err("DISTINCT is not supported".to_string());
    }
    Ok(())
}

fn extract_table_and_join(from: &[TableWithJoins]) -> Result<(String, Option<JoinSpec>), String> {
    if from.len() != 1 {
        return Err("query must reference exactly one FROM item".to_string());
    }
    let base = &from[0];
    let (left_table, left_alias) = extract_table_name(&base.relation)?;
    if base.joins.is_empty() {
        return Ok((left_table, None));
    }
    if base.joins.len() != 1 {
        return Err("only one JOIN is supported".to_string());
    }
    let join = &base.joins[0];
    let (right_table, right_alias) = extract_table_name(&join.relation)?;
    let join_type = match join.join_operator {
        JoinOperator::Inner(_) => JoinType::Inner,
        JoinOperator::LeftOuter(_) => JoinType::LeftOuter,
    };
    let constraint = match &join.join_operator {
        JoinOperator::Inner(constraint) | JoinOperator::LeftOuter(constraint) => constraint,
    };
    let on_expr = match constraint {
        JoinConstraint::On(expr) => expr,
        JoinConstraint::None => return Err("JOIN requires ON condition".to_string()),
    };
    let (left_on, right_on) = parse_join_on(
        on_expr,
        &left_table,
        left_alias.as_deref(),
        &right_table,
        right_alias.as_deref(),
    )?;

    Ok((
        left_table,
        Some(JoinSpec {
            join_type,
            right_table,
            left_on,
            right_on,
            left_ts: None,
            right_ts: None,
        }),
    ))
}

fn extract_table_name(relation: &TableFactor) -> Result<(String, Option<String>), String> {
    match relation {
        TableFactor::Table { name, alias } => Ok((name.to_string(), alias.as_ref().map(|a| a.name.value.clone()))),
        _ => Err("only direct table references are supported".to_string()),
    }
}

fn parse_join_on(
    expr: &Expr,
    left_table: &str,
    left_alias: Option<&str>,
    right_table: &str,
    right_alias: Option<&str>,
) -> Result<(String, String), String> {
    let mut expr = expr;
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    let Expr::BinaryOp { left, op, right } = expr else {
        return Err("JOIN ON must be a simple equality".to_string());
    };
    if *op != BinaryOperator::Eq {
        return Err("JOIN ON must use =".to_string());
    }
    let (l_qual, l_name) = extract_qualified_name(left)?;
    let (r_qual, r_name) = extract_qualified_name(right)?;

    let is_left = |qual: &Option<String>| {
        qual.as_deref() == Some(left_table) || qual.as_deref() == left_alias
    };
    let is_right = |qual: &Option<String>| {
        qual.as_deref() == Some(right_table) || qual.as_deref() == right_alias
    };

    match (is_left(&l_qual), is_right(&l_qual), is_left(&r_qual), is_right(&r_qual)) {
        (true, false, false, true) => Ok((l_name, r_name)),
        (false, true, true, false) => Ok((r_name, l_name)),
        (false, false, _, _) | (_, _, false, false) => {
            Err("JOIN ON must qualify columns with table name or alias".to_string())
        }
        _ => Err("JOIN ON columns are ambiguous".to_string()),
    }
}

fn extract_group_by(group_by: &GroupByExpr) -> Result<Vec<String>, String> {
    let GroupByExpr::Expressions(exprs) = group_by;
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        out.push(expr_to_column_name(expr)?);
    }
    Ok(out)
}

fn extract_projection(items: &[fp_core::sql_ast::SelectItem]) -> Result<Vec<SelectItem>, String> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            fp_core::sql_ast::SelectItem::Wildcard => out.push(SelectItem {
                expr: SelectExpr::Wildcard,
                alias: None,
            }),
            fp_core::sql_ast::SelectItem::UnnamedExpr(expr) => {
                let expr = expr_to_select_expr(expr)?;
                out.push(SelectItem { expr, alias: None });
            }
            fp_core::sql_ast::SelectItem::ExprWithAlias { expr, alias } => {
                let expr = expr_to_select_expr(expr)?;
                out.push(SelectItem {
                    expr,
                    alias: Some(alias.value.clone()),
                });
            }
        }
    }
    Ok(out)
}

fn expr_to_select_expr(expr: &Expr) -> Result<SelectExpr, String> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Ok(SelectExpr::Column(expr_to_column_name(expr)?))
        }
        Expr::Value(_) => Ok(SelectExpr::Literal(expr_to_literal_value(expr)?)),
        Expr::Function(func) => function_to_select_expr(func),
        Expr::Nested(inner) => expr_to_select_expr(inner),
        _ => Err("unsupported expression in SELECT projection".to_string()),
    }
}

fn function_to_select_expr(func: &Function) -> Result<SelectExpr, String> {
    let func_name = func
        .name
        .parts
        .last()
        .map(|ident| ident.value.to_ascii_lowercase())
        .ok_or_else(|| "function name missing".to_string())?;
    let agg = map_agg_func(&func_name)?;
    let arg = function_arg_expr(&func.args)?;

    if let Some(over) = &func.over {
        let spec = build_window_spec(over)?;
        return Ok(SelectExpr::Window(WindowExpr { func: agg, arg, spec }));
    }

    Ok(SelectExpr::Aggregate(AggregateExpr { func: agg, arg }))
}

fn function_arg_expr(args: &[FunctionArg]) -> Result<PxlExpr, String> {
    if args.is_empty() {
        return Ok(PxlExpr::Literal(PxlValue::I64(1)));
    }
    if args.len() != 1 {
        return Err("function expects exactly one argument".to_string());
    }
    let FunctionArg::Unnamed(arg) = &args[0];
    let FunctionArgExpr::Expr(expr) = arg;
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Ok(PxlExpr::Column(expr_to_column_name(expr)?))
        }
        Expr::Value(_) | Expr::Nested(_) | Expr::UnaryOp { .. } => {
            Ok(PxlExpr::Literal(expr_to_literal_value(expr)?))
        }
        _ => Err("function argument must be a column or literal".to_string()),
    }
}

fn build_window_spec(window: &WindowType) -> Result<WindowSpec, String> {
    let WindowType::WindowSpec(spec) = window;
    let spec = spec.as_ref();
    let mut partition_by = Vec::with_capacity(spec.partition_by.len());
    for expr in &spec.partition_by {
        partition_by.push(expr_to_column_name(expr)?);
    }
    let order_by = parse_order_by(&spec.order_by)?;
    let (start, start_value) = parse_window_frame(spec.window_frame.as_deref())?;
    Ok(WindowSpec {
        partition_by,
        order_by,
        unit: WindowFrameUnit::Rows,
        start,
        start_value,
    })
}

fn parse_order_by(order_by: &[OrderByExpr]) -> Result<String, String> {
    if order_by.len() != 1 {
        return Err("window ORDER BY must specify exactly one column".to_string());
    }
    expr_to_column_name(&order_by[0].expr)
}

fn parse_window_frame(
    frame: Option<&fp_core::sql_ast::WindowFrame>,
) -> Result<(WindowBound, Option<u64>), String> {
    let Some(frame) = frame else {
        return Ok((WindowBound::UnboundedPreceding, None));
    };
    if frame.units != WindowFrameUnits::Rows {
        return Err("window frame only supports ROWS".to_string());
    }
    if let Some(end) = &frame.end_bound {
        if !matches!(end, WindowFrameBound::CurrentRow) {
            return Err("window frame end bound is not supported".to_string());
        }
    }
    match &frame.start_bound {
        WindowFrameBound::CurrentRow => Ok((WindowBound::CurrentRow, None)),
        WindowFrameBound::Preceding(expr_opt) => match expr_opt {
            None => Ok((WindowBound::UnboundedPreceding, None)),
            Some(expr) => Ok((WindowBound::Preceding, Some(expr_to_u64(expr)?))),
        },
        WindowFrameBound::Following(_) => Err("window frame FOLLOWING is not supported".to_string()),
    }
}

fn map_agg_func(name: &str) -> Result<AggFunc, String> {
    match name {
        "count" => Ok(AggFunc::Count),
        "sum" => Ok(AggFunc::Sum),
        "avg" => Ok(AggFunc::Avg),
        "min" => Ok(AggFunc::Min),
        "max" => Ok(AggFunc::Max),
        _ => Err(format!("unsupported function: {name}")),
    }
}

fn map_data_type(data_type: &DataType) -> Result<ColumnType, String> {
    match data_type {
        DataType::Boolean => Ok(ColumnType::Bool),
        DataType::Int64 | DataType::UnsignedBigInt => Ok(ColumnType::I64),
        DataType::Float64 | DataType::Decimal { .. } => Ok(ColumnType::F64),
        DataType::Date | DataType::DateTime | DataType::DateTime64(_) => Ok(ColumnType::Timestamp),
        DataType::String => Ok(ColumnType::String),
        DataType::Uuid | DataType::Ipv4 => Ok(ColumnType::String),
        _ => Err("unsupported column type".to_string()),
    }
}

fn expr_to_predicate(expr: &Expr) -> Result<PxlExpr, String> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(PxlExpr::And(
                Box::new(expr_to_predicate(left)?),
                Box::new(expr_to_predicate(right)?),
            )),
            BinaryOperator::Or => Ok(PxlExpr::Or(
                Box::new(expr_to_predicate(left)?),
                Box::new(expr_to_predicate(right)?),
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => Ok(PxlExpr::Binary {
                op: map_binary_op(op)?,
                left: Box::new(expr_to_predicate(left)?),
                right: Box::new(expr_to_predicate(right)?),
            }),
            _ => Err("unsupported binary operator in WHERE".to_string()),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(PxlExpr::Not(Box::new(expr_to_predicate(expr)?))),
            UnaryOperator::Minus | UnaryOperator::Plus => {
                Ok(PxlExpr::Literal(expr_to_literal_value(expr)?))
            }
        },
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Ok(PxlExpr::Column(expr_to_column_name(expr)?))
        }
        Expr::Value(_) => Ok(PxlExpr::Literal(expr_to_literal_value(expr)?)),
        Expr::Nested(inner) => expr_to_predicate(inner),
        _ => Err("unsupported expression in WHERE".to_string()),
    }
}

fn map_binary_op(op: &BinaryOperator) -> Result<PxlBinaryOp, String> {
    match op {
        BinaryOperator::Eq => Ok(PxlBinaryOp::Eq),
        BinaryOperator::NotEq => Ok(PxlBinaryOp::NotEq),
        BinaryOperator::Lt => Ok(PxlBinaryOp::Lt),
        BinaryOperator::LtEq => Ok(PxlBinaryOp::LtEq),
        BinaryOperator::Gt => Ok(PxlBinaryOp::Gt),
        BinaryOperator::GtEq => Ok(PxlBinaryOp::GtEq),
        _ => Err("unsupported comparison operator".to_string()),
    }
}

fn expr_to_column_name(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| "expected column name".to_string()),
        Expr::Nested(inner) => expr_to_column_name(inner),
        _ => Err("expected column name".to_string()),
    }
}

fn extract_qualified_name(expr: &Expr) -> Result<(Option<String>, String), String> {
    match expr {
        Expr::Identifier(ident) => Ok((None, ident.value.clone())),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() < 2 {
                return Err("expected qualified column".to_string());
            }
            let qual = idents.first().map(|ident| ident.value.clone());
            let name = idents
                .last()
                .map(|ident| ident.value.clone())
                .ok_or_else(|| "expected column name".to_string())?;
            Ok((qual, name))
        }
        Expr::Nested(inner) => extract_qualified_name(inner),
        _ => Err("expected column reference".to_string()),
    }
}

fn expr_to_literal_value(expr: &Expr) -> Result<PxlValue, String> {
    match expr {
        Expr::Value(value) => sql_value_to_pxl(value),
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Minus => negate_literal(expr_to_literal_value(expr)?) ,
            UnaryOperator::Plus => expr_to_literal_value(expr),
            UnaryOperator::Not => {
                let value = expr_to_literal_value(expr)?;
                match value {
                    PxlValue::Bool(v) => Ok(PxlValue::Bool(!v)),
                    _ => Err("NOT only supported for boolean literals".to_string()),
                }
            }
        },
        Expr::Nested(inner) => expr_to_literal_value(inner),
        _ => Err("expected literal value".to_string()),
    }
}

fn negate_literal(value: PxlValue) -> Result<PxlValue, String> {
    match value {
        PxlValue::I64(v) => Ok(PxlValue::I64(-v)),
        PxlValue::F64(v) => Ok(PxlValue::F64(-v)),
        _ => Err("unary minus only supported for numeric literals".to_string()),
    }
}

fn sql_value_to_pxl(value: &SqlValue) -> Result<PxlValue, String> {
    match value {
        SqlValue::Number(v, _) => {
            if v.contains('.') || v.contains('e') || v.contains('E') {
                let parsed = v
                    .parse::<f64>()
                    .map_err(|_| "invalid float literal".to_string())?;
                Ok(PxlValue::F64(parsed))
            } else {
                let parsed = v
                    .parse::<i64>()
                    .map_err(|_| "invalid integer literal".to_string())?;
                Ok(PxlValue::I64(parsed))
            }
        }
        SqlValue::SingleQuotedString(v)
        | SqlValue::DoubleQuotedString(v)
        | SqlValue::UnquotedString(v) => Ok(PxlValue::String(v.clone())),
        SqlValue::Boolean(v) => Ok(PxlValue::Bool(*v)),
        SqlValue::Null => Ok(PxlValue::Null),
    }
}

fn expr_to_u64(expr: &Expr) -> Result<u64, String> {
    match expr {
        Expr::Value(SqlValue::Number(value, _)) => value
            .parse::<u64>()
            .map_err(|_| "window frame value must be a positive integer".to_string()),
        Expr::Nested(inner) => expr_to_u64(inner),
        _ => Err("window frame value must be a numeric literal".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pxd::pxl::{
        decode_delete_payload, decode_insert_payload, decode_query_payload,
        decode_schema_payload, decode_update_payload, read_frame,
    };
    use std::io::Cursor;

    fn decode_frame(bytes: Vec<u8>) -> Frame {
        let mut cursor = Cursor::new(bytes);
        read_frame(&mut cursor)
            .expect("read frame")
            .expect("frame present")
    }

    #[test]
    fn compile_create_table_to_create() {
        let sql = "CREATE TABLE ticks (symbol String, ts Int64, value Float64)";
        let bytes = compile_to_frame(sql, InputKind::Sql, 10).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Create);
        let (table, schema) = decode_schema_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(schema.columns().len(), 3);
    }

    #[test]
    fn compile_insert_sql_to_insert() {
        let sql = "INSERT INTO ticks (symbol, ts, value) VALUES ('AAPL', 1, 1.5)";
        let bytes = compile_to_frame(sql, InputKind::Sql, 42).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Insert);
        let (table, cols, rows) = decode_insert_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(cols, vec!["symbol", "ts", "value"]);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn compile_select_sql_to_query() {
        let sql = "SELECT value FROM ticks WHERE symbol = 'AAPL'";
        let bytes = compile_to_frame(sql, InputKind::Sql, 7).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Query);
        let plan = decode_query_payload(&frame.payload).expect("payload");
        assert_eq!(plan.table, "ticks");
        assert!(plan.filter.is_some());
    }

    #[test]
    fn compile_update_sql_to_update() {
        let sql = "UPDATE ticks SET value = 2.0 WHERE symbol = 'AAPL'";
        let bytes = compile_to_frame(sql, InputKind::Sql, 5).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Update);
        let (table, assigns, filter) = decode_update_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(assigns.len(), 1);
        assert!(filter.is_some());
    }

    #[test]
    fn compile_delete_sql_to_delete() {
        let sql = "DELETE FROM ticks WHERE symbol = 'AAPL'";
        let bytes = compile_to_frame(sql, InputKind::Sql, 3).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Delete);
        let (table, filter) = decode_delete_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert!(filter.is_some());
    }

    #[test]
    fn compile_prql_to_query() {
        let prql = r#"
from ticks
| filter symbol == "AAPL"
| select {value}
"#;
        let bytes = compile_to_frame(prql, InputKind::Prql, 9).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Query);
        let plan = decode_query_payload(&frame.payload).expect("payload");
        assert_eq!(plan.table, "ticks");
    }
}
