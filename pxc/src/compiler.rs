use fp_core::query::SqlDialect;
use fp_core::sql_ast::{
    AlterTableOperation, BinaryOperator, ColumnDef, ColumnOption, DataType, Expr, GroupByExpr,
    ObjectName, SetExpr, Statement, TableFactor, TableWithJoins, UnaryOperator,
    Value as SqlValue,
};
use fp_prql::compile_prql;
use fp_sql::sql_ast::parse_sql_ast;
use pxd::expr::{BinaryOp as PxlBinaryOp, Expr as PxlExpr};
use pxd::pxl::{
    encode_alter_add_column_payload, encode_alter_drop_column_payload,
    encode_alter_rename_column_payload, encode_alter_set_default_payload,
    encode_delete_payload, encode_drop_table_payload, encode_frame, encode_insert_payload,
    encode_query_col_payload,
    encode_rename_table_payload, encode_schema_payload, encode_update_payload, ColumnInstr,
    ColumnProjectExpr, ColumnProjectItem, ColumnQuery, Frame, Op,
};
use pxd::types::{ColumnSpec, ColumnType, Schema, Value as PxlValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Sql,
    Prql,
}

pub fn compile_to_frame(input: &str, kind: InputKind, req_id: u32) -> Result<Vec<u8>, String> {
    let frame = match kind {
        InputKind::Sql => match parse_single_sql(input) {
            Ok(statement) => match statement_to_frame(statement, req_id) {
                Ok(frame) => frame,
                Err(err) => match parse_manual_ddl(input)? {
                    Some(manual) => manual_to_frame(manual, req_id)?,
                    None => return Err(err),
                },
            },
            Err(err) => match parse_manual_ddl(input)? {
                Some(manual) => manual_to_frame(manual, req_id)?,
                None => return Err(err),
            },
        },
        InputKind::Prql => {
            let compiled = compile_prql(input, Some(SqlDialect::Generic))
                .map_err(|err| format!("PRQL compile failed: {err}"))?;
            if compiled.statements.is_empty() {
                return Err("PRQL compile produced no statements".to_string());
            }
            if compiled.statements.len() != 1 {
                return Err("PRQL compile produced multiple statements".to_string());
            }
            let statement = parse_single_sql(&compiled.statements[0])?;
            statement_to_frame(statement, req_id)?
        }
    };
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
            if let Some(query) = query {
                if !columns.is_empty() {
                    return Err("CTAS does not support column definitions".to_string());
                }
                compile_create_as(name, *query, req_id)
            } else {
                compile_create(name, columns, req_id)
            }
        }
        Statement::AlterTable { name, operations } => {
            compile_alter_table(name, operations, req_id)
        }
        Statement::Drop { object_type, names } => {
            compile_drop_table(object_type, names, req_id)
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
        let default = column_default_from_options(&col.options)?;
        specs.push(ColumnSpec {
            name: col.name.value.clone(),
            col_type,
            nullable: true,
            default,
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

fn compile_create_as(
    name: ObjectName,
    query: fp_core::sql_ast::Query,
    req_id: u32,
) -> Result<Frame, String> {
    let _ = (name, query, req_id);
    Err("CTAS is not supported in pxc (query plan removed; use CREATE TABLE + INSERT)".to_string())
}

fn compile_alter_table(
    name: ObjectName,
    operations: Vec<AlterTableOperation>,
    req_id: u32,
) -> Result<Frame, String> {
    if operations.len() != 1 {
        return Err("ALTER TABLE supports one operation at a time".to_string());
    }
    let op = operations.into_iter().next().expect("operation");
    match op {
        AlterTableOperation::AddColumn { column_def } => {
            compile_alter_add_column(name, column_def, req_id)
        }
    }
}

fn compile_alter_add_column(
    name: ObjectName,
    column: ColumnDef,
    req_id: u32,
) -> Result<Frame, String> {
    let col_type = map_data_type(&column.data_type)?;
    let default = column_default_from_options(&column.options)?;
    let spec = ColumnSpec {
        name: column.name.value,
        col_type,
        nullable: true,
        default,
    };
    let payload = encode_alter_add_column_payload(&name.to_string(), &spec)
        .map_err(|err| format!("encode alter add column payload failed: {err}"))?;
    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::AlterAddColumn,
        payload,
    })
}

fn compile_drop_table(
    object_type: fp_core::sql_ast::ObjectType,
    names: Vec<ObjectName>,
    req_id: u32,
) -> Result<Frame, String> {
    if object_type != fp_core::sql_ast::ObjectType::Table {
        return Err("only DROP TABLE is supported".to_string());
    }
    if names.len() != 1 {
        return Err("DROP TABLE supports one table at a time".to_string());
    }
    let table = names
        .into_iter()
        .next()
        .expect("table")
        .to_string();
    let payload = encode_drop_table_payload(&table)
        .map_err(|err| format!("encode drop table payload failed: {err}"))?;
    Ok(Frame {
        flags: 0,
        req_id,
        op: Op::DropTable,
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
    if let Some(col_query) = build_column_query(&query)? {
        let payload = encode_query_col_payload(&col_query)
            .map_err(|err| format!("encode query_col payload failed: {err}"))?;
        return Ok(Frame {
            flags: 0,
            req_id,
            op: Op::QueryCol,
            payload,
        });
    }
    Err("SELECT uses unsupported features (only column-based SELECT is supported)".to_string())
}

fn compile_delete(
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    req_id: u32,
) -> Result<Frame, String> {
    let table = extract_table(&from)?;
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

#[derive(Debug, Clone)]
enum ManualDdl {
    AlterDropColumn { table: String, column: String },
    AlterRenameColumn { table: String, from: String, to: String },
    AlterSetDefault {
        table: String,
        column: String,
        default: PxlValue,
    },
    RenameTable { from: String, to: String },
}

fn parse_manual_ddl(input: &str) -> Result<Option<ManualDdl>, String> {
    let trimmed = input.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens.len() < 4 {
        return Ok(None);
    }
    let upper: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();
    if upper[0] == "RENAME" && upper[1] == "TABLE" && upper.len() >= 5 && upper[3] == "TO" {
        return Ok(Some(ManualDdl::RenameTable {
            from: tokens[2].to_string(),
            to: tokens[4].to_string(),
        }));
    }
    if upper[0] == "ALTER" && upper[1] == "TABLE" && upper.len() >= 6 {
        let table = tokens[2].to_string();
        if upper[3] == "DROP" && upper.get(4) == Some(&"COLUMN".to_string()) {
            return Ok(Some(ManualDdl::AlterDropColumn {
                table,
                column: tokens[5].to_string(),
            }));
        }
        if upper[3] == "RENAME"
            && upper.get(4) == Some(&"COLUMN".to_string())
            && upper.len() >= 8
            && upper[6] == "TO"
        {
            return Ok(Some(ManualDdl::AlterRenameColumn {
                table,
                from: tokens[5].to_string(),
                to: tokens[7].to_string(),
            }));
        }
        if upper[3] == "ALTER" && upper.get(4) == Some(&"COLUMN".to_string()) {
            let column = tokens[5].to_string();
            if let Some(set_idx) = upper.iter().position(|v| v == "SET") {
                if upper.get(set_idx + 1) == Some(&"DEFAULT".to_string())
                    && set_idx + 2 < tokens.len()
                {
                    let expr_tokens = &tokens[(set_idx + 2)..];
                    let expr = expr_tokens.join(" ");
                    let default = parse_default_expr(&expr)?;
                    return Ok(Some(ManualDdl::AlterSetDefault {
                        table,
                        column,
                        default,
                    }));
                }
            }
        }
    }
    Ok(None)
}

fn manual_to_frame(manual: ManualDdl, req_id: u32) -> Result<Frame, String> {
    match manual {
        ManualDdl::AlterDropColumn { table, column } => {
            let payload = encode_alter_drop_column_payload(&table, &column)
                .map_err(|err| format!("encode alter drop column payload failed: {err}"))?;
            Ok(Frame {
                flags: 0,
                req_id,
                op: Op::AlterDropColumn,
                payload,
            })
        }
        ManualDdl::AlterRenameColumn { table, from, to } => {
            let payload = encode_alter_rename_column_payload(&table, &from, &to)
                .map_err(|err| format!("encode alter rename column payload failed: {err}"))?;
            Ok(Frame {
                flags: 0,
                req_id,
                op: Op::AlterRenameColumn,
                payload,
            })
        }
        ManualDdl::AlterSetDefault {
            table,
            column,
            default,
        } => {
            let payload = encode_alter_set_default_payload(&table, &column, Some(&default))
                .map_err(|err| format!("encode alter set default payload failed: {err}"))?;
            Ok(Frame {
                flags: 0,
                req_id,
                op: Op::AlterSetDefault,
                payload,
            })
        }
        ManualDdl::RenameTable { from, to } => {
            let payload = encode_rename_table_payload(&from, &to)
                .map_err(|err| format!("encode rename table payload failed: {err}"))?;
            Ok(Frame {
                flags: 0,
                req_id,
                op: Op::RenameTable,
                payload,
            })
        }
    }
}

fn column_default_from_options(
    options: &[fp_core::sql_ast::ColumnOptionDef],
) -> Result<Option<PxlValue>, String> {
    let mut found: Option<PxlValue> = None;
    for option in options {
        match &option.option {
            ColumnOption::Default(expr) => {
                if found.is_some() {
                    return Err("column default specified more than once".to_string());
                }
                let value = expr_to_literal_value(expr)?;
                found = Some(value);
            }
            ColumnOption::Materialized(_) => {
                return Err("MATERIALIZED columns are not supported".to_string());
            }
        }
    }
    Ok(found)
}

fn parse_default_expr(expr: &str) -> Result<PxlValue, String> {
    let sql = format!("SELECT {expr}");
    let statement = parse_single_sql(&sql)?;
    let Statement::Query(query) = statement else {
        return Err("DEFAULT expression parse failed".to_string());
    };
    let SetExpr::Select(select) = *query.body else {
        return Err("DEFAULT expression parse failed".to_string());
    };
    let select = *select;
    if select.projection.len() != 1 {
        return Err("DEFAULT expression must be a single literal".to_string());
    }
    match &select.projection[0] {
        fp_core::sql_ast::SelectItem::UnnamedExpr(expr)
        | fp_core::sql_ast::SelectItem::ExprWithAlias { expr, .. } => {
            expr_to_literal_value(expr)
        }
        _ => Err("DEFAULT expression must be a literal".to_string()),
    }
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

fn build_column_query(query: &fp_core::sql_ast::Query) -> Result<Option<ColumnQuery>, String> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let select = select.as_ref();
    validate_select_shape(select)?;
    let table = match extract_table(&select.from) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let group_by = extract_group_by(&select.group_by)?;
    if !group_by.is_empty() {
        return Ok(None);
    }
    if select
        .projection
        .iter()
        .any(|item| matches!(item, fp_core::sql_ast::SelectItem::Wildcard))
    {
        return Ok(None);
    }
    let mut columns = Vec::new();
    let filter = match select.selection.as_ref() {
        Some(expr) => {
            let predicate = expr_to_predicate(expr)?;
            expr_to_col_instrs(&predicate, &mut columns)?
        }
        None => Vec::new(),
    };
    let project = build_project_list(&select.projection, &mut columns)?;
    Ok(Some(ColumnQuery {
        table,
        columns,
        filter,
        project,
    }))
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

fn extract_table(from: &[TableWithJoins]) -> Result<String, String> {
    if from.len() != 1 {
        return Err("query must reference exactly one FROM item".to_string());
    }
    let base = &from[0];
    if !base.joins.is_empty() {
        return Err("JOIN is not supported".to_string());
    }
    let (table, _alias) = extract_table_name(&base.relation)?;
    Ok(table)
}

fn extract_table_name(relation: &TableFactor) -> Result<(String, Option<String>), String> {
    match relation {
        TableFactor::Table { name, alias } => Ok((name.to_string(), alias.as_ref().map(|a| a.name.value.clone()))),
        _ => Err("only direct table references are supported".to_string()),
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

fn build_project_list(
    projection: &[fp_core::sql_ast::SelectItem],
    columns: &mut Vec<String>,
) -> Result<Vec<ColumnProjectItem>, String> {
    let mut items = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            fp_core::sql_ast::SelectItem::Wildcard => {
                return Err("wildcard not supported for column bytecode".to_string());
            }
            fp_core::sql_ast::SelectItem::UnnamedExpr(expr) => {
                let (name, expr) = project_expr(expr, None, columns)?;
                items.push(ColumnProjectItem { name, expr });
            }
            fp_core::sql_ast::SelectItem::ExprWithAlias { expr, alias } => {
                let (name, expr) = project_expr(expr, Some(&alias.value), columns)?;
                items.push(ColumnProjectItem { name, expr });
            }
        }
    }
    Ok(items)
}

fn project_expr(
    expr: &Expr,
    alias: Option<&str>,
    columns: &mut Vec<String>,
) -> Result<(String, ColumnProjectExpr), String> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let name = expr_to_column_name(expr)?;
            let idx = intern_column(columns, &name)?;
            let out_name = alias.map(|v| v.to_string()).unwrap_or_else(|| name.clone());
            Ok((out_name, ColumnProjectExpr::Column(idx)))
        }
        Expr::Value(_) | Expr::UnaryOp { .. } | Expr::Nested(_) => {
            let value = expr_to_literal_value(expr)?;
            let out_name = alias.map(|v| v.to_string()).unwrap_or_else(|| "literal".to_string());
            Ok((out_name, ColumnProjectExpr::Literal(value)))
        }
        Expr::Function(_) => Err("function not supported for column bytecode".to_string()),
        _ => Err("projection not supported for column bytecode".to_string()),
    }
}

fn expr_to_col_instrs(expr: &PxlExpr, columns: &mut Vec<String>) -> Result<Vec<ColumnInstr>, String> {
    let mut out = Vec::new();
    expr_to_col_instrs_inner(expr, columns, &mut out)?;
    Ok(out)
}

fn expr_to_col_instrs_inner(
    expr: &PxlExpr,
    columns: &mut Vec<String>,
    out: &mut Vec<ColumnInstr>,
) -> Result<(), String> {
    match expr {
        PxlExpr::Column(name) => {
            let idx = intern_column(columns, name)?;
            out.push(ColumnInstr::PushCol(idx));
            Ok(())
        }
        PxlExpr::Literal(value) => {
            out.push(ColumnInstr::PushLit(value.clone()));
            Ok(())
        }
        PxlExpr::Binary { op, left, right } => {
            expr_to_col_instrs_inner(left, columns, out)?;
            expr_to_col_instrs_inner(right, columns, out)?;
            out.push(ColumnInstr::Cmp(*op));
            Ok(())
        }
        PxlExpr::And(left, right) => {
            expr_to_col_instrs_inner(left, columns, out)?;
            expr_to_col_instrs_inner(right, columns, out)?;
            out.push(ColumnInstr::And);
            Ok(())
        }
        PxlExpr::Or(left, right) => {
            expr_to_col_instrs_inner(left, columns, out)?;
            expr_to_col_instrs_inner(right, columns, out)?;
            out.push(ColumnInstr::Or);
            Ok(())
        }
        PxlExpr::Not(inner) => {
            expr_to_col_instrs_inner(inner, columns, out)?;
            out.push(ColumnInstr::Not);
            Ok(())
        }
    }
}

fn intern_column(columns: &mut Vec<String>, name: &str) -> Result<u16, String> {
    if let Some((idx, _)) = columns.iter().enumerate().find(|(_, v)| v.as_str() == name) {
        return Ok(idx as u16);
    }
    if columns.len() >= u16::MAX as usize {
        return Err("too many columns in bytecode".to_string());
    }
    columns.push(name.to_string());
    Ok((columns.len() - 1) as u16)
}

fn map_data_type(data_type: &DataType) -> Result<ColumnType, String> {
    match data_type {
        DataType::Boolean => Ok(ColumnType::Bool),
        DataType::Int64 | DataType::UnsignedBigInt => Ok(ColumnType::I64),
        DataType::Float64 | DataType::Decimal { .. } => Ok(ColumnType::F64),
        DataType::Date | DataType::DateTime | DataType::DateTime64(_) => Ok(ColumnType::Timestamp),
        DataType::String => Ok(ColumnType::String),
        DataType::Uuid | DataType::Ipv4 => Ok(ColumnType::String),
        DataType::Custom(name, _) => {
            let name = name.to_ascii_lowercase();
            match name.as_str() {
                "symbol" => Ok(ColumnType::Symbol),
                "bytes" | "binary" | "blob" => Ok(ColumnType::Bytes),
                "timestamp" | "ts" => Ok(ColumnType::Timestamp),
                _ => Err(format!("unsupported column type: {name}")),
            }
        }
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
        decode_alter_add_column_payload, decode_alter_drop_column_payload,
        decode_alter_rename_column_payload, decode_alter_set_default_payload,
        decode_delete_payload, decode_drop_table_payload,
        decode_insert_payload, decode_query_col_payload,
        decode_rename_table_payload, decode_schema_payload, decode_update_payload, read_frame,
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
    fn compile_create_table_with_defaults() {
        let sql = "CREATE TABLE ticks (symbol String DEFAULT 'AAPL', ts Int64 DEFAULT 1, value Float64 DEFAULT 1.5)";
        let bytes = compile_to_frame(sql, InputKind::Sql, 11).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::Create);
        let (_, schema) = decode_schema_payload(&frame.payload).expect("payload");
        let symbol = schema.columns().iter().find(|c| c.name == "symbol").expect("symbol");
        let ts = schema.columns().iter().find(|c| c.name == "ts").expect("ts");
        let value = schema.columns().iter().find(|c| c.name == "value").expect("value");
        assert_eq!(symbol.default, Some(PxlValue::String("AAPL".to_string())));
        assert_eq!(ts.default, Some(PxlValue::I64(1)));
        assert_eq!(value.default, Some(PxlValue::F64(1.5)));
    }

    #[test]
    fn compile_alter_add_column_with_default() {
        let sql = "ALTER TABLE ticks ADD COLUMN lot Int64 DEFAULT 100";
        let bytes = compile_to_frame(sql, InputKind::Sql, 12).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::AlterAddColumn);
        let (table, column) = decode_alter_add_column_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(column.name, "lot");
        assert_eq!(column.default, Some(PxlValue::I64(100)));
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
    fn compile_select_sql_to_query_col() {
        let sql = "SELECT value FROM ticks WHERE symbol = 'AAPL'";
        let bytes = compile_to_frame(sql, InputKind::Sql, 7).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::QueryCol);
        let query = decode_query_col_payload(&frame.payload).expect("payload");
        assert_eq!(query.table, "ticks");
        assert!(!query.filter.is_empty());
        assert_eq!(query.project.len(), 1);
    }

    #[test]
    fn compile_select_sql_rejects_group_by() {
        let sql = "SELECT symbol, avg(value) FROM ticks GROUP BY symbol";
        let err = compile_to_frame(sql, InputKind::Sql, 99).expect_err("compile");
        assert!(err.contains("unsupported"));
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
    fn compile_drop_table_sql() {
        let sql = "DROP TABLE ticks";
        let bytes = compile_to_frame(sql, InputKind::Sql, 13).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::DropTable);
        let table = decode_drop_table_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
    }

    #[test]
    fn compile_rename_table_manual() {
        let sql = "RENAME TABLE ticks TO trades";
        let bytes = compile_to_frame(sql, InputKind::Sql, 14).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::RenameTable);
        let (from, to) = decode_rename_table_payload(&frame.payload).expect("payload");
        assert_eq!(from, "ticks");
        assert_eq!(to, "trades");
    }

    #[test]
    fn compile_alter_drop_column_manual() {
        let sql = "ALTER TABLE ticks DROP COLUMN price";
        let bytes = compile_to_frame(sql, InputKind::Sql, 15).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::AlterDropColumn);
        let (table, column) = decode_alter_drop_column_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(column, "price");
    }

    #[test]
    fn compile_alter_rename_column_manual() {
        let sql = "ALTER TABLE ticks RENAME COLUMN price TO px";
        let bytes = compile_to_frame(sql, InputKind::Sql, 16).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::AlterRenameColumn);
        let (table, from, to) =
            decode_alter_rename_column_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(from, "price");
        assert_eq!(to, "px");
    }

    #[test]
    fn compile_alter_set_default_manual() {
        let sql = "ALTER TABLE ticks ALTER COLUMN price SET DEFAULT 9";
        let bytes = compile_to_frame(sql, InputKind::Sql, 17).expect("compile");
        let frame = decode_frame(bytes);
        assert_eq!(frame.op, Op::AlterSetDefault);
        let (table, column, default) =
            decode_alter_set_default_payload(&frame.payload).expect("payload");
        assert_eq!(table, "ticks");
        assert_eq!(column, "price");
        assert_eq!(default, Some(PxlValue::I64(9)));
    }

    #[test]
    fn compile_create_table_as_select() {
        let sql = "CREATE TABLE snap AS SELECT symbol, price FROM ticks";
        let err = compile_to_frame(sql, InputKind::Sql, 18).expect_err("compile");
        assert!(err.contains("CTAS"));
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
        assert_eq!(frame.op, Op::QueryCol);
        let query = decode_query_col_payload(&frame.payload).expect("payload");
        assert_eq!(query.table, "ticks");
    }

    #[test]
    fn compile_sql_multiple_statements_error() {
        let sql = "SELECT 1; SELECT 2";
        let err = compile_to_frame(sql, InputKind::Sql, 1).expect_err("error");
        assert!(err.contains("multiple"));
    }

    #[test]
    fn compile_insert_requires_columns_error() {
        let sql = "INSERT INTO ticks VALUES (1)";
        let err = compile_to_frame(sql, InputKind::Sql, 1).expect_err("error");
        assert!(err.contains("column list"));
    }

    #[test]
    fn compile_create_table_as_select_error() {
        let sql = "CREATE TABLE t AS SELECT 1";
        let err = compile_to_frame(sql, InputKind::Sql, 1).expect_err("error");
        assert!(err.contains("CTAS"));
    }

    #[test]
    fn compile_prql_error_path() {
        let err = compile_to_frame("from", InputKind::Prql, 1).expect_err("error");
        assert!(err.contains("PRQL"));
    }
}
