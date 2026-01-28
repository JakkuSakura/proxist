use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::Ipv4Addr;

use anyhow::anyhow;
use chrono::{Datelike, NaiveDateTime, TimeZone, Utc};
use fp_sql::ast::{
    BinaryOperator, DataType as AstDataType, Expr, Function, FunctionArg, FunctionArgExpr,
    GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, OrderByExpr, Query, Select,
    SelectItem, SetExpr, Statement, Value as AstValue, WindowFrameBound, WindowFrameUnits,
    WindowType,
};
use fp_sql::dialect::ClickHouseDialect;
use fp_sql::parser::Parser;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, Tokenizer, Word, Whitespace};
use serde_json::{Map, Value as JsonValue};
use uuid::Uuid;

use crate::scheduler::SqlResult;

#[derive(Debug, Clone)]
struct ColumnDefn {
    name: String,
    data_type: DataType,
    default: Option<Expr>,
    materialized: Option<Expr>,
}

#[derive(Debug, Clone)]
struct Table {
    name: String,
    columns: Vec<ColumnDefn>,
    rows: Vec<Vec<Value>>,
    ttl: Option<Expr>,
    sample_by: Option<Expr>,
}

#[derive(Debug, Clone)]
struct View {
    name: String,
    query: Query,
}

#[derive(Debug, Clone)]
enum DataType {
    Null,
    Bool,
    Int64,
    UInt64,
    Float64,
    Decimal { precision: u8, scale: u8 },
    String,
    DateTime,
    DateTime64(u32),
    Uuid,
    Ipv4,
    Array(Box<DataType>),
    Tuple(Vec<DataType>),
    Map(Box<DataType>, Box<DataType>),
    LowCardinality(Box<DataType>),
    Enum8(Vec<(String, i8)>),
    Enum16(Vec<(String, i16)>),
    AggregateFunction(String, Vec<DataType>),
    Nested(Vec<(String, DataType)>),
    Nullable(Box<DataType>),
}

#[derive(Debug, Clone, PartialEq)]
struct Decimal {
    value: i128,
    scale: u32,
}

impl Decimal {
    fn parse(input: &str) -> Option<Self> {
        let trimmed = input.trim();
        let mut parts = trimmed.split('.');
        let whole = parts.next().unwrap_or("0");
        let frac = parts.next();
        if parts.next().is_some() {
            return None;
        }
        let scale = frac.map(|s| s.len() as u32).unwrap_or(0);
        let mut digits = String::new();
        digits.push_str(whole.trim_start_matches('+'));
        if let Some(frac_part) = frac {
            digits.push_str(frac_part);
        }
        let value = digits.parse::<i128>().ok()?;
        Some(Self { value, scale })
    }

    fn to_f64(&self) -> f64 {
        let divisor = 10f64.powi(self.scale as i32);
        self.value as f64 / divisor
    }

    fn round(&self, scale: u32) -> Self {
        let value = self.to_f64();
        let factor = 10f64.powi(scale as i32);
        let rounded = (value * factor).round() / factor;
        let scaled = (rounded * factor).round() as i128;
        Self {
            value: scaled,
            scale,
        }
    }

    fn to_string_with_scale(&self) -> String {
        if self.scale == 0 {
            return self.value.to_string();
        }
        let sign = if self.value < 0 { "-" } else { "" };
        let abs = self.value.abs();
        let factor = 10i128.pow(self.scale);
        let whole = abs / factor;
        let frac = (abs % factor).to_string();
        let padded = format!("{:0>width$}", frac, width = self.scale as usize);
        format!("{sign}{whole}.{padded}")
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DateTimeValue {
    micros: i64,
    precision: u32,
}

impl DateTimeValue {
    fn from_micros(micros: i64, precision: u32) -> Self {
        let scale = 10i64.pow(6u32.saturating_sub(precision) as u32);
        let truncated = if scale > 1 {
            micros / scale * scale
        } else {
            micros
        };
        Self {
            micros: truncated,
            precision,
        }
    }

    fn from_datetime(dt: chrono::DateTime<Utc>, precision: u32) -> Self {
        let micros = dt.timestamp_micros();
        Self::from_micros(micros, precision)
    }

    fn to_chrono(&self) -> chrono::DateTime<Utc> {
        let naive = NaiveDateTime::from_timestamp_micros(self.micros)
            .unwrap_or_else(|| NaiveDateTime::from_timestamp_opt(0, 0).unwrap());
        Utc.from_utc_datetime(&naive)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct AggregateState {
    count: u64,
    sum: f64,
}

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Decimal(Decimal),
    String(String),
    DateTime(DateTimeValue),
    DateTime64(DateTimeValue),
    Uuid(Uuid),
    Ipv4(Ipv4Addr),
    Array(Vec<Value>),
    Tuple(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Aggregate(AggregateState),
}

impl Value {
    fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(v) => Some(*v as f64),
            Value::UInt(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            Value::Decimal(v) => Some(v.to_f64()),
            Value::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            Value::String(v) => v.parse::<f64>().ok(),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            Value::UInt(v) => i64::try_from(*v).ok(),
            Value::Float(v) => Some(*v as i64),
            Value::Decimal(v) => Some(v.to_f64() as i64),
            Value::Bool(v) => Some(if *v { 1 } else { 0 }),
            Value::String(v) => v.parse::<i64>().ok(),
            _ => None,
        }
    }

    fn as_u64(&self) -> Option<u64> {
        match self {
            Value::UInt(v) => Some(*v),
            Value::Int(v) => u64::try_from(*v).ok(),
            Value::Float(v) => Some(*v as u64),
            Value::Decimal(v) => Some(v.to_f64() as u64),
            Value::Bool(v) => Some(if *v { 1 } else { 0 }),
            Value::String(v) => v.parse::<u64>().ok(),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            Value::Int(v) => Some(*v != 0),
            Value::UInt(v) => Some(*v != 0),
            Value::Float(v) => Some(*v != 0.0),
            Value::Decimal(v) => Some(v.to_f64() != 0.0),
            Value::String(v) => match v.to_ascii_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    fn to_string_value(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(v) => v.to_string(),
            Value::Int(v) => v.to_string(),
            Value::UInt(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::Decimal(v) => v.to_string_with_scale(),
            Value::String(v) => v.clone(),
            Value::DateTime(v) => v.to_chrono().format("%Y-%m-%d %H:%M:%S").to_string(),
            Value::DateTime64(v) => {
                let base = v.to_chrono().format("%Y-%m-%d %H:%M:%S").to_string();
                if v.precision == 0 {
                    base
                } else {
                    let micros = v.micros.rem_euclid(1_000_000);
                    let scale = 10u32.pow(6u32.saturating_sub(v.precision));
                    let fraction = micros / scale as i64;
                    format!("{base}.{:0>width$}", fraction, width = v.precision as usize)
                }
            }
            Value::Uuid(v) => v.to_string(),
            Value::Ipv4(v) => v.to_string(),
            Value::Array(values) => {
                let inner = values
                    .iter()
                    .map(|v| v.to_string_value())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{inner}]")
            }
            Value::Tuple(values) => {
                let inner = values
                    .iter()
                    .map(|v| v.to_string_value())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({inner})")
            }
            Value::Map(values) => {
                let inner = values
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k.to_string_value(), v.to_string_value()))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{inner}}}")
            }
            Value::Aggregate(state) => format!("count={},sum={}", state.count, state.sum),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ValueKey {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(i64),
    Decimal(i128, u32),
    String(String),
    DateTime(i64, u32),
    Uuid(String),
    Ipv4(u32),
    Array(Vec<ValueKey>),
    Tuple(Vec<ValueKey>),
    Map(Vec<(ValueKey, ValueKey)>),
    Aggregate(u64, i64),
}

impl ValueKey {
    fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => ValueKey::Null,
            Value::Bool(v) => ValueKey::Bool(*v),
            Value::Int(v) => ValueKey::Int(*v),
            Value::UInt(v) => ValueKey::UInt(*v),
            Value::Float(v) => ValueKey::Float(v.to_bits() as i64),
            Value::Decimal(v) => ValueKey::Decimal(v.value, v.scale),
            Value::String(v) => ValueKey::String(v.clone()),
            Value::DateTime(v) => ValueKey::DateTime(v.micros, v.precision),
            Value::DateTime64(v) => ValueKey::DateTime(v.micros, v.precision),
            Value::Uuid(v) => ValueKey::Uuid(v.to_string()),
            Value::Ipv4(v) => ValueKey::Ipv4(u32::from(*v)),
            Value::Array(values) => {
                ValueKey::Array(values.iter().map(ValueKey::from_value).collect())
            }
            Value::Tuple(values) => {
                ValueKey::Tuple(values.iter().map(ValueKey::from_value).collect())
            }
            Value::Map(values) => ValueKey::Map(
                values
                    .iter()
                    .map(|(k, v)| (ValueKey::from_value(k), ValueKey::from_value(v)))
                    .collect(),
            ),
            Value::Aggregate(state) => ValueKey::Aggregate(state.count, state.sum.to_bits() as i64),
        }
    }
}

#[derive(Debug, Clone)]
struct ColumnRef {
    name: String,
    qualifier: Option<String>,
}

#[derive(Debug, Clone)]
struct DataSet {
    columns: Vec<ColumnRef>,
    rows: Vec<Vec<Value>>,
}

impl DataSet {
    fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct CreateTableInfo {
    name: String,
    columns: Vec<ColumnDefn>,
    engine: Option<String>,
    as_table: Option<String>,
    query: Option<Query>,
    order_by: Option<Vec<String>>,
    ttl: Option<Expr>,
    sample_by: Option<Expr>,
}

pub struct HotSqlEngine {
    tables: HashMap<String, Table>,
    views: HashMap<String, View>,
}

impl HotSqlEngine {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            views: HashMap::new(),
        }
    }

    pub fn execute(&mut self, sql: &str) -> anyhow::Result<SqlResult> {
        let sql = rewrite_insert_columns_with_dots(sql);
        let (sql, format) = split_format_clause(&sql);
        let normalized = sql.trim();
        if normalized.is_empty() {
            return Ok(SqlResult::Text(String::new()));
        }
        let normalized_lower = normalized.to_ascii_lowercase();

        if let Some(mutation) = parse_alter_mutation(normalized)? {
            return self.execute_mutation(mutation);
        }

        if normalized_lower.starts_with("create") && normalized_lower.contains("table") {
            let info = parse_create_table(normalized)?;
            return self.execute_create_table(info);
        }

        let (normalized, sample_ratio) = split_sample_clause(normalized);
        let stmt = match parse_statement(&normalized) {
            Ok(stmt) => stmt,
            Err(err) => {
                if normalized_lower.contains("create table") {
                    let info = parse_create_table(normalized.as_str())?;
                    return self.execute_create_table(info);
                }
                return Err(err);
            }
        };
        match stmt {
            Statement::CreateView { name, query, .. } => {
                let key = name.to_string().to_ascii_lowercase();
                self.views.insert(
                    key.clone(),
                    View {
                        name: key,
                        query: *query,
                    },
                );
                Ok(SqlResult::Text("Ok.\n".to_string()))
            }
            Statement::Drop { object_type, names, .. } => {
                if matches!(object_type, fp_sql::ast::ObjectType::Table) {
                    for name in &names {
                        self.tables.remove(&name.to_string().to_ascii_lowercase());
                    }
                }
                if matches!(object_type, fp_sql::ast::ObjectType::View) {
                    for name in &names {
                        self.views.remove(&name.to_string().to_ascii_lowercase());
                    }
                }
                Ok(SqlResult::Text("Ok.\n".to_string()))
            }
            Statement::AlterTable { name, operations, .. } => {
                let table_name = name.to_string().to_ascii_lowercase();
                let mut table = match self.tables.remove(&table_name) {
                    Some(table) => table,
                    None => return Ok(SqlResult::Text("Ok.\n".to_string())),
                };
                let column_refs = column_refs_from_defs(&table.columns);
                for op in operations {
                    if let fp_sql::ast::AlterTableOperation::AddColumn { column_def, .. } = op {
                        let name = column_def.name.value.to_ascii_lowercase();
                        let mut default = None;
                        for opt in column_def.options {
                            if let fp_sql::ast::ColumnOption::Default(expr) = opt.option {
                                default = Some(expr);
                            }
                        }
                        let data_type = map_data_type(&column_def.data_type);
                        table.columns.push(ColumnDefn {
                            name: name.clone(),
                            data_type,
                            default: default.clone(),
                            materialized: None,
                        });
                        for row in &mut table.rows {
                            let value = default
                                .as_ref()
                                .map(|expr| self.eval_expr(expr, &column_refs, row))
                                .unwrap_or(Value::Null);
                            row.push(value);
                        }
                    }
                }
                self.tables.insert(table_name, table);
                Ok(SqlResult::Text("Ok.\n".to_string()))
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.execute_insert(table_name, columns, source, Some(normalized.as_str())),
            Statement::Query(query) => {
                let (dataset, format_hint) =
                    self.execute_query(&query, format.as_deref(), sample_ratio)?;
                let output = render_dataset(dataset, format_hint)?;
                Ok(output)
            }
            _ => Ok(SqlResult::Text("".to_string())),
        }
    }

    fn execute_mutation(&mut self, mutation: Mutation) -> anyhow::Result<SqlResult> {
        let table_name = mutation.table.to_ascii_lowercase();
        let mut table = match self.tables.remove(&table_name) {
            Some(table) => table,
            None => return Ok(SqlResult::Text("Ok.\n".to_string())),
        };
        let column_refs = column_refs_from_defs(&table.columns);
        match mutation.kind {
            MutationKind::Update { assignments, selection } => {
                for row in &mut table.rows {
                    if let Some(expr) = selection.as_ref() {
                        let keep = self
                            .eval_expr(expr, &column_refs, row)
                            .as_bool()
                            .unwrap_or(false);
                        if !keep {
                            continue;
                        }
                    }
                    for (name, expr) in &assignments {
                        if let Some(idx) = table
                            .columns
                            .iter()
                            .position(|col| col.name == *name)
                        {
                            let value = self.eval_expr(expr, &column_refs, row);
                            row[idx] = value;
                        }
                    }
                }
            }
            MutationKind::Delete { selection } => {
                table.rows.retain(|row| {
                    if let Some(expr) = selection.as_ref() {
                        let keep = self
                            .eval_expr(expr, &column_refs, row)
                            .as_bool()
                            .unwrap_or(false);
                        !keep
                    } else {
                        false
                    }
                });
            }
        }
        self.tables.insert(table_name, table);
        Ok(SqlResult::Text("Ok.\n".to_string()))
    }

    fn execute_create_table(&mut self, info: CreateTableInfo) -> anyhow::Result<SqlResult> {
        let mut columns = info.columns.clone();
        if let Some(source) = info.as_table.as_ref() {
            if let Some(existing) = self.tables.get(source) {
                columns = existing.columns.clone();
            }
        }

        let table_name = info.name.clone();
        let mut table = Table {
            name: table_name.clone(),
            columns,
            rows: Vec::new(),
            ttl: info.ttl,
            sample_by: info.sample_by,
        };

        if let Some(query) = info.query {
            let (dataset, _) = self.execute_query(&query, None, None)?;
            apply_rows_to_table(&mut table, dataset.rows, self);
        }

        self.tables.insert(table_name, table);
        Ok(SqlResult::Text("Ok.\n".to_string()))
    }

    fn execute_insert(
        &mut self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Option<Box<Query>>,
        raw_sql: Option<&str>,
    ) -> anyhow::Result<SqlResult> {
        let table_name = table_name.to_string().to_ascii_lowercase();
        let mut table = match self.tables.remove(&table_name) {
            Some(table) => table,
            None => return Ok(SqlResult::Text("Ok.\n".to_string())),
        };

        let target_columns: Vec<String> = if columns.is_empty() {
            table.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            columns.iter().map(|c| c.value.to_ascii_lowercase()).collect()
        };

        let mut rows_to_insert: Vec<Vec<Value>> =
            match source.as_deref().map(|q| q.body.as_ref()) {
                Some(SetExpr::Values(values)) => values
                    .rows
                    .iter()
                    .map(|row_exprs| {
                        row_exprs
                            .iter()
                            .map(|expr| self.eval_expr(expr, &[], &[]))
                            .collect()
                    })
                    .collect(),
                Some(SetExpr::Select(select)) => {
                    let query = Query {
                        with: None,
                        body: SetExpr::Select(select.clone()).into(),
                        order_by: Vec::new(),
                        limit: None,
                        limit_by: Vec::new(),
                        offset: None,
                        fetch: None,
                        locks: Vec::new(),
                        for_clause: None,
                    };
                    let (dataset, _) = self.execute_query(&query, None, None)?;
                    dataset.rows
                }
                _ => Vec::new(),
            };
        if rows_to_insert.is_empty() {
            if let Some(raw_sql) = raw_sql {
                if let Ok(rows) = parse_insert_values_fallback(raw_sql, self) {
                    rows_to_insert = rows;
                }
            }
        }

        let column_refs = column_refs_from_defs(&table.columns);

        for row_values in rows_to_insert {
            let ts_idx = target_columns.iter().position(|c| c == "ts");
            let ts_micros_idx = table.columns.iter().position(|c| c.name == "ts_micros");
            let mut row = vec![Value::Null; table.columns.len()];
            for (idx, value) in row_values.iter().cloned().enumerate() {
                if let Some(col_name) = target_columns.get(idx) {
                    if let Some(pos) = table.columns.iter().position(|c| &c.name == col_name) {
                        row[pos] = value;
                    } else if let Some(pos) =
                        table.columns.iter().position(|c| c.name == format!("{col_name}_micros"))
                    {
                        if let Some(micros) = datetime_to_micros(&value) {
                            row[pos] = Value::Int(micros);
                        }
                    }
                }
            }
            if let (Some(ts_pos), Some(micros_pos)) = (ts_idx, ts_micros_idx) {
                if matches!(row[micros_pos], Value::Null) {
                    if let Some(value) = row_values.get(ts_pos) {
                        if let Some(micros) = datetime_to_micros(value) {
                            row[micros_pos] = Value::Int(micros);
                        }
                    }
                }
            }
            for (idx, col) in table.columns.iter().enumerate() {
                if let Some(expr) = &col.materialized {
                    row[idx] = self.eval_expr(expr, &column_refs, &row);
                } else if matches!(row[idx], Value::Null) {
                    if let Some(expr) = &col.default {
                        row[idx] = self.eval_expr(expr, &column_refs, &row);
                    }
                }
            }
            table.rows.push(row);
        }
        self.tables.insert(table_name, table);
        Ok(SqlResult::Text("Ok.\n".to_string()))
    }


    fn execute_query(
        &mut self,
        query: &Query,
        format_hint: Option<&str>,
        sample_ratio: Option<f64>,
    ) -> anyhow::Result<(DataSet, Option<FormatHint>)> {
        let mut dataset = match &*query.body {
            SetExpr::Select(select) => self.execute_select(select, query, sample_ratio)?,
            SetExpr::SetOperation {
                left,
                right,
                op,
                set_quantifier,
            } => {
                let left = self.execute_query(
                    &Query {
                        with: None,
                        body: left.clone(),
                        order_by: Vec::new(),
                        limit: None,
                        limit_by: Vec::new(),
                        offset: None,
                        fetch: None,
                        locks: Vec::new(),
                        for_clause: None,
                    },
                    None,
                    sample_ratio,
                )?;
                let right = self.execute_query(
                    &Query {
                        with: None,
                        body: right.clone(),
                        order_by: Vec::new(),
                        limit: None,
                        limit_by: Vec::new(),
                        offset: None,
                        fetch: None,
                        locks: Vec::new(),
                        for_clause: None,
                    },
                    None,
                    sample_ratio,
                )?;
                if !matches!(op, fp_sql::ast::SetOperator::Union)
                    || !matches!(set_quantifier, fp_sql::ast::SetQuantifier::All)
                {
                    return Err(anyhow!("only UNION ALL is supported"));
                }
                let mut combined = left.0;
                combined.rows.extend(right.0.rows);
                combined
            }
            _ => return Err(anyhow!("unsupported query")),
        };

        let format_hint = parse_format_hint(format_hint);

        if !query.order_by.is_empty() {
            apply_order_by(&mut dataset, &query.order_by, self)?;
        }

        if let Some(offset) = &query.offset {
            let offset_value = self
                .eval_expr(&offset.value, &[], &[])
                .as_u64()
                .unwrap_or(0) as usize;
            if offset_value < dataset.rows.len() {
                dataset.rows = dataset.rows[offset_value..].to_vec();
            } else {
                dataset.rows.clear();
            }
        }

        if let Some(limit) = &query.limit {
            let limit_value = self.eval_expr(limit, &[], &[]).as_u64().unwrap_or(0) as usize;
            if dataset.rows.len() > limit_value {
                dataset.rows.truncate(limit_value);
            }
        }

        Ok((dataset, format_hint))
    }

    fn execute_select(
        &mut self,
        select: &Select,
        query: &Query,
        sample_ratio: Option<f64>,
    ) -> anyhow::Result<DataSet> {
        let mut dataset = self.build_from(select)?;

        if let Some(selection) = &select.selection {
            dataset.rows.retain(|row| {
                self.eval_expr(selection, &dataset.columns, row)
                    .as_bool()
                    .unwrap_or(false)
            });
        }

        if let Some(table_name) = select.from.get(0).and_then(|from| match &from.relation {
            fp_sql::ast::TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => None,
        }) {
            if let Some(table) = self.tables.get(&table_name.to_ascii_lowercase()).cloned() {
                let columns = table.columns_as_refs();
                let ttl = table.ttl;
                if let Some(expr) = ttl {
                    dataset.rows.retain(|row| !is_ttl_expired(&expr, &columns, row, self));
                }
            }
        }

        if let Some(ratio) = sample_ratio {
            dataset = apply_sample(dataset, select, ratio, self)?;
        }

        let dataset = apply_array_join(dataset, select, self)?;

        let window_values = compute_window_values(&dataset, select, self)?;

        let needs_group = has_group_by(select) || has_aggregate_projection(select);
        let mut output = if needs_group {
            self.execute_grouped(select, &dataset, &window_values)?
        } else {
            let mut rows = Vec::new();
            for (idx, row) in dataset.rows.iter().enumerate() {
                let projected =
                    self.eval_projection(select, &dataset.columns, row, idx, &window_values)?;
                rows.push(projected);
            }
            DataSet {
                columns: projected_columns(select)?,
                rows,
            }
        };

        if select.distinct.is_some() {
            dedup_rows(&mut output);
        }

        if let Some(having) = &select.having {
            if needs_group {
                output.rows.retain(|row| {
                    self.eval_expr(having, &output.columns, row)
                        .as_bool()
                        .unwrap_or(false)
                });
            }
        }

        if !query.order_by.is_empty() {
            apply_order_by(&mut output, &query.order_by, self)?;
        }

        Ok(output)
    }

    fn execute_grouped(
        &mut self,
        select: &Select,
        dataset: &DataSet,
        window_values: &HashMap<String, Vec<Value>>,
    ) -> anyhow::Result<DataSet> {
        let mut groups: HashMap<Vec<ValueKey>, Vec<Vec<Value>>> = HashMap::new();
        let group_exprs = group_by_exprs(&select.group_by);
        for row in &dataset.rows {
            let key = group_exprs
                .iter()
                .map(|expr| ValueKey::from_value(&self.eval_expr(expr, &dataset.columns, row)))
                .collect::<Vec<_>>();
            groups.entry(key).or_default().push(row.clone());
        }

        let columns = projected_columns(select)?;
        let mut rows = Vec::new();
        for group_rows in groups.values() {
            let base_row = group_rows.first().cloned().unwrap_or_default();
            let projected = self.eval_group_projection(select, dataset, &base_row, group_rows, window_values)?;
            rows.push(projected);
        }

        Ok(DataSet { columns, rows })
    }

    fn build_from(&mut self, select: &Select) -> anyhow::Result<DataSet> {
        if select.from.is_empty() {
            return Ok(DataSet {
                columns: Vec::new(),
                rows: vec![Vec::new()],
            });
        }
        let mut dataset = self.build_relation_factor(&select.from[0].relation)?;
        for join in &select.from[0].joins {
            let right = self.build_relation_factor(&join.relation)?;
            dataset = apply_join(dataset, right, &join.join_operator, self)?;
        }
        Ok(dataset)
    }

    fn build_relation_factor(
        &mut self,
        relation: &fp_sql::ast::TableFactor,
    ) -> anyhow::Result<DataSet> {
        match relation {
            fp_sql::ast::TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string().to_ascii_lowercase();
                if let Some(view) = self.views.get(&table_name).cloned() {
                    let query = view.query;
                    let (mut dataset, _) = self.execute_query(&query, None, None)?;
                    if let Some(alias) = alias {
                        for col in &mut dataset.columns {
                            col.qualifier = Some(alias.name.value.to_ascii_lowercase());
                        }
                    }
                    return Ok(dataset);
                }
                let Some(table) = self.tables.get(&table_name) else {
                    return Ok(DataSet::empty());
                };
                let qualifier = alias
                    .as_ref()
                    .map(|a| a.name.value.to_ascii_lowercase())
                    .or_else(|| Some(table_name));
                let columns = table
                    .columns
                    .iter()
                    .map(|c| ColumnRef {
                        name: c.name.clone(),
                        qualifier: qualifier.clone(),
                    })
                    .collect();
                Ok(DataSet {
                    columns,
                    rows: table.rows.clone(),
                })
            }
            fp_sql::ast::TableFactor::Derived { subquery, alias, .. } => {
                let (mut dataset, _) = self.execute_query(subquery, None, None)?;
                if let Some(alias) = alias {
                    for col in &mut dataset.columns {
                        col.qualifier = Some(alias.name.value.to_ascii_lowercase());
                    }
                }
                Ok(dataset)
            }
            _ => Ok(DataSet::empty()),
        }
    }

    fn eval_projection(
        &mut self,
        select: &Select,
        columns: &[ColumnRef],
        row: &[Value],
        row_idx: usize,
        window_values: &HashMap<String, Vec<Value>>,
    ) -> anyhow::Result<Vec<Value>> {
        let mut out = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if is_array_join(expr) {
                        out.push(lookup_column(
                            columns,
                            row,
                            &expr.to_string(),
                            None,
                        ));
                    } else {
                        out.push(self.eval_expr_with_window(
                            expr,
                            columns,
                            row,
                            row_idx,
                            window_values,
                        ));
                    }
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    if is_array_join(expr) {
                        out.push(lookup_column(
                            columns,
                            row,
                            &expr.to_string(),
                            None,
                        ));
                    } else {
                        out.push(self.eval_expr_with_window(
                            expr,
                            columns,
                            row,
                            row_idx,
                            window_values,
                        ));
                    }
                }
                SelectItem::Wildcard(_) => {
                    out.extend(row.iter().cloned());
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    let qualifier = object_name.to_string().to_ascii_lowercase();
                    for (idx, col) in columns.iter().enumerate() {
                        if col
                            .qualifier
                            .as_ref()
                            .map(|q| q == &qualifier)
                            .unwrap_or(false)
                        {
                            out.push(row.get(idx).cloned().unwrap_or(Value::Null));
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn eval_group_projection(
        &mut self,
        select: &Select,
        dataset: &DataSet,
        base_row: &[Value],
        group_rows: &[Vec<Value>],
        window_values: &HashMap<String, Vec<Value>>,
    ) -> anyhow::Result<Vec<Value>> {
        let mut out = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    out.push(self.eval_group_expr(expr, dataset, base_row, group_rows, window_values));
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    out.push(self.eval_group_expr(expr, dataset, base_row, group_rows, window_values));
                }
                SelectItem::Wildcard(_) => {
                    out.extend(base_row.iter().cloned());
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    let qualifier = object_name.to_string().to_ascii_lowercase();
                    for (idx, col) in dataset.columns.iter().enumerate() {
                        if col
                            .qualifier
                            .as_ref()
                            .map(|q| q == &qualifier)
                            .unwrap_or(false)
                        {
                            out.push(base_row.get(idx).cloned().unwrap_or(Value::Null));
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn eval_group_expr(
        &mut self,
        expr: &Expr,
        dataset: &DataSet,
        base_row: &[Value],
        group_rows: &[Vec<Value>],
        window_values: &HashMap<String, Vec<Value>>,
    ) -> Value {
        match expr {
            Expr::Function(func) if func.over.is_none() => {
                if is_aggregate_function(func) {
                    return eval_aggregate(func, dataset, group_rows, self);
                }
                self.eval_expr_with_window(expr, &dataset.columns, base_row, 0, window_values)
            }
            _ => self.eval_expr_with_window(expr, &dataset.columns, base_row, 0, window_values),
        }
    }

    fn eval_expr_with_window(
        &mut self,
        expr: &Expr,
        columns: &[ColumnRef],
        row: &[Value],
        row_idx: usize,
        window_values: &HashMap<String, Vec<Value>>,
    ) -> Value {
        if let Expr::Function(func) = expr {
            if func.over.is_some() {
                let key = expr.to_string();
                if let Some(values) = window_values.get(&key) {
                    if let Some(value) = values.get(row_idx) {
                        return value.clone();
                    }
                }
            }
        }
        self.eval_expr(expr, columns, row)
    }

    fn eval_expr(&mut self, expr: &Expr, columns: &[ColumnRef], row: &[Value]) -> Value {
        match expr {
            Expr::Identifier(ident) => lookup_column(columns, row, &ident.value, None),
            Expr::CompoundIdentifier(idents) => {
                if idents.len() == 2 {
                    lookup_column(
                        columns,
                        row,
                        &idents[1].value,
                        Some(&idents[0].value),
                    )
                } else {
                    Value::Null
                }
            }
            Expr::Value(value) => parse_value(value),
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr(left, columns, row);
                let right_val = self.eval_expr(right, columns, row);
                eval_binary(op, left_val, right_val)
            }
            Expr::UnaryOp { op, expr } => {
                let value = self.eval_expr(expr, columns, row);
                eval_unary(op, value)
            }
            Expr::Nested(expr) => self.eval_expr(expr, columns, row),
            Expr::Tuple(exprs) => {
                let values = exprs
                    .iter()
                    .map(|e| self.eval_expr(e, columns, row))
                    .collect();
                Value::Tuple(values)
            }
            Expr::Array(array) => {
                let values = array
                    .elem
                    .iter()
                    .map(|e| self.eval_expr(e, columns, row))
                    .collect();
                Value::Array(values)
            }
            Expr::Function(func) => eval_function(func, columns, row, self),
            Expr::Cast { expr, data_type, .. } => {
                let value = self.eval_expr(expr, columns, row);
                cast_value(value, data_type)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let value = self.eval_expr(expr, columns, row);
                let set = list
                    .iter()
                    .map(|e| ValueKey::from_value(&self.eval_expr(e, columns, row)))
                    .collect::<HashSet<_>>();
                let exists = set.contains(&ValueKey::from_value(&value));
                Value::Bool(if *negated { !exists } else { exists })
            }
            Expr::InSubquery { expr, subquery, negated } => {
                let value = self.eval_expr(expr, columns, row);
                let (dataset, _) = match self.execute_query(subquery, None, None) {
                    Ok(result) => result,
                    Err(_) => return Value::Bool(false),
                };
                let mut set = HashSet::new();
                for row in dataset.rows {
                    if let Some(first) = row.get(0) {
                        set.insert(ValueKey::from_value(first));
                    }
                }
                let exists = set.contains(&ValueKey::from_value(&value));
                Value::Bool(if *negated { !exists } else { exists })
            }
            Expr::Interval(interval) => parse_interval(interval, columns, row, self),
            _ => Value::Null,
        }
    }

}

fn is_ttl_expired(
    expr: &Expr,
    columns: &[ColumnRef],
    row: &[Value],
    engine: &mut HotSqlEngine,
) -> bool {
    let expiry = engine.eval_expr(expr, columns, row);
    match expiry {
        Value::DateTime(dt) => dt.to_chrono() < Utc::now(),
        Value::DateTime64(dt) => dt.to_chrono() < Utc::now(),
        _ => false,
    }
}

impl Table {
    fn columns_as_refs(&self) -> Vec<ColumnRef> {
        self.columns
            .iter()
            .map(|c| ColumnRef {
                name: c.name.clone(),
                qualifier: Some(self.name.clone()),
            })
            .collect()
    }
}

fn column_refs_from_defs(columns: &[ColumnDefn]) -> Vec<ColumnRef> {
    columns
        .iter()
        .map(|c| ColumnRef {
            name: c.name.clone(),
            qualifier: None,
        })
        .collect()
}

fn apply_rows_to_table(table: &mut Table, rows: Vec<Vec<Value>>, engine: &mut HotSqlEngine) {
    let column_refs = column_refs_from_defs(&table.columns);
    for mut row in rows {
        if row.len() < table.columns.len() {
            row.resize(table.columns.len(), Value::Null);
        }
        for (idx, col) in table.columns.iter().enumerate() {
            if let Some(expr) = &col.materialized {
                row[idx] = engine.eval_expr(expr, &column_refs, &row);
            } else if matches!(row[idx], Value::Null) {
                if let Some(expr) = &col.default {
                    row[idx] = engine.eval_expr(expr, &column_refs, &row);
                }
            }
        }
        table.rows.push(row);
    }
}

fn parse_statement(sql: &str) -> anyhow::Result<Statement> {
    let mut statements = Parser::parse_sql(&ClickHouseDialect {}, sql)
        .map_err(|err| anyhow!("failed to parse SQL: {err}"))?;
    if statements.is_empty() {
        return Err(anyhow!("empty SQL"));
    }
    Ok(statements.remove(0))
}

fn split_format_clause(sql: &str) -> (String, Option<String>) {
    let mut tokenizer = Tokenizer::new(&ClickHouseDialect {}, sql);
    let tokens: Vec<Token> = match tokenizer.tokenize() {
        Ok(tokens) => tokens,
        Err(_) => return (sql.trim().to_string(), None),
    };
    let mut depth = 0i32;
    let mut format_idx = None;
    let mut format_token = None;
    let mut idx = 0usize;
    while idx < tokens.len() {
        match &tokens[idx] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(word) if depth == 0 => {
                if word.value.eq_ignore_ascii_case("format") {
                    let next = next_non_ws(&tokens, idx + 1);
                    if let Some(next_idx) = next {
                        if let Token::Word(format_word) = &tokens[next_idx] {
                            format_idx = Some(idx);
                            format_token = Some(format_word.value.clone());
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
        idx += 1;
    }

    if let Some(start) = format_idx {
        let mut end = start + 1;
        while end < tokens.len() {
            if let Token::SemiColon = tokens[end] {
                break;
            }
            end += 1;
        }
        let mut trimmed = Vec::new();
        for (idx, token) in tokens.iter().enumerate() {
            if idx >= start && idx < end {
                continue;
            }
            trimmed.push(token.to_string());
        }
        return (trimmed.join(" "), format_token);
    }

    (sql.trim().to_string(), None)
}

fn rewrite_insert_columns_with_dots(sql: &str) -> String {
    let mut tokenizer = Tokenizer::new(&ClickHouseDialect {}, sql);
    let tokens: Vec<Token> = match tokenizer.tokenize() {
        Ok(tokens) => tokens,
        Err(_) => return rewrite_insert_columns_with_dots_fallback(sql),
    };

    let mut output = Vec::new();
    let mut idx = 0usize;
    let mut in_insert_columns = false;
    let mut paren_depth = 0i32;

    while idx < tokens.len() {
        let token = &tokens[idx];
        if is_insert_start(&tokens, idx) {
            in_insert_columns = true;
        }

        if in_insert_columns {
            match token {
                Token::LParen => {
                    paren_depth += 1;
                    output.push(token.clone());
                    idx += 1;
                    continue;
                }
                Token::RParen => {
                    paren_depth -= 1;
                    output.push(token.clone());
                    if paren_depth == 0 {
                        in_insert_columns = false;
                    }
                    idx += 1;
                    continue;
                }
                Token::Word(word) if paren_depth == 1 => {
                    let next_idx = next_non_ws(&tokens, idx + 1);
                    let last_idx = next_idx.and_then(|i| next_non_ws(&tokens, i + 1));
                    if let (Some(dot_idx), Some(name_idx)) = (next_idx, last_idx) {
                        if matches!(tokens[dot_idx], Token::Period) {
                            if let Token::Word(next_word) = &tokens[name_idx] {
                                let combined = format!("{}.{}", word.value, next_word.value);
                                output.push(Token::Word(Word {
                                    value: combined,
                                    quote_style: Some('"'),
                                    keyword: Keyword::NoKeyword,
                                }));
                                idx = name_idx + 1;
                                continue;
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        output.push(token.clone());
        idx += 1;
    }

    output
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn rewrite_insert_columns_with_dots_fallback(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let insert_idx = match lower.find("insert") {
        Some(idx) => idx,
        None => return sql.to_string(),
    };
    let into_idx = match lower[insert_idx..].find("into") {
        Some(idx) => insert_idx + idx,
        None => return sql.to_string(),
    };
    let open_idx = match lower[into_idx..].find('(') {
        Some(idx) => into_idx + idx,
        None => return sql.to_string(),
    };

    let mut depth = 0i32;
    let mut close_idx = None;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    for (offset, ch) in sql[open_idx..].char_indices() {
        match ch {
            '\'' if !in_double && !in_backtick => in_single = !in_single,
            '"' if !in_single && !in_backtick => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            '(' if !(in_single || in_double || in_backtick) => {
                depth += 1;
            }
            ')' if !(in_single || in_double || in_backtick) => {
                depth -= 1;
                if depth == 0 {
                    close_idx = Some(open_idx + offset);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(close_idx) = close_idx else {
        return sql.to_string();
    };

    let cols = &sql[open_idx + 1..close_idx];
    let mut rewritten_cols = Vec::new();
    for part in cols.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let rewritten = if trimmed.starts_with('"')
            || trimmed.starts_with('`')
            || trimmed.starts_with('\'')
            || !trimmed.contains('.')
        {
            trimmed.to_string()
        } else {
            format!("\"{}\"", trimmed)
        };
        rewritten_cols.push(rewritten);
    }

    let mut out = String::new();
    out.push_str(&sql[..open_idx + 1]);
    out.push_str(&rewritten_cols.join(", "));
    out.push_str(&sql[close_idx..]);
    out
}

fn split_sample_clause(sql: &str) -> (String, Option<f64>) {
    let mut tokenizer = Tokenizer::new(&ClickHouseDialect {}, sql);
    let tokens: Vec<Token> = match tokenizer.tokenize() {
        Ok(tokens) => tokens,
        Err(_) => return (sql.trim().to_string(), None),
    };
    let mut depth = 0i32;
    let mut sample_start = None;
    let mut sample_end = None;
    let mut ratio = None;

    let mut idx = 0usize;
    while idx < tokens.len() {
        match &tokens[idx] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(word) if depth == 0 => {
                if word.value.eq_ignore_ascii_case("sample") {
                    sample_start = Some(idx);
                    let mut cursor = idx + 1;
                    while cursor < tokens.len() {
                        if let Token::Word(next_word) = &tokens[cursor] {
                            if is_select_clause_start(next_word) {
                                break;
                            }
                        }
                        if let Token::Number(num, _) = &tokens[cursor] {
                            ratio = parse_sample_ratio(num);
                            sample_end = Some(cursor + 1);
                        }
                        cursor += 1;
                    }
                    sample_end = sample_end.or(Some(cursor));
                    break;
                }
            }
            _ => {}
        }
        idx += 1;
    }

    if let (Some(start), Some(end)) = (sample_start, sample_end) {
        let mut trimmed = Vec::new();
        for (idx, token) in tokens.iter().enumerate() {
            if idx >= start && idx < end {
                continue;
            }
            trimmed.push(token.to_string());
        }
        return (trimmed.join(" "), ratio);
    }

    (sql.trim().to_string(), None)
}

fn parse_format_hint(format: Option<&str>) -> Option<FormatHint> {
    let format = format?.trim();
    if format.eq_ignore_ascii_case("tsvwithnames") {
        Some(FormatHint::TsvWithNames)
    } else if format.eq_ignore_ascii_case("tabseparated") || format.eq_ignore_ascii_case("tsv") {
        Some(FormatHint::TabSeparated)
    } else if format.eq_ignore_ascii_case("jsoneachrow") {
        Some(FormatHint::JsonEachRow)
    } else {
        None
    }
}

fn parse_create_table(sql: &str) -> anyhow::Result<CreateTableInfo> {
    let mut tokenizer = Tokenizer::new(&ClickHouseDialect {}, sql);
    let tokens: Vec<Token> = tokenizer
        .tokenize()
        .map_err(|err: sqlparser::tokenizer::TokenizerError| anyhow!(err.to_string()))?;
    let clauses = extract_create_table_clauses(&tokens)?;

    let (table_name, column_spans) = parse_create_table_span(&tokens)?;
    if column_spans.is_empty() && clauses.as_table.is_none() {
        return Err(anyhow!("column list missing"));
    }
    let mut column_defs = Vec::new();
    for span in column_spans {
        let (name, data_type, default, materialized) =
            parse_column_defn_fallback(&tokens[span.0..span.1])?;
        let materialized = materialized
            .or_else(|| clauses.materialized_exprs.get(&name).cloned());
        column_defs.push(ColumnDefn {
            name,
            data_type,
            default: if materialized.is_some() { None } else { default },
            materialized,
        });
    }
    let name = ObjectName(vec![Ident::new(table_name)]);
    let engine = None;
    let query = None;

    let mut expanded = Vec::new();
    for column in column_defs {
        match &column.data_type {
            DataType::Nested(fields) => {
                for (field_name, field_type) in fields {
                    expanded.push(ColumnDefn {
                        name: format!("{}.{}", column.name, field_name),
                        data_type: DataType::Array(Box::new(field_type.clone())),
                        default: None,
                        materialized: None,
                    });
                }
            }
            _ => expanded.push(column),
        }
    }

    Ok(CreateTableInfo {
        name: name.to_string().to_ascii_lowercase(),
        columns: expanded,
        engine,
        as_table: clauses.as_table,
        query,
        order_by: None,
        ttl: clauses.ttl,
        sample_by: clauses.sample_by,
    })
}

fn parse_create_table_span(tokens: &[Token]) -> anyhow::Result<(String, Vec<(usize, usize)>)> {
    let mut idx = 0usize;
    let mut table_name = None;
    let mut name_end = None;
    while idx < tokens.len() {
        if let Token::Word(word) = &tokens[idx] {
            if word.value.eq_ignore_ascii_case("table") {
                let mut name_parts = Vec::new();
                let mut cursor = idx + 1;
                while cursor < tokens.len() {
                    match &tokens[cursor] {
                        Token::Word(word) => name_parts.push(word.value.clone()),
                        Token::Period => name_parts.push(".".to_string()),
                        Token::Whitespace(_) => {}
                        Token::LParen => break,
                        _ => break,
                    }
                    cursor += 1;
                }
                if !name_parts.is_empty() {
                    table_name = Some(name_parts.join(""));
                    name_end = Some(cursor);
                }
            }
        }
        if table_name.is_some() {
            break;
        }
        idx += 1;
    }
    let table_name = table_name.ok_or_else(|| anyhow!("missing table name"))?;
    let name_end = name_end.unwrap_or(idx);
    let open_idx = next_non_ws(tokens, name_end)
        .and_then(|idx| if matches!(tokens[idx], Token::LParen) { Some(idx) } else { None });
    let column_spans = if let Some(open_idx) = open_idx {
        let close_idx = find_matching_paren(tokens, open_idx)?;
        split_column_definitions_in_span(tokens, open_idx + 1, close_idx)
    } else {
        Vec::new()
    };
    Ok((table_name.to_ascii_lowercase(), column_spans))
}

fn split_column_definitions_in_span(
    tokens: &[Token],
    start: usize,
    end: usize,
) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut depth = 0i32;
    let mut col_start = start;
    let mut idx = start;
    while idx < end {
        match &tokens[idx] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Comma if depth == 0 => {
                spans.push((col_start, idx));
                col_start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }
    if col_start < end {
        spans.push((col_start, end));
    }
    spans
}

fn parse_column_defn_fallback(tokens: &[Token]) -> anyhow::Result<(String, DataType, Option<Expr>, Option<Expr>)> {
    let mut idx = 0usize;
    let mut name = None;
    while idx < tokens.len() {
        if let Token::Word(word) = &tokens[idx] {
            name = Some(word.value.to_ascii_lowercase());
            idx += 1;
            break;
        }
        idx += 1;
    }
    let name = name.ok_or_else(|| anyhow!("column name missing"))?;

    let mut type_tokens = Vec::new();
    let mut default_expr = None;
    let mut materialized_expr = None;
    let mut depth = 0i32;
    while idx < tokens.len() {
        match &tokens[idx] {
            Token::LParen => {
                depth += 1;
                type_tokens.push(tokens[idx].to_string());
            }
            Token::RParen => {
                depth -= 1;
                type_tokens.push(tokens[idx].to_string());
            }
            Token::Word(word) if depth == 0 && word.value.eq_ignore_ascii_case("default") => {
                let expr_sql = tokens[idx + 1..]
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                if !expr_sql.trim().is_empty() {
                    default_expr = parse_expr_from_sql(expr_sql.trim()).ok();
                }
                break;
            }
            Token::Word(word) if depth == 0 && word.value.eq_ignore_ascii_case("materialized") => {
                let expr_sql = tokens[idx + 1..]
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                if !expr_sql.trim().is_empty() {
                    materialized_expr = parse_expr_from_sql(expr_sql.trim()).ok();
                }
                break;
            }
            Token::Whitespace(_) => {}
            _ => type_tokens.push(tokens[idx].to_string()),
        }
        idx += 1;
    }

    let ty_sql = type_tokens.join(" ").trim().to_string();
    let data_type = parse_type_string(&ty_sql);
    Ok((name, data_type, default_expr, materialized_expr))
}

#[derive(Debug, Clone)]
struct CreateTableClauses {
    strip_spans: Vec<(usize, usize)>,
    engine_args_span: Option<(usize, usize)>,
    as_table_span: Option<(usize, usize)>,
    as_table: Option<String>,
    ttl: Option<Expr>,
    sample_by: Option<Expr>,
    materialized_spans: Vec<usize>,
    materialized_exprs: HashMap<String, Expr>,
}

fn extract_create_table_clauses(tokens: &[Token]) -> anyhow::Result<CreateTableClauses> {
    let mut depth = 0i32;
    let mut strip_spans = Vec::new();
    let mut engine_args_span = None;
    let mut as_table_span = None;
    let mut as_table = None;
    let mut ttl = None;
    let mut sample_by = None;
    let mut materialized_spans = Vec::new();
    let mut materialized_exprs = HashMap::new();

    let mut idx = 0usize;
    while idx < tokens.len() {
        match &tokens[idx] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(word) if depth == 0 => {
                if word.value.eq_ignore_ascii_case("engine") {
                    if let Some(eq_idx) = next_non_ws(tokens, idx + 1) {
                        if matches!(tokens[eq_idx], Token::Eq) {
                            if let Some(name_idx) = next_non_ws(tokens, eq_idx + 1) {
                                if let Some(arg_start) = next_non_ws(tokens, name_idx + 1) {
                                    if matches!(tokens[arg_start], Token::LParen) {
                                        let end = find_matching_paren(tokens, arg_start)?;
                                        engine_args_span = Some((arg_start, end + 1));
                                    }
                                }
                            }
                        }
                    }
                }
                if word.value.eq_ignore_ascii_case("partition") {
                    if is_keyword_sequence(tokens, idx, &["partition", "by"]) {
                        if let Some((start, end)) = capture_clause(tokens, idx)? {
                            strip_spans.push((start, end));
                        }
                    }
                }
                if word.value.eq_ignore_ascii_case("primary") {
                    if is_keyword_sequence(tokens, idx, &["primary", "key"]) {
                        if let Some((start, end)) = capture_clause(tokens, idx)? {
                            strip_spans.push((start, end));
                        }
                    }
                }
                if word.value.eq_ignore_ascii_case("sample") {
                    if is_keyword_sequence(tokens, idx, &["sample", "by"]) {
                        if let Some((start, end)) = capture_clause(tokens, idx)? {
                            let expr_sql = tokens[start + 2..end]
                                .iter()
                                .map(|t| t.to_string())
                                .collect::<Vec<_>>()
                                .join(" ");
                            sample_by = parse_expr_from_sql(&expr_sql).ok();
                            strip_spans.push((start, end));
                        }
                    }
                }
                if word.value.eq_ignore_ascii_case("ttl") {
                    if let Some((start, end)) = capture_clause(tokens, idx)? {
                        let expr_sql = tokens[start + 1..end]
                            .iter()
                            .map(|t| t.to_string())
                            .collect::<Vec<_>>()
                            .join(" ");
                        ttl = parse_expr_from_sql(&expr_sql).ok();
                        strip_spans.push((start, end));
                    }
                }
                if word.value.eq_ignore_ascii_case("settings") {
                    if let Some((start, end)) = capture_clause(tokens, idx)? {
                        strip_spans.push((start, end));
                    }
                }
                if word.value.eq_ignore_ascii_case("as") {
                    if let Some(next_idx) = next_non_ws(tokens, idx + 1) {
                        if let Token::Word(next_word) = &tokens[next_idx] {
                            if !next_word.value.eq_ignore_ascii_case("select") {
                                let end = next_non_ws(tokens, next_idx + 1).unwrap_or(tokens.len());
                                as_table_span = Some((idx, end));
                                as_table = Some(next_word.value.to_ascii_lowercase());
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        idx += 1;
    }

    let column_spans = split_column_definitions(tokens)?;
    for span in column_spans {
        let col_tokens = &tokens[span.0..span.1];
        if let Some((name, materialized_idx, expr_span)) = find_materialized_in_column(col_tokens)? {
            let expr_tokens = &col_tokens[expr_span.0..expr_span.1];
            let expr_sql = expr_tokens
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            if let Ok(expr) = parse_expr_from_sql(&expr_sql) {
                materialized_exprs.insert(name.clone(), expr);
            }
            materialized_spans.push(span.0 + materialized_idx);
        }
    }

    Ok(CreateTableClauses {
        strip_spans,
        engine_args_span,
        as_table_span,
        as_table,
        ttl,
        sample_by,
        materialized_spans,
        materialized_exprs,
    })
}

fn split_column_definitions(tokens: &[Token]) -> anyhow::Result<Vec<(usize, usize)>> {
    let mut result = Vec::new();
    let mut depth = 0i32;
    let mut start = None;
    let mut idx = 0usize;
    while idx < tokens.len() {
        match &tokens[idx] {
            Token::LParen => {
                depth += 1;
                if depth == 1 {
                    start = Some(idx + 1);
                }
            }
            Token::RParen => {
                if depth == 1 {
                    if let Some(start_idx) = start.take() {
                        result.push((start_idx, idx));
                    }
                }
                depth -= 1;
            }
            Token::Comma if depth == 1 => {
                if let Some(start_idx) = start {
                    result.push((start_idx, idx));
                    start = Some(idx + 1);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    Ok(result)
}

fn find_materialized_in_column(
    tokens: &[Token],
) -> anyhow::Result<Option<(String, usize, (usize, usize))>> {
    let mut name = None;
    let mut idx = 0usize;
    while idx < tokens.len() {
        if let Token::Word(word) = &tokens[idx] {
            if name.is_none() {
                name = Some(word.value.to_ascii_lowercase());
            }
            if word.value.eq_ignore_ascii_case("materialized") {
                let expr_start = next_non_ws(tokens, idx + 1).unwrap_or(tokens.len());
                let expr_end = tokens.len();
                if let Some(name) = name {
                    return Ok(Some((name, idx, (expr_start, expr_end))));
                }
            }
        }
        idx += 1;
    }
    Ok(None)
}

fn capture_clause(tokens: &[Token], start_idx: usize) -> anyhow::Result<Option<(usize, usize)>> {
    let mut depth = 0i32;
    let mut idx = start_idx;
    while idx < tokens.len() {
        match &tokens[idx] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(word) if depth == 0 => {
                if idx > start_idx && is_clause_start(word) {
                    return Ok(Some((start_idx, idx)));
                }
            }
            _ => {}
        }
        idx += 1;
    }
    Ok(Some((start_idx, tokens.len())))
}

fn is_clause_start(word: &Word) -> bool {
    matches!(
        word.value.to_ascii_lowercase().as_str(),
        "partition" | "primary" | "sample" | "ttl" | "settings" | "order" | "as"
    )
}

fn is_keyword_sequence(tokens: &[Token], start: usize, seq: &[&str]) -> bool {
    let mut idx = start;
    for keyword in seq {
        let Some(next_idx) = next_non_ws(tokens, idx) else {
            return false;
        };
        match &tokens[next_idx] {
            Token::Word(word) if word.value.eq_ignore_ascii_case(keyword) => {
                idx = next_idx + 1;
            }
            _ => return false,
        }
    }
    true
}

fn next_non_ws(tokens: &[Token], start: usize) -> Option<usize> {
    let mut idx = start;
    while idx < tokens.len() {
        match tokens[idx] {
            Token::Whitespace(_) => idx += 1,
            _ => return Some(idx),
        }
    }
    None
}

fn find_matching_paren(tokens: &[Token], start: usize) -> anyhow::Result<usize> {
    let mut depth = 0i32;
    for (idx, token) in tokens.iter().enumerate().skip(start) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Ok(idx);
                }
            }
            _ => {}
        }
    }
    Err(anyhow!("unbalanced parentheses"))
}

fn is_select_clause_start(word: &Word) -> bool {
    matches!(
        word.value.to_ascii_lowercase().as_str(),
        "where" | "group" | "order" | "limit" | "format" | "settings" | "union"
    )
}

fn parse_sample_ratio(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    if let Some(value) = trimmed.strip_suffix('%') {
        return value.parse::<f64>().ok().map(|v| v / 100.0);
    }
    trimmed.parse::<f64>().ok()
}

fn rewrite_array_types(tokens: &mut [Token]) -> anyhow::Result<()> {
    let mut idx = 0usize;
    while idx < tokens.len() {
        if let Token::Word(word) = &tokens[idx] {
            if word.value.eq_ignore_ascii_case("array") {
                if let Some(open_idx) = next_non_ws(tokens, idx + 1) {
                    if matches!(tokens[open_idx], Token::LParen) {
                        let close_idx = find_matching_paren(tokens, open_idx)?;
                        tokens[idx] = Token::Word(Word {
                            value: "ARRAY".to_string(),
                            quote_style: None,
                            keyword: Keyword::ARRAY,
                        });
                        tokens[open_idx] = Token::Lt;
                        tokens[close_idx] = Token::Gt;
                        idx = close_idx + 1;
                        continue;
                    }
                }
            }
        }
        idx += 1;
    }
    Ok(())
}

fn rewrite_enum_equals(tokens: &mut [Token]) -> anyhow::Result<()> {
    let mut idx = 0usize;
    while idx < tokens.len() {
        if let Token::Word(word) = &tokens[idx] {
            if word.value.eq_ignore_ascii_case("enum8")
                || word.value.eq_ignore_ascii_case("enum16")
            {
                if let Some(open_idx) = next_non_ws(tokens, idx + 1) {
                    if matches!(tokens[open_idx], Token::LParen) {
                        let close_idx = find_matching_paren(tokens, open_idx)?;
                        for pos in open_idx + 1..close_idx {
                            if matches!(tokens[pos], Token::Eq) {
                                tokens[pos] = Token::Whitespace(Whitespace::Space);
                            }
                        }
                        idx = close_idx + 1;
                        continue;
                    }
                }
            }
        }
        idx += 1;
    }
    Ok(())
}

fn is_insert_start(tokens: &[Token], idx: usize) -> bool {
    match tokens.get(idx) {
        Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("insert") => {
            if let Some(Token::Word(next)) = next_non_ws(tokens, idx + 1).and_then(|i| tokens.get(i)) {
                next.value.eq_ignore_ascii_case("into")
            } else {
                false
            }
        }
        _ => false,
    }
}

#[derive(Debug, Clone)]
struct Mutation {
    table: String,
    kind: MutationKind,
}

#[derive(Debug, Clone)]
enum MutationKind {
    Update {
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
    },
    Delete {
        selection: Option<Expr>,
    },
}

fn parse_alter_mutation(sql: &str) -> anyhow::Result<Option<Mutation>> {
    let lower = sql.trim_start().to_ascii_lowercase();
    if !lower.starts_with("alter table") {
        return Ok(None);
    }
    let tokens = lower.split_whitespace().collect::<Vec<_>>();
    if tokens.iter().any(|t| *t == "update") {
        let rewritten = rewrite_alter_to_update(sql);
        let stmt = parse_statement(&rewritten)?;
        if let Statement::Update { table, assignments, selection, .. } = stmt {
            let table_name = table.to_string().to_ascii_lowercase();
            let assigns = assignments
                .into_iter()
                .map(|a| {
                    let name = a
                        .id
                        .into_iter()
                        .map(|ident| ident.value.to_ascii_lowercase())
                        .collect::<Vec<_>>()
                        .join(".");
                    (name, a.value)
                })
                .collect();
            return Ok(Some(Mutation {
                table: table_name,
                kind: MutationKind::Update { assignments: assigns, selection },
            }));
        }
    }
    if tokens.iter().any(|t| *t == "delete") {
        let rewritten = rewrite_alter_to_delete(sql);
        let stmt = parse_statement(&rewritten)?;
        if let Statement::Delete { from, selection, .. } = stmt {
            if let Some(first) = from.get(0) {
                if let fp_sql::ast::TableFactor::Table { name, .. } = &first.relation {
                    return Ok(Some(Mutation {
                        table: name.to_string().to_ascii_lowercase(),
                        kind: MutationKind::Delete { selection },
                    }));
                }
            }
        }
    }
    Ok(None)
}

fn rewrite_alter_to_update(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if let Some(idx) = lower.find("alter table") {
        if let Some(update_idx) = lower.find(" update") {
            let table = sql[idx + "alter table".len()..update_idx].trim();
            let tail = &sql[update_idx + 1..];
            let rest = tail.trim_start_matches("update");
            return format!("UPDATE {table} SET {rest}");
        }
    }
    sql.to_string()
}

fn rewrite_alter_to_delete(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if let Some(idx) = lower.find("alter table") {
        if let Some(delete_idx) = lower.find(" delete") {
            let table = sql[idx + "alter table".len()..delete_idx].trim();
            let tail = &sql[delete_idx + 1..];
            let rest = tail.trim_start_matches("delete");
            return format!("DELETE FROM {table} {rest}");
        }
    }
    sql.to_string()
}

fn parse_expr_from_sql(expr_sql: &str) -> anyhow::Result<Expr> {
    let query = format!("SELECT {expr_sql}");
    let mut stmts = Parser::parse_sql(&ClickHouseDialect {}, &query)
        .map_err(|err| anyhow!("failed to parse expr: {err}"))?;
    let stmt = stmts.remove(0);
    if let Statement::Query(query) = stmt {
        if let SetExpr::Select(select) = *query.body {
            if let Some(item) = select.projection.first() {
                if let SelectItem::UnnamedExpr(expr) = item {
                    return Ok(expr.clone());
                }
            }
        }
    }
    Err(anyhow!("failed to parse expression"))
}

fn parse_insert_values_fallback(
    sql: &str,
    engine: &mut HotSqlEngine,
) -> anyhow::Result<Vec<Vec<Value>>> {
    let lower = sql.to_ascii_lowercase();
    let mut values_idx = None;
    let mut scan = 0usize;
    while scan < lower.len() {
        if lower[scan..].starts_with("values") {
            let preceded_by_ws = if scan == 0 {
                true
            } else {
                lower[..scan]
                    .chars()
                    .last()
                    .map(|ch| ch.is_whitespace())
                    .unwrap_or(false)
            };
            if preceded_by_ws {
                values_idx = Some(scan);
                break;
            }
        }
        scan += 1;
    }
    let values_idx = values_idx.ok_or_else(|| anyhow!("missing VALUES"))?;
    let mut tuples = Vec::new();
    let tail = &sql[values_idx + "values".len()..];
    let mut depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut start = None;
    for (offset, ch) in tail.char_indices() {
        match ch {
            '\'' if !in_double && !in_backtick => in_single = !in_single,
            '"' if !in_single && !in_backtick => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            '[' if !(in_single || in_double || in_backtick) => bracket_depth += 1,
            ']' if !(in_single || in_double || in_backtick) => bracket_depth -= 1,
            '(' if !(in_single || in_double || in_backtick) => {
                if depth == 0 {
                    start = Some(offset + 1);
                }
                depth += 1;
            }
            ')' if !(in_single || in_double || in_backtick) => {
                depth -= 1;
                if depth == 0 {
                    if let Some(start_idx) = start.take() {
                        tuples.push(tail[start_idx..offset].to_string());
                    }
                }
            }
            _ => {}
        }
        if depth == 0 && bracket_depth == 0 && start.is_none() && offset > 0 {
            if ch == ';' {
                break;
            }
        }
    }

    let mut rows = Vec::new();
    for tuple in tuples {
        let exprs = split_tuple_expressions(&tuple);
        let mut row = Vec::new();
        for expr_sql in exprs {
            row.push(parse_insert_expr(&expr_sql, engine)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

fn parse_insert_expr(expr_sql: &str, engine: &mut HotSqlEngine) -> anyhow::Result<Value> {
    let trimmed = expr_sql.trim();
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        let inner = &trimmed[1..trimmed.len().saturating_sub(1)];
        let values = split_tuple_expressions(inner)
            .into_iter()
            .map(|expr| parse_insert_expr(&expr, engine))
            .collect::<anyhow::Result<Vec<_>>>()?;
        return Ok(Value::Array(values));
    }
    if trimmed.starts_with('(') && trimmed.ends_with(')') {
        let inner = &trimmed[1..trimmed.len().saturating_sub(1)];
        let values = split_tuple_expressions(inner)
            .into_iter()
            .map(|expr| parse_insert_expr(&expr, engine))
            .collect::<anyhow::Result<Vec<_>>>()?;
        return Ok(Value::Tuple(values));
    }
    let expr = parse_expr_from_sql(trimmed)?;
    Ok(engine.eval_expr(&expr, &[], &[]))
}

fn split_tuple_expressions(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    for ch in input.chars() {
        match ch {
            '\'' if !in_double && !in_backtick => in_single = !in_single,
            '"' if !in_single && !in_backtick => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            '[' if !(in_single || in_double || in_backtick) => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' if !(in_single || in_double || in_backtick) => {
                bracket_depth -= 1;
                current.push(ch);
            }
            '(' if !(in_single || in_double || in_backtick) => {
                depth += 1;
                current.push(ch);
            }
            ')' if !(in_single || in_double || in_backtick) => {
                depth -= 1;
                current.push(ch);
            }
            ',' if !(in_single || in_double || in_backtick) && depth == 0 && bracket_depth == 0 => {
                if !current.trim().is_empty() {
                    parts.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn map_data_type(data_type: &AstDataType) -> DataType {
    match data_type {
        AstDataType::Boolean | AstDataType::Bool => DataType::Bool,
        AstDataType::Int64 | AstDataType::BigInt(_) | AstDataType::Int8(_) => DataType::Int64,
        AstDataType::UnsignedBigInt(_) | AstDataType::UnsignedInt8(_) => DataType::UInt64,
        AstDataType::Float64 | AstDataType::Double | AstDataType::DoublePrecision => {
            DataType::Float64
        }
        AstDataType::Float(_) | AstDataType::Real | AstDataType::Float4 | AstDataType::Float8 => {
            DataType::Float64
        }
        AstDataType::Decimal(info) | AstDataType::Numeric(info) | AstDataType::Dec(info) => {
            let (precision, scale) = match info {
                fp_sql::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
                fp_sql::ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                fp_sql::ast::ExactNumberInfo::None => (18, 4),
            };
            DataType::Decimal { precision, scale }
        }
        AstDataType::Date | AstDataType::Datetime(_) | AstDataType::Timestamp(_, _) => {
            DataType::DateTime
        }
        AstDataType::Uuid => DataType::Uuid,
        AstDataType::Array(elem) => {
            let inner = match elem {
                fp_sql::ast::ArrayElemTypeDef::SquareBracket(data) => map_data_type(data),
                fp_sql::ast::ArrayElemTypeDef::AngleBracket(data) => map_data_type(data),
                fp_sql::ast::ArrayElemTypeDef::None => DataType::String,
            };
            DataType::Array(Box::new(inner))
        }
        AstDataType::Custom(name, modifiers) => {
            let name = name.to_string().to_ascii_lowercase();
            parse_custom_type(&name, modifiers)
        }
        _ => DataType::String,
    }
}

fn parse_custom_type(name: &str, modifiers: &[String]) -> DataType {
    match name {
        "string" => DataType::String,
        "int8" | "int16" | "int32" | "int64" | "int128" => DataType::Int64,
        "uint8" | "uint16" | "uint32" | "uint64" | "uint128" => DataType::UInt64,
        "float32" | "float64" => DataType::Float64,
        "float" | "double" => DataType::Float64,
        "decimal" => {
            let precision = modifiers.get(0).and_then(|s| s.parse::<u8>().ok()).unwrap_or(18);
            let scale = modifiers.get(1).and_then(|s| s.parse::<u8>().ok()).unwrap_or(4);
            DataType::Decimal { precision, scale }
        }
        "datetime" => DataType::DateTime,
        "datetime64" => {
            let precision = modifiers
                .get(0)
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(6);
            DataType::DateTime64(precision)
        }
        "uuid" => DataType::Uuid,
        "ipv4" => DataType::Ipv4,
        "lowcardinality" => {
            let inner = modifiers
                .get(0)
                .map(|s| parse_type_string(s))
                .unwrap_or(DataType::String);
            DataType::LowCardinality(Box::new(inner))
        }
        "nullable" => {
            let inner = modifiers
                .get(0)
                .map(|s| parse_type_string(s))
                .unwrap_or(DataType::String);
            DataType::Nullable(Box::new(inner))
        }
        "array" => {
            let inner = modifiers
                .get(0)
                .map(|s| parse_type_string(s))
                .unwrap_or(DataType::String);
            DataType::Array(Box::new(inner))
        }
        "tuple" => {
            let inner = modifiers.iter().map(|s| parse_type_string(s)).collect();
            DataType::Tuple(inner)
        }
        "map" => {
            let key = modifiers
                .get(0)
                .map(|s| parse_type_string(s))
                .unwrap_or(DataType::String);
            let value = modifiers
                .get(1)
                .map(|s| parse_type_string(s))
                .unwrap_or(DataType::String);
            DataType::Map(Box::new(key), Box::new(value))
        }
        "enum8" => DataType::Enum8(
            parse_enum_variants(modifiers)
                .into_iter()
                .map(|(name, value)| (name, value as i8))
                .collect(),
        ),
        "enum16" => DataType::Enum16(parse_enum_variants(modifiers)),
        "aggregatefunction" => {
            let func = modifiers.get(0).cloned().unwrap_or_default();
            let args = modifiers
                .iter()
                .skip(1)
                .map(|s| parse_type_string(s))
                .collect();
            DataType::AggregateFunction(func, args)
        }
        "nested" => DataType::Nested(parse_nested_fields(modifiers)),
        _ => DataType::String,
    }
}

fn parse_nested_fields(modifiers: &[String]) -> Vec<(String, DataType)> {
    let mut fields = Vec::new();
    for modifier in modifiers {
        let mut parts = modifier.split_whitespace();
        let name = match parts.next() {
            Some(name) => name.to_ascii_lowercase(),
            None => continue,
        };
        let ty = parts.collect::<Vec<_>>().join(" ");
        let data_type = if ty.is_empty() {
            DataType::String
        } else {
            parse_type_string(&ty)
        };
        fields.push((name, data_type));
    }
    fields
}

fn parse_enum_variants(modifiers: &[String]) -> Vec<(String, i16)> {
    let mut variants = Vec::new();
    if modifiers.iter().any(|m| m.contains('=')) {
        for modifier in modifiers {
            let trimmed = modifier.trim();
            let Some((name, value)) = trimmed.split_once('=') else {
                continue;
            };
            let name = name.trim().trim_matches('\'').to_string();
            let value = value.trim().parse::<i16>().unwrap_or(0);
            variants.push((name, value));
        }
        return variants;
    }

    let mut iter = modifiers.iter();
    while let (Some(name), Some(value)) = (iter.next(), iter.next()) {
        let name = name.trim().trim_matches('\'').to_string();
        let value = value.trim().parse::<i16>().unwrap_or(0);
        variants.push((name, value));
    }
    variants
}

fn parse_type_string(input: &str) -> DataType {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return DataType::String;
    }
    let mut tokenizer = TypeTokenizer::new(trimmed);
    tokenizer.parse_type().unwrap_or(DataType::String)
}

struct TypeTokenizer<'a> {
    input: &'a str,
    chars: std::str::Chars<'a>,
    lookahead: Option<char>,
}

impl<'a> TypeTokenizer<'a> {
    fn new(input: &'a str) -> Self {
        let mut chars = input.chars();
        let lookahead = chars.next();
        Self {
            input,
            chars,
            lookahead,
        }
    }

    fn bump(&mut self) -> Option<char> {
        let current = self.lookahead;
        self.lookahead = self.chars.next();
        current
    }

    fn skip_ws(&mut self) {
        while matches!(self.lookahead, Some(ch) if ch.is_ascii_whitespace()) {
            self.bump();
        }
    }

    fn parse_type(&mut self) -> Option<DataType> {
        self.skip_ws();
        let ident = self.parse_ident()?;
        let lower = ident.to_ascii_lowercase();
        self.skip_ws();
        if self.lookahead == Some('(') {
            self.bump();
            let args = self.parse_args();
            self.skip_ws();
            if self.lookahead == Some(')') {
                self.bump();
            }
            return Some(parse_custom_type(&lower, &args));
        }
        Some(parse_custom_type(&lower, &[]))
    }

    fn parse_ident(&mut self) -> Option<String> {
        self.skip_ws();
        let mut out = String::new();
        while let Some(ch) = self.lookahead {
            if ch.is_alphanumeric() || ch == '_' {
                out.push(ch);
                self.bump();
            } else {
                break;
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    fn parse_args(&mut self) -> Vec<String> {
        let mut args = Vec::new();
        let mut current = String::new();
        let mut depth = 0i32;
        while let Some(ch) = self.lookahead {
            match ch {
                '(' => {
                    depth += 1;
                    current.push(ch);
                    self.bump();
                }
                ')' if depth == 0 => break,
                ')' => {
                    depth -= 1;
                    current.push(ch);
                    self.bump();
                }
                ',' if depth == 0 => {
                    args.push(current.trim().to_string());
                    current.clear();
                    self.bump();
                }
                _ => {
                    current.push(ch);
                    self.bump();
                }
            }
        }
        if !current.trim().is_empty() {
            args.push(current.trim().to_string());
        }
        args
    }
}

fn parse_value(value: &AstValue) -> Value {
    match value {
        AstValue::Number(n, _) => {
            if n.contains('.') {
                if let Some(dec) = Decimal::parse(n) {
                    Value::Decimal(dec)
                } else {
                    Value::Float(n.parse::<f64>().unwrap_or(0.0))
                }
            } else if let Ok(int) = n.parse::<i64>() {
                Value::Int(int)
            } else {
                Value::Float(n.parse::<f64>().unwrap_or(0.0))
            }
        }
        AstValue::SingleQuotedString(s) => Value::String(s.clone()),
        AstValue::DoubleQuotedString(s) => Value::String(s.clone()),
        AstValue::Boolean(v) => Value::Bool(*v),
        AstValue::Null => Value::Null,
        _ => Value::Null,
    }
}


fn eval_binary(op: &BinaryOperator, left: Value, right: Value) -> Value {
    match op {
        BinaryOperator::Plus => match (&left, &right) {
            (Value::DateTime(dt), Value::Int(offset)) => {
                Value::DateTime(add_seconds(dt, *offset))
            }
            (Value::DateTime64(dt), Value::Int(offset)) => {
                Value::DateTime64(add_seconds(dt, *offset))
            }
            _ => match (left.as_f64(), right.as_f64()) {
                (Some(a), Some(b)) => Value::Float(a + b),
                _ => Value::Null,
            },
        },
        BinaryOperator::Minus => match (&left, &right) {
            (Value::DateTime(dt), Value::Int(offset)) => {
                Value::DateTime(add_seconds(dt, -*offset))
            }
            (Value::DateTime64(dt), Value::Int(offset)) => {
                Value::DateTime64(add_seconds(dt, -*offset))
            }
            _ => match (left.as_f64(), right.as_f64()) {
                (Some(a), Some(b)) => Value::Float(a - b),
                _ => Value::Null,
            },
        },
        BinaryOperator::Multiply => match (left.as_f64(), right.as_f64()) {
            (Some(a), Some(b)) => Value::Float(a * b),
            _ => Value::Null,
        },
        BinaryOperator::Divide => match (left.as_f64(), right.as_f64()) {
            (Some(a), Some(b)) => Value::Float(a / b),
            _ => Value::Null,
        },
        BinaryOperator::Eq => Value::Bool(value_eq(&left, &right)),
        BinaryOperator::NotEq => Value::Bool(!value_eq(&left, &right)),
        BinaryOperator::Gt => Value::Bool(value_cmp(&left, &right) == Some(Ordering::Greater)),
        BinaryOperator::GtEq => {
            Value::Bool(matches!(value_cmp(&left, &right), Some(Ordering::Greater | Ordering::Equal)))
        }
        BinaryOperator::Lt => Value::Bool(value_cmp(&left, &right) == Some(Ordering::Less)),
        BinaryOperator::LtEq => {
            Value::Bool(matches!(value_cmp(&left, &right), Some(Ordering::Less | Ordering::Equal)))
        }
        BinaryOperator::And => {
            Value::Bool(left.as_bool().unwrap_or(false) && right.as_bool().unwrap_or(false))
        }
        BinaryOperator::Or => {
            Value::Bool(left.as_bool().unwrap_or(false) || right.as_bool().unwrap_or(false))
        }
        _ => Value::Null,
    }
}

fn eval_unary(op: &fp_sql::ast::UnaryOperator, value: Value) -> Value {
    match op {
        fp_sql::ast::UnaryOperator::Not => Value::Bool(!value.as_bool().unwrap_or(false)),
        fp_sql::ast::UnaryOperator::Minus => value
            .as_f64()
            .map(|v| Value::Float(-v))
            .unwrap_or(Value::Null),
        fp_sql::ast::UnaryOperator::Plus => value
            .as_f64()
            .map(Value::Float)
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn eval_function(
    func: &Function,
    columns: &[ColumnRef],
    row: &[Value],
    engine: &mut HotSqlEngine,
) -> Value {
    let name = func.name.to_string().to_ascii_lowercase();
    let args = func
        .args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                Some(engine.eval_expr(expr, columns, row))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    match name.as_str() {
        "count" => Value::Int(args.len() as i64),
        "sum" => {
            let sum = args.iter().filter_map(|v| v.as_f64()).sum::<f64>();
            Value::Float(sum)
        }
        "min" => args.into_iter().min_by(|a, b| value_cmp(a, b).unwrap_or(Ordering::Equal)).unwrap_or(Value::Null),
        "max" => args.into_iter().max_by(|a, b| value_cmp(a, b).unwrap_or(Ordering::Equal)).unwrap_or(Value::Null),
        "now" => {
            let dt = DateTimeValue::from_datetime(Utc::now(), 0);
            Value::DateTime(dt)
        }
        "todatetime" => {
            if let Some(Value::String(s)) = args.get(0) {
                parse_datetime_value(s, 0).map(Value::DateTime).unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        "todatetime64" => {
            let precision = args
                .get(1)
                .and_then(|v| v.as_u64())
                .unwrap_or(6) as u32;
            if let Some(Value::String(s)) = args.get(0) {
                parse_datetime_value(s, precision)
                    .map(Value::DateTime64)
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        "toyyyymm" => {
            let dt = match args.get(0) {
                Some(Value::DateTime(v)) => v.to_chrono(),
                Some(Value::DateTime64(v)) => v.to_chrono(),
                Some(Value::String(s)) => {
                    if let Some(dt) = parse_datetime_value(s, 0) {
                        dt.to_chrono()
                    } else {
                        return Value::Null;
                    }
                }
                _ => return Value::Null,
            };
            let val = dt.year() * 100 + dt.month() as i32;
            Value::Int(val as i64)
        }
        "touint64" => args.get(0).and_then(|v| v.as_u64()).map(Value::UInt).unwrap_or(Value::Null),
        "toint64" => args.get(0).and_then(|v| v.as_i64()).map(Value::Int).unwrap_or(Value::Null),
        "tofloat64" => args.get(0).and_then(|v| v.as_f64()).map(Value::Float).unwrap_or(Value::Null),
        "arrayjoin" => args.get(0).cloned().unwrap_or(Value::Null),
        "map" => {
            let mut pairs = Vec::new();
            let mut iter = args.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                pairs.push((k, v));
            }
            Value::Map(pairs)
        }
        "mapkeys" => {
            if let Some(Value::Map(values)) = args.get(0) {
                Value::Array(values.iter().map(|(k, _)| k.clone()).collect())
            } else {
                Value::Null
            }
        }
        "tupleelement" => {
            let Some(Value::Tuple(values)) = args.get(0) else {
                return Value::Null;
            };
            let index = args.get(1).and_then(|v| v.as_u64()).unwrap_or(1) as usize;
            values.get(index.saturating_sub(1)).cloned().unwrap_or(Value::Null)
        }
        "jsonextractstring" => {
            if let Some(Value::String(payload)) = args.get(0) {
                if let Some(Value::String(key)) = args.get(1) {
                    let parsed: JsonValue = serde_json::from_str(payload).unwrap_or(JsonValue::Null);
                    if let JsonValue::Object(map) = parsed {
                        if let Some(JsonValue::String(value)) = map.get(key) {
                            return Value::String(value.clone());
                        }
                    }
                }
            }
            Value::Null
        }
        "round" => {
            let scale = args.get(1).and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            if let Some(Value::Decimal(dec)) = args.get(0) {
                return Value::Decimal(dec.round(scale));
            }
            if let Some(v) = args.get(0).and_then(|v| v.as_f64()) {
                let factor = 10f64.powi(scale as i32);
                return Value::Float((v * factor).round() / factor);
            }
            Value::Null
        }
        "generateuuidv4" => Value::Uuid(Uuid::new_v4()),
        "currentdatabase" => Value::String("default".to_string()),
        "version" => Value::String("hot".to_string()),
        "countstate" => Value::Aggregate(AggregateState { count: 1, sum: 0.0 }),
        "sumstate" => Value::Aggregate(AggregateState { count: 1, sum: args.get(0).and_then(|v| v.as_f64()).unwrap_or(0.0) }),
        "countmerge" => {
            if let Some(Value::Aggregate(state)) = args.get(0) {
                Value::Int(state.count as i64)
            } else {
                Value::Null
            }
        }
        "summerge" => {
            if let Some(Value::Aggregate(state)) = args.get(0) {
                Value::Float(state.sum)
            } else {
                Value::Null
            }
        }
        "tounixtimestamp" => {
            if let Some(Value::DateTime(dt)) = args.get(0) {
                Value::Int(dt.to_chrono().timestamp())
            } else if let Some(Value::DateTime64(dt)) = args.get(0) {
                Value::Int(dt.to_chrono().timestamp())
            } else {
                Value::Null
            }
        }
        "tounixtimestamp64micro" => {
            if let Some(Value::DateTime(dt)) = args.get(0) {
                Value::Int(dt.to_chrono().timestamp_micros())
            } else if let Some(Value::DateTime64(dt)) = args.get(0) {
                Value::Int(dt.to_chrono().timestamp_micros())
            } else {
                Value::Null
            }
        }
        "cityhash64" => {
            if let Some(v) = args.get(0).and_then(|v| v.as_u64()) {
                Value::UInt(v.wrapping_mul(0x9E3779B97F4A7C15))
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

fn cast_value(value: Value, data_type: &AstDataType) -> Value {
    let target = map_data_type(data_type);
    match target {
        DataType::String => Value::String(value.to_string_value()),
        DataType::Int64 => value.as_i64().map(Value::Int).unwrap_or(Value::Null),
        DataType::UInt64 => value.as_u64().map(Value::UInt).unwrap_or(Value::Null),
        DataType::Float64 => value.as_f64().map(Value::Float).unwrap_or(Value::Null),
        DataType::Bool => value.as_bool().map(Value::Bool).unwrap_or(Value::Null),
        DataType::DateTime => Value::DateTime(DateTimeValue::from_datetime(Utc::now(), 0)),
        _ => value,
    }
}

fn eval_aggregate(
    func: &Function,
    dataset: &DataSet,
    rows: &[Vec<Value>],
    engine: &mut HotSqlEngine,
) -> Value {
    let name = func.name.to_string().to_ascii_lowercase();
    let args = func
        .args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    match name.as_str() {
        "count" => Value::Int(rows.len() as i64),
        "sum" => {
            let sum = rows
                .iter()
                .filter_map(|row| engine.eval_expr(&args[0], &dataset.columns, row).as_f64())
                .sum::<f64>();
            Value::Float(sum)
        }
        "min" => rows
            .iter()
            .map(|row| engine.eval_expr(&args[0], &dataset.columns, row))
            .min_by(|a, b| value_cmp(a, b).unwrap_or(Ordering::Equal))
            .unwrap_or(Value::Null),
        "max" => rows
            .iter()
            .map(|row| engine.eval_expr(&args[0], &dataset.columns, row))
            .max_by(|a, b| value_cmp(a, b).unwrap_or(Ordering::Equal))
            .unwrap_or(Value::Null),
        "argmax" => {
            let mut best: Option<(Value, Value)> = None;
            for row in rows {
                let value = engine.eval_expr(&args[0], &dataset.columns, row);
                let key = engine.eval_expr(&args[1], &dataset.columns, row);
                match &best {
                    None => best = Some((value, key)),
                    Some((_, current)) => {
                        if value_cmp(&key, current) == Some(Ordering::Greater) {
                            best = Some((value, key));
                        }
                    }
                }
            }
            best.map(|(v, _)| v).unwrap_or(Value::Null)
        }
        "countmerge" => {
            let total = rows
                .iter()
                .filter_map(|row| match engine.eval_expr(&args[0], &dataset.columns, row) {
                    Value::Aggregate(state) => Some(state.count),
                    _ => None,
                })
                .sum::<u64>();
            Value::Int(total as i64)
        }
        "summerge" => {
            let total = rows
                .iter()
                .filter_map(|row| match engine.eval_expr(&args[0], &dataset.columns, row) {
                    Value::Aggregate(state) => Some(state.sum),
                    _ => None,
                })
                .sum::<f64>();
            Value::Float(total)
        }
        _ => Value::Null,
    }
}

fn lookup_column(
    columns: &[ColumnRef],
    row: &[Value],
    name: &str,
    qualifier: Option<&str>,
) -> Value {
    let name = name.to_ascii_lowercase();
    let qualifier = qualifier.map(|q| q.to_ascii_lowercase());
    for (idx, col) in columns.iter().enumerate() {
        if col.name == name {
            if let Some(ref qual) = qualifier {
                if col.qualifier.as_ref() != Some(qual) {
                    continue;
                }
            }
            return row.get(idx).cloned().unwrap_or(Value::Null);
        }
    }
    Value::Null
}

fn value_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::UInt(a), Value::UInt(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Decimal(a), Value::Decimal(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        _ if left.as_f64().is_some() && right.as_f64().is_some() => {
            left.as_f64().unwrap() == right.as_f64().unwrap()
        }
        _ => false,
    }
}

fn value_cmp(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::UInt(a), Value::UInt(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Decimal(a), Value::Decimal(b)) => a.to_f64().partial_cmp(&b.to_f64()),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::DateTime(a), Value::DateTime(b)) => Some(a.micros.cmp(&b.micros)),
        (Value::DateTime64(a), Value::DateTime64(b)) => Some(a.micros.cmp(&b.micros)),
        _ => {
            let left_num = left.as_f64()?;
            let right_num = right.as_f64()?;
            left_num.partial_cmp(&right_num)
        }
    }
}

fn hash_value(value: &Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match value {
        Value::Null => 0u8.hash(&mut hasher),
        Value::Bool(v) => v.hash(&mut hasher),
        Value::Int(v) => v.hash(&mut hasher),
        Value::UInt(v) => v.hash(&mut hasher),
        Value::Float(v) => v.to_bits().hash(&mut hasher),
        Value::Decimal(v) => {
            v.value.hash(&mut hasher);
            v.scale.hash(&mut hasher);
        }
        Value::String(v) => v.hash(&mut hasher),
        Value::DateTime(v) | Value::DateTime64(v) => {
            v.micros.hash(&mut hasher);
            v.precision.hash(&mut hasher);
        }
        Value::Uuid(v) => v.to_string().hash(&mut hasher),
        Value::Ipv4(v) => u32::from(*v).hash(&mut hasher),
        Value::Array(values) | Value::Tuple(values) => {
            for v in values {
                hash_value(v).hash(&mut hasher);
            }
        }
        Value::Map(values) => {
            for (k, v) in values {
                hash_value(k).hash(&mut hasher);
                hash_value(v).hash(&mut hasher);
            }
        }
        Value::Aggregate(state) => {
            state.count.hash(&mut hasher);
            state.sum.to_bits().hash(&mut hasher);
        }
    }
    hasher.finish()
}

fn parse_datetime_value(s: &str, precision: u32) -> Option<DateTimeValue> {
    let fmt = "%Y-%m-%d %H:%M:%S";
    let parsed = NaiveDateTime::parse_from_str(s, fmt)
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .ok()?;
    Some(DateTimeValue::from_datetime(Utc.from_utc_datetime(&parsed), precision))
}

fn add_seconds(value: &DateTimeValue, seconds: i64) -> DateTimeValue {
    DateTimeValue::from_micros(value.micros + seconds * 1_000_000, value.precision)
}

fn datetime_to_micros(value: &Value) -> Option<i64> {
    match value {
        Value::DateTime(dt) => Some(dt.micros),
        Value::DateTime64(dt) => Some(dt.micros),
        _ => None,
    }
}

fn parse_interval(
    interval: &fp_sql::ast::Interval,
    columns: &[ColumnRef],
    row: &[Value],
    engine: &mut HotSqlEngine,
) -> Value {
    let quantity = engine
        .eval_expr(&interval.value, columns, row)
        .as_i64()
        .unwrap_or(0);
    let unit = interval
        .leading_field
        .unwrap_or(fp_sql::ast::DateTimeField::Day);
    let seconds = match unit {
        fp_sql::ast::DateTimeField::Day => quantity * 86_400,
        fp_sql::ast::DateTimeField::Hour => quantity * 3600,
        fp_sql::ast::DateTimeField::Minute => quantity * 60,
        fp_sql::ast::DateTimeField::Second => quantity,
        _ => quantity,
    };
    Value::Int(seconds)
}

fn has_group_by(select: &Select) -> bool {
    !matches!(select.group_by, GroupByExpr::Expressions(ref exprs) if exprs.is_empty())
}

fn has_aggregate_projection(select: &Select) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            contains_aggregate(expr)
        }
        _ => false,
    })
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => is_aggregate_function(func) && func.over.is_none(),
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::Nested(expr) => contains_aggregate(expr),
        Expr::Tuple(exprs) => exprs.iter().any(contains_aggregate),
        Expr::Array(array) => array.elem.iter().any(contains_aggregate),
        _ => false,
    }
}

fn is_aggregate_function(func: &Function) -> bool {
    let name = func.name.to_string().to_ascii_lowercase();
    matches!(
        name.as_str(),
        "count" | "sum" | "min" | "max" | "argmax" | "countmerge" | "summerge"
    )
}

fn is_array_join(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => func.name.to_string().eq_ignore_ascii_case("arrayJoin"),
        _ => false,
    }
}

fn group_by_exprs(group_by: &GroupByExpr) -> Vec<Expr> {
    match group_by {
        GroupByExpr::Expressions(exprs) => exprs.clone(),
        GroupByExpr::All => Vec::new(),
    }
}

fn projected_columns(select: &Select) -> anyhow::Result<Vec<ColumnRef>> {
    let mut columns = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::ExprWithAlias { alias, .. } => columns.push(ColumnRef {
                name: alias.value.to_ascii_lowercase(),
                qualifier: None,
            }),
            SelectItem::UnnamedExpr(expr) => columns.push(ColumnRef {
                name: expr.to_string().to_ascii_lowercase(),
                qualifier: None,
            }),
            SelectItem::Wildcard(_) => return Err(anyhow!("wildcard in projection not supported")),
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(anyhow!("qualified wildcard in projection not supported"))
            }
        }
    }
    Ok(columns)
}

fn apply_join(
    left: DataSet,
    right: DataSet,
    join: &JoinOperator,
    engine: &mut HotSqlEngine,
) -> anyhow::Result<DataSet> {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    let mut rows = Vec::new();

    match join {
        JoinOperator::Inner(constraint) => {
            for left_row in &left.rows {
                for right_row in &right.rows {
                    let mut combined = left_row.clone();
                    combined.extend(right_row.clone());
                    if match_join_constraint(constraint, &columns, &combined, engine) {
                        rows.push(combined);
                    }
                }
            }
        }
        JoinOperator::LeftOuter(constraint) => {
            for left_row in &left.rows {
                let mut matched = false;
                for right_row in &right.rows {
                    let mut combined = left_row.clone();
                    combined.extend(right_row.clone());
                    if match_join_constraint(constraint, &columns, &combined, engine) {
                        rows.push(combined);
                        matched = true;
                    }
                }
                if !matched {
                    let mut combined = left_row.clone();
                    combined.extend(vec![Value::Null; right.columns.len()]);
                    rows.push(combined);
                }
            }
        }
        _ => {}
    }

    Ok(DataSet { columns, rows })
}

fn match_join_constraint(
    constraint: &JoinConstraint,
    columns: &[ColumnRef],
    row: &[Value],
    engine: &mut HotSqlEngine,
) -> bool {
    match constraint {
        JoinConstraint::On(expr) => engine
            .eval_expr(expr, columns, row)
            .as_bool()
            .unwrap_or(false),
        JoinConstraint::Using(_) => true,
        _ => true,
    }
}

fn apply_order_by(
    dataset: &mut DataSet,
    order_by: &[OrderByExpr],
    engine: &mut HotSqlEngine,
) -> anyhow::Result<()> {
    dataset.rows.sort_by(|a, b| {
        for order in order_by {
            let left = engine.eval_expr(&order.expr, &dataset.columns, a);
            let right = engine.eval_expr(&order.expr, &dataset.columns, b);
            let cmp = value_cmp(&left, &right).unwrap_or(Ordering::Equal);
            if cmp != Ordering::Equal {
                return if order.asc.unwrap_or(true) { cmp } else { cmp.reverse() };
            }
        }
        Ordering::Equal
    });
    Ok(())
}

fn apply_array_join(
    dataset: DataSet,
    select: &Select,
    engine: &mut HotSqlEngine,
) -> anyhow::Result<DataSet> {
    let mut array_exprs = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(func))
            | SelectItem::ExprWithAlias { expr: Expr::Function(func), .. } => {
                if func.name.to_string().eq_ignore_ascii_case("arrayJoin") {
                    if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = func.args.get(0)
                    {
                        array_exprs.push(expr.clone());
                    }
                }
            }
            _ => {}
        }
    }

    if array_exprs.is_empty() {
        return Ok(dataset);
    }

    let mut rows = Vec::new();
    for row in &dataset.rows {
        let mut expanded = vec![row.clone()];
        for expr in &array_exprs {
            let mut next_rows = Vec::new();
            for base in expanded {
                let value = engine.eval_expr(expr, &dataset.columns, &base);
                if let Value::Array(values) = value {
                    for val in values {
                        let mut new_row = base.clone();
                        new_row.push(val);
                        next_rows.push(new_row);
                    }
                } else {
                    next_rows.push(base);
                }
            }
            expanded = next_rows;
        }
        rows.extend(expanded);
    }

    let mut columns = dataset.columns.clone();
    for expr in array_exprs {
        columns.push(ColumnRef {
            name: expr.to_string().to_ascii_lowercase(),
            qualifier: None,
        });
    }

    Ok(DataSet { columns, rows })
}

fn apply_sample(
    dataset: DataSet,
    select: &Select,
    ratio: f64,
    engine: &mut HotSqlEngine,
) -> anyhow::Result<DataSet> {
    let clamped = ratio.clamp(0.0, 1.0);
    if clamped <= 0.0 {
        return Ok(DataSet {
            columns: dataset.columns,
            rows: Vec::new(),
        });
    }
    if clamped >= 1.0 {
        return Ok(dataset);
    }

    let sample_expr = select
        .from
        .get(0)
        .and_then(|from| match &from.relation {
            fp_sql::ast::TableFactor::Table { name, .. } => {
                engine.tables.get(&name.to_string().to_ascii_lowercase())
            }
            _ => None,
        })
        .and_then(|table| table.sample_by.clone());

    let mut rows = Vec::new();
    for (idx, row) in dataset.rows.iter().enumerate() {
        let key = if let Some(expr) = &sample_expr {
            engine.eval_expr(expr, &dataset.columns, row)
        } else {
            Value::UInt(idx as u64)
        };
        let hashed = hash_value(&key);
        let threshold = (clamped * u64::MAX as f64) as u64;
        if hashed <= threshold {
            rows.push(row.clone());
        }
    }

    Ok(DataSet {
        columns: dataset.columns,
        rows,
    })
}

fn compute_window_values(
    dataset: &DataSet,
    select: &Select,
    engine: &mut HotSqlEngine,
) -> anyhow::Result<HashMap<String, Vec<Value>>> {
    let mut values = HashMap::new();
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        let Expr::Function(func) = expr else {
            continue;
        };
        if func.over.is_none() {
            continue;
        }
        let key = expr.to_string();
        let window_values = eval_window_function(dataset, func, engine)?;
        values.insert(key, window_values);
    }
    Ok(values)
}

fn eval_window_function(
    dataset: &DataSet,
    func: &Function,
    engine: &mut HotSqlEngine,
) -> anyhow::Result<Vec<Value>> {
    let spec = match func.over.clone() {
        Some(WindowType::WindowSpec(spec)) => spec,
        None => fp_sql::ast::WindowSpec {
            partition_by: Vec::new(),
            order_by: Vec::new(),
            window_frame: None,
        },
        _ => return Ok(vec![Value::Null; dataset.rows.len()]),
    };
    let partition_exprs = spec.partition_by;
    let order_exprs = spec.order_by;
    let frame = spec.window_frame;

    let mut partitions: BTreeMap<Vec<ValueKey>, Vec<usize>> = BTreeMap::new();
    for (idx, row) in dataset.rows.iter().enumerate() {
        let key = partition_exprs
            .iter()
            .map(|expr| ValueKey::from_value(&engine.eval_expr(expr, &dataset.columns, row)))
            .collect();
        partitions.entry(key).or_default().push(idx);
    }

    let mut output = vec![Value::Null; dataset.rows.len()];
    for indices in partitions.values_mut() {
        indices.sort_by(|a, b| {
            for order in &order_exprs {
                let left = engine.eval_expr(&order.expr, &dataset.columns, &dataset.rows[*a]);
                let right = engine.eval_expr(&order.expr, &dataset.columns, &dataset.rows[*b]);
                let cmp = value_cmp(&left, &right).unwrap_or(Ordering::Equal);
                if cmp != Ordering::Equal {
                    return if order.asc.unwrap_or(true) { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });

        for (pos, row_idx) in indices.iter().enumerate() {
            let window_range = window_frame_range(frame.as_ref(), pos, indices.len());
            let rows = &indices[window_range.0..window_range.1];
            let mut sum = 0f64;
            for idx in rows {
                if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = func.args.get(0) {
                    if let Some(v) = engine.eval_expr(expr, &dataset.columns, &dataset.rows[*idx]).as_f64() {
                        sum += v;
                    }
                }
            }
            output[*row_idx] = Value::Float(sum);
        }
    }

    Ok(output)
}

fn window_frame_range(
    frame: Option<&fp_sql::ast::WindowFrame>,
    position: usize,
    total: usize,
) -> (usize, usize) {
    let frame = frame.cloned().unwrap_or_default();
    if frame.units != WindowFrameUnits::Rows {
        return (0, position + 1);
    }
    let start = match frame.start_bound {
        WindowFrameBound::Preceding(None) => 0,
        WindowFrameBound::CurrentRow => position,
        WindowFrameBound::Preceding(Some(expr)) => {
            let offset = expr.to_string().parse::<usize>().unwrap_or(0);
            position.saturating_sub(offset)
        }
        WindowFrameBound::Following(Some(expr)) => {
            let offset = expr.to_string().parse::<usize>().unwrap_or(0);
            (position + offset).min(total)
        }
        WindowFrameBound::Following(None) => position,
    };
    let end = match frame.end_bound.unwrap_or(WindowFrameBound::CurrentRow) {
        WindowFrameBound::CurrentRow => position + 1,
        WindowFrameBound::Following(None) => total,
        WindowFrameBound::Following(Some(expr)) => {
            let offset = expr.to_string().parse::<usize>().unwrap_or(0);
            (position + offset + 1).min(total)
        }
        WindowFrameBound::Preceding(Some(expr)) => {
            let offset = expr.to_string().parse::<usize>().unwrap_or(0);
            position.saturating_sub(offset) + 1
        }
        WindowFrameBound::Preceding(None) => position + 1,
    };
    (start, end)
}

fn dedup_rows(dataset: &mut DataSet) {
    let mut seen = HashSet::new();
    dataset.rows.retain(|row| {
        let key = row.iter().map(ValueKey::from_value).collect::<Vec<_>>();
        if seen.contains(&key) {
            false
        } else {
            seen.insert(key);
            true
        }
    });
}

#[derive(Debug, Clone, Copy)]
enum FormatHint {
    JsonEachRow,
    TsvWithNames,
    TabSeparated,
}

fn render_dataset(dataset: DataSet, format: Option<FormatHint>) -> anyhow::Result<SqlResult> {
    match format.unwrap_or(FormatHint::JsonEachRow) {
        FormatHint::JsonEachRow => {
            let mut rows = Vec::new();
            for row in dataset.rows {
                let mut map = Map::new();
                for (idx, col) in dataset.columns.iter().enumerate() {
                    map.insert(col.name.clone(), value_to_json(row.get(idx)));
                }
                rows.push(map);
            }
            Ok(SqlResult::Rows(rows))
        }
        FormatHint::TsvWithNames => {
            let mut out = String::new();
            if !dataset.columns.is_empty() {
                out.push_str(
                    &dataset
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect::<Vec<_>>()
                        .join("\t"),
                );
                out.push('\n');
            }
            for row in dataset.rows {
                let line = row
                    .iter()
                    .map(|v| v.to_string_value())
                    .collect::<Vec<_>>()
                    .join("\t");
                out.push_str(&line);
                out.push('\n');
            }
            Ok(SqlResult::Text(out))
        }
        FormatHint::TabSeparated => {
            let mut out = String::new();
            for row in dataset.rows {
                let line = row
                    .iter()
                    .map(|v| v.to_string_value())
                    .collect::<Vec<_>>()
                    .join("\t");
                out.push_str(&line);
                out.push('\n');
            }
            Ok(SqlResult::Text(out))
        }
    }
}

fn value_to_json(value: Option<&Value>) -> JsonValue {
    match value {
        Some(Value::Null) | None => JsonValue::Null,
        Some(Value::Bool(v)) => JsonValue::Bool(*v),
        Some(Value::Int(v)) => JsonValue::Number((*v).into()),
        Some(Value::UInt(v)) => JsonValue::Number((*v).into()),
        Some(Value::Float(v)) => JsonValue::Number(
            serde_json::Number::from_f64(*v).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        Some(Value::Decimal(v)) => JsonValue::String(v.to_string_with_scale()),
        Some(Value::String(v)) => JsonValue::String(v.clone()),
        Some(Value::DateTime(v)) | Some(Value::DateTime64(v)) => {
            JsonValue::String(v.to_chrono().to_rfc3339())
        }
        Some(Value::Uuid(v)) => JsonValue::String(v.to_string()),
        Some(Value::Ipv4(v)) => JsonValue::String(v.to_string()),
        Some(Value::Array(values)) => JsonValue::Array(values.iter().map(|v| value_to_json(Some(v))).collect()),
        Some(Value::Tuple(values)) => JsonValue::Array(values.iter().map(|v| value_to_json(Some(v))).collect()),
        Some(Value::Map(values)) => {
            let mut map = Map::new();
            for (k, v) in values {
                map.insert(k.to_string_value(), value_to_json(Some(v)));
            }
            JsonValue::Object(map)
        }
        Some(Value::Aggregate(state)) => {
            let mut map = Map::new();
            map.insert("count".to_string(), JsonValue::Number(state.count.into()));
            map.insert(
                "sum".to_string(),
                JsonValue::Number(
                    serde_json::Number::from_f64(state.sum)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
            );
            JsonValue::Object(map)
        }
    }
}
