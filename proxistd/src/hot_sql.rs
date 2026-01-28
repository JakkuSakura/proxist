use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::Ipv4Addr;

use anyhow::anyhow;
use chrono::{Datelike, NaiveDateTime, TimeZone, Utc};
use fp_core::sql_ast::{
    AlterTableOperation, Assignment, BinaryOperator, ColumnOption, DataType as AstDataType,
    DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, Interval,
    JoinConstraint, JoinOperator, ObjectName, ObjectType, OrderByExpr, Query, Select, SelectItem,
    SampleRatio, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, TableWithJoins,
    UnaryOperator, Value as AstValue, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowSpec,
    WindowType,
};
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

    pub fn execute_statement(&mut self, stmt: &Statement) -> anyhow::Result<SqlResult> {
        match stmt {
            Statement::CreateView { name, query, .. } => {
                let key = name.to_string().to_ascii_lowercase();
                self.views.insert(
                    key.clone(),
                    View {
                        name: key,
                        query: *query.clone(),
                    },
                );
                Ok(SqlResult::Text("Ok.\n".to_string()))
            }
            Statement::Drop { object_type, names, .. } => {
                if matches!(object_type, ObjectType::Table) {
                    for name in names {
                        self.tables.remove(&name.to_string().to_ascii_lowercase());
                    }
                }
                if matches!(object_type, ObjectType::View) {
                    for name in names {
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
                    if let AlterTableOperation::AddColumn { column_def, .. } = op {
                        let name = column_def.name.value.to_ascii_lowercase();
                        let mut default = None;
                        for opt in &column_def.options {
                            if let ColumnOption::Default(expr) = &opt.option {
                                default = Some(expr.clone());
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
            } => self.execute_insert(table_name, columns, source),
            Statement::Query(query) => {
                let (dataset, format_hint) = self.execute_query(
                    query,
                    query.format.as_deref(),
                    query.sample_ratio.as_ref().map(|s| s.as_f64()),
                )?;
                let output = render_dataset(dataset, format_hint)?;
                Ok(output)
            }
            Statement::CreateTable {
                name,
                columns,
                engine,
                order_by,
                as_table,
                ttl,
                sample_by,
                query,
            } => {
                let info = CreateTableInfo {
                    name: name.to_string().to_ascii_lowercase(),
                    columns: columns
                        .iter()
                        .map(|col| {
                            let name = col.name.value.to_ascii_lowercase();
                            let data_type = map_data_type(&col.data_type);
                            let mut default = None;
                            let mut materialized = None;
                            for opt in &col.options {
                                match &opt.option {
                                    ColumnOption::Default(expr) => {
                                        default = Some(expr.clone());
                                    }
                                    ColumnOption::Materialized(expr) => {
                                        materialized = Some(expr.clone());
                                    }
                                }
                            }
                            ColumnDefn {
                                name,
                                data_type,
                                default,
                                materialized,
                            }
                        })
                        .collect(),
                    engine: engine.clone(),
                    as_table: as_table
                        .as_ref()
                        .map(|t: &ObjectName| t.to_string().to_ascii_lowercase()),
                    query: query.as_ref().map(|q: &Box<Query>| q.as_ref().clone()),
                    order_by: order_by
                        .as_ref()
                        .map(|cols: &Vec<Ident>| cols.iter().map(|c| c.value.clone()).collect()),
                    ttl: ttl.clone(),
                    sample_by: sample_by.clone(),
                };
                self.execute_create_table(info)
            }
            Statement::Update {
                table,
                assignments,
                selection,
            } => self.execute_update(table, assignments, selection.as_ref()),
            Statement::Delete { from, selection } => {
                self.execute_delete(from, selection.as_ref())
            }
        }
    }

    fn execute_update(
        &mut self,
        table: &ObjectName,
        assignments: &[Assignment],
        selection: Option<&Expr>,
    ) -> anyhow::Result<SqlResult> {
        let table_name = table.to_string().to_ascii_lowercase();
        let mut table = match self.tables.remove(&table_name) {
            Some(table) => table,
            None => return Ok(SqlResult::Text("Ok.\n".to_string())),
        };
        let column_refs = column_refs_from_defs(&table.columns);
        for row in &mut table.rows {
            if let Some(expr) = selection {
                let keep = self
                    .eval_expr(expr, &column_refs, row)
                    .as_bool()
                    .unwrap_or(false);
                if !keep {
                    continue;
                }
            }
            for assignment in assignments {
                let name = assignment
                    .id
                    .last()
                    .map(|ident| ident.value.to_ascii_lowercase());
                let Some(name) = name else {
                    continue;
                };
                if let Some(idx) = table.columns.iter().position(|col| col.name == name) {
                    let value = self.eval_expr(&assignment.value, &column_refs, row);
                    row[idx] = value;
                }
            }
        }
        self.tables.insert(table_name, table);
        Ok(SqlResult::Text("Ok.\n".to_string()))
    }

    fn execute_delete(
        &mut self,
        from: &[TableWithJoins],
        selection: Option<&Expr>,
    ) -> anyhow::Result<SqlResult> {
        if from.len() != 1 {
            return Ok(SqlResult::Text("Ok.\n".to_string()));
        }
        let table = match &from[0].relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Ok(SqlResult::Text("Ok.\n".to_string())),
        };
        let table_name = table.to_ascii_lowercase();
        let mut table = match self.tables.remove(&table_name) {
            Some(table) => table,
            None => return Ok(SqlResult::Text("Ok.\n".to_string())),
        };
        let column_refs = column_refs_from_defs(&table.columns);
        table.rows.retain(|row| {
            if let Some(expr) = selection {
                let keep = self
                    .eval_expr(expr, &column_refs, row)
                    .as_bool()
                    .unwrap_or(false);
                !keep
            } else {
                false
            }
        });
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
        table_name: &ObjectName,
        columns: &[Ident],
        source: &Option<Box<Query>>,
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

        let rows_to_insert: Vec<Vec<Value>> = match source.as_deref().map(|q| q.body.as_ref()) {
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
            Some(SetExpr::Select(_)) => {
                let query = source.as_ref().unwrap();
                let (dataset, _) = self.execute_query(query, None, None)?;
                dataset.rows
            }
            _ => Vec::new(),
        };

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
                        body: left.clone(),
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                        format: None,
                        sample_ratio: sample_ratio.map(SampleRatio),
                    },
                    None,
                    sample_ratio,
                )?;
                let right = self.execute_query(
                    &Query {
                        body: right.clone(),
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                        format: None,
                        sample_ratio: sample_ratio.map(SampleRatio),
                    },
                    None,
                    sample_ratio,
                )?;
                if !matches!(op, SetOperator::Union)
                    || !matches!(set_quantifier, SetQuantifier::All)
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
            let offset_value = self.eval_expr(offset, &[], &[]).as_u64().unwrap_or(0) as usize;
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
            TableFactor::Table { name, .. } => Some(name.to_string()),
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
        relation: &TableFactor,
    ) -> anyhow::Result<DataSet> {
        match relation {
            TableFactor::Table { name, alias, .. } => {
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
            TableFactor::Derived { subquery, alias, .. } => {
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
                SelectItem::Wildcard => {
                    out.extend(row.iter().cloned());
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
                SelectItem::Wildcard => {
                    out.extend(base_row.iter().cloned());
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
            Expr::Array(exprs) => {
                let values = exprs
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

fn map_data_type(data_type: &AstDataType) -> DataType {
    match data_type {
        AstDataType::Boolean => DataType::Bool,
        AstDataType::Int64 => DataType::Int64,
        AstDataType::UnsignedBigInt => DataType::UInt64,
        AstDataType::Float64 => DataType::Float64,
        AstDataType::Decimal { precision, scale } => DataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        AstDataType::Date => DataType::DateTime,
        AstDataType::DateTime => DataType::DateTime,
        AstDataType::DateTime64(precision) => DataType::DateTime64(*precision),
        AstDataType::Uuid => DataType::Uuid,
        AstDataType::Ipv4 => DataType::Ipv4,
        AstDataType::String => DataType::String,
        AstDataType::Array(inner) => DataType::Array(Box::new(map_data_type(inner))),
        AstDataType::Tuple(items) => DataType::Tuple(items.iter().map(map_data_type).collect()),
        AstDataType::Map(key, value) => {
            DataType::Map(Box::new(map_data_type(key)), Box::new(map_data_type(value)))
        }
        AstDataType::Custom(name, modifiers) => {
            let name = name.to_ascii_lowercase();
            parse_custom_type(&name, modifiers)
        }
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

fn eval_unary(op: &UnaryOperator, value: Value) -> Value {
    match op {
        UnaryOperator::Not => Value::Bool(!value.as_bool().unwrap_or(false)),
        UnaryOperator::Minus => value
            .as_f64()
            .map(|v| Value::Float(-v))
            .unwrap_or(Value::Null),
        UnaryOperator::Plus => value
            .as_f64()
            .map(Value::Float)
            .unwrap_or(Value::Null),
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
    if let Some(qual) = qualifier {
        let combined = format!("{qual}.{name}");
        for (idx, col) in columns.iter().enumerate() {
            if col.name == combined {
                return row.get(idx).cloned().unwrap_or(Value::Null);
            }
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
    interval: &Interval,
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
        .clone()
        .unwrap_or(DateTimeField::Day);
    let seconds = match unit {
        DateTimeField::Day => quantity * 86_400,
        DateTimeField::Hour => quantity * 3600,
        DateTimeField::Minute => quantity * 60,
        DateTimeField::Second => quantity,
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
        Expr::Array(exprs) => exprs.iter().any(contains_aggregate),
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
            SelectItem::Wildcard => return Err(anyhow!("wildcard in projection not supported")),
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
        JoinConstraint::None => true,
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
            let expr_name = expr.to_string().to_ascii_lowercase();
            let expr_idx = dataset.columns.iter().position(|c| c.name == expr_name);
            let mut next_rows = Vec::new();
            for base in expanded {
                let value = engine.eval_expr(expr, &dataset.columns, &base);
                if let Value::Array(values) = value {
                    for val in values {
                        let mut new_row = base.clone();
                        if let Some(idx) = expr_idx {
                            new_row[idx] = val;
                        } else {
                            new_row.push(val);
                        }
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
    Ok(DataSet {
        columns: dataset.columns,
        rows,
    })
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
            TableFactor::Table { name, .. } => {
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
        Some(WindowType::WindowSpec(spec)) => *spec,
        None => WindowSpec::default(),
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
            let window_range = window_frame_range(frame.as_ref().map(|v| &**v), pos, indices.len());
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
    frame: Option<&WindowFrame>,
    position: usize,
    total: usize,
) -> (usize, usize) {
    let frame = frame.cloned().unwrap_or(WindowFrame {
        units: WindowFrameUnits::Rows,
        start_bound: WindowFrameBound::CurrentRow,
        end_bound: None,
    });
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
