use crate::clickhouse::{ClickhouseHttpClient, ClickhouseNativeClient};
use crate::metadata_sqlite::SqliteMetadataStore;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use bytes::Bytes;
mod plan;
use plan::{
    build_table_plan, parse_table_schema_from_ddl, rewrite_with_bounds, OrderItem, Predicate,
};
use proxist_core::query::QueryRange;
use proxist_mem::{HotColumnStore, HotSymbolSummary};
use serde_json::{Map, Value as JsonValue};
use sqlx::{Column, Row};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

#[cfg(feature = "postgres")]
use tokio_postgres;

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub sqlite_path: Option<String>,
    pub pg_url: Option<String>,
}

#[async_trait]
pub trait SqlExecutor: Send + Sync {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickhouseWireFormat {
    JsonEachRow,
    Other,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct ClickhouseWire {
    pub format: ClickhouseWireFormat,
    pub body: String,
}

impl ClickhouseWire {
    fn jsoneachrow(body: String) -> Self {
        Self {
            format: ClickhouseWireFormat::JsonEachRow,
            body,
        }
    }

    fn with_unknown(body: String) -> Self {
        Self {
            format: ClickhouseWireFormat::Unknown,
            body,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SqlResult {
    Clickhouse(ClickhouseWire),
    Text(String),
    Rows(Vec<Map<String, JsonValue>>),
    TypedRows(RowSet),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Text,
    Int64,
    Float64,
    Bool,
}

#[derive(Debug, Clone)]
pub struct RowSet {
    pub columns: Vec<String>,
    pub column_types: Vec<ColumnType>,
    pub rows: Vec<Vec<ScalarValue>>,
}

#[derive(Debug, Clone)]
pub struct TableConfig {
    pub order_col: String,
    pub filter_cols: Vec<String>,
    pub seq_col: Option<String>,
    pub columns: Vec<String>,
    pub column_types: Vec<ColumnType>,
}

pub struct TableRegistry {
    tables: Mutex<HashMap<String, TableConfig>>,
}

impl TableRegistry {
    pub fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
        }
    }

    pub fn register_table(&self, table: impl Into<String>, cfg: TableConfig) {
        let key = table.into().to_ascii_lowercase();
        self.tables.lock().unwrap().insert(key, cfg);
    }

    pub fn table_config(&self, table: &str) -> Option<TableConfig> {
        let key = table.to_ascii_lowercase();
        self.tables.lock().unwrap().get(&key).cloned()
    }
}

pub struct ProxistScheduler {
    sqlite: Option<SqliteExecutor>,
    clickhouse: Option<ClickhouseHttpClient>,
    clickhouse_native: Option<ClickhouseNativeClient>,
    postgres: Option<PostgresExecutor>,
    hot_store: Option<Arc<dyn HotColumnStore>>,
    registry: Arc<TableRegistry>,
    persisted_cutoff_micros: AtomicI64,
    hot_stats: Arc<RwLock<HashMap<(String, String), HotStats>>>,
}

impl ProxistScheduler {
    pub async fn new(
        cfg: ExecutorConfig,
        clickhouse: Option<ClickhouseHttpClient>,
        clickhouse_native: Option<ClickhouseNativeClient>,
        hot_store: Option<Arc<dyn HotColumnStore>>,
        registry: Arc<TableRegistry>,
    ) -> anyhow::Result<Self> {
        let sqlite = match cfg.sqlite_path {
            Some(path) => Some(
                SqliteExecutor::connect(&path)
                    .await
                    .context("connect sqlite")?,
            ),
            None => None,
        };
        let postgres = PostgresExecutor::maybe_connect(cfg.pg_url.as_deref()).await?;

        Ok(Self {
            sqlite,
            clickhouse,
            clickhouse_native,
            postgres,
            hot_store,
            registry,
            persisted_cutoff_micros: AtomicI64::new(-1),
            hot_stats: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn register_table(&self, table: impl Into<String>, cfg: TableConfig) {
        self.registry.register_table(table, cfg);
    }

    pub fn register_ddl(&self, ddl_sql: &str) -> anyhow::Result<()> {
        let Some(annotation) = parse_proxist_table_config(ddl_sql)? else {
            return Ok(());
        };
        let (table, columns) = parse_table_schema_from_ddl(ddl_sql)
            .ok_or_else(|| anyhow!("unable to determine table schema from DDL"))?;
        if columns.is_empty() {
            anyhow::bail!("proxist annotation requires explicit column definitions");
        }
        let mut column_names = Vec::with_capacity(columns.len());
        let mut column_types = Vec::with_capacity(columns.len());
        for (name, ty) in columns {
            column_names.push(name);
            column_types.push(ty);
        }
        self.registry.register_table(
            table,
            TableConfig {
                order_col: annotation.order_col,
                filter_cols: annotation.filter_cols,
                seq_col: annotation.seq_col,
                columns: column_names,
                column_types,
            },
        );
        Ok(())
    }

    pub fn table_config(&self, table: &str) -> Option<TableConfig> {
        self.registry.table_config(table)
    }

    pub fn set_persisted_cutoff(&self, cutoff: Option<SystemTime>) {
        let micros = cutoff
            .and_then(|ts| ts.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|dur| dur.as_micros() as i64)
            .unwrap_or(-1);
        self.persisted_cutoff_micros.store(micros, Ordering::SeqCst);
    }

    pub async fn update_hot_stats(&self, stats: &[HotSymbolSummary]) {
        let mut map = self.hot_stats.write().await;
        map.clear();
        for entry in stats {
            let key0 = unsafe { std::str::from_utf8_unchecked(&entry.key0) }.to_string();
            let key1 = unsafe { std::str::from_utf8_unchecked(&entry.key1) }.to_string();
            map.insert(
                (key0, key1),
                HotStats {
                    first_micros: entry.first_micros,
                    last_micros: entry.last_micros,
                },
            );
        }
    }

    async fn hot_covers_window(
        &self,
        tenant: &str,
        symbols: &[String],
        start: i64,
        end: i64,
    ) -> bool {
        let map = self.hot_stats.read().await;
        for sym in symbols {
            let Some(stats) = map.get(&(tenant.to_string(), sym.clone())) else {
                return false;
            };
            let Some(first) = stats.first_micros else {
                return false;
            };
            let Some(last) = stats.last_micros else {
                return false;
            };
            if first > start || last < end {
                return false;
            }
        }
        true
    }
}

#[async_trait]
impl SqlExecutor for ProxistScheduler {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult> {
        let normalized = sql.trim().to_ascii_lowercase();

        if normalized.starts_with("select") || normalized.starts_with("with") {
            if let Some(hot) = &self.hot_store {
                let plan = {
                    analyze_select(sql, &self.registry)
                };

                if let Some(plan) = plan {
                    let requested_format = detect_wire_format(sql);
                    if !matches!(
                        requested_format,
                        ClickhouseWireFormat::JsonEachRow | ClickhouseWireFormat::Unknown
                    ) {
                        if let Some(ch) = &self.clickhouse {
                            return Ok(SqlResult::Clickhouse(ClickhouseWire {
                                format: requested_format,
                                body: ch.execute_raw(sql).await?,
                            }));
                        }
                        // fall through to default executors if ClickHouse is unavailable
                    } else {
                        let limit_hint = plan
                            .limit
                            .map(|limit| limit.saturating_add(plan.offset.unwrap_or(0)));

                        let cutoff = self.persisted_cutoff_micros.load(Ordering::SeqCst);
                        let mut cold_rows: Option<RowSet> = None;
                        let mut hot_rows: Option<RowSet> = None;
                        let mut plan_failed = false;

                        if cutoff >= 0 {
                            let start_u = system_time_to_micros(plan.select.start);
                            let end_u = system_time_to_micros(plan.select.end);
                            let mut need_cold = start_u <= cutoff;

                            if need_cold
                                && self
                                    .hot_covers_window(
                                        &plan.select.key0,
                                        &plan.select.key1_list,
                                        start_u,
                                        end_u,
                                    )
                                    .await
                            {
                                need_cold = false;
                            }

                            if need_cold && start_u <= cutoff {
                                let cold_end = cutoff.min(end_u);
                                if let Some(cold_sql) = rewrite_with_bounds(
                                    sql,
                                    &plan.cfg.order_col,
                                    Some(start_u),
                                    Some(cold_end),
                                    limit_hint,
                                ) {
                                    let columns = expected_columns(&plan);
                                    let column_types = expected_column_types(&plan);
                                    match requested_format {
                                        ClickhouseWireFormat::JsonEachRow => {
                                            if let Some(ch) = &self.clickhouse {
                                                let cold_sql = ensure_jsoneachrow(&cold_sql);
                                                let text = ch.execute_raw(&cold_sql).await?;
                                                cold_rows = Some(parse_json_rows(
                                                    &text,
                                                    &columns,
                                                    &column_types,
                                                )?);
                                            } else {
                                                plan_failed = true;
                                            }
                                        }
                                        ClickhouseWireFormat::Other => {
                                            plan_failed = true;
                                        }
                                        ClickhouseWireFormat::Unknown => {
                                            if let Some(native) = &self.clickhouse_native {
                                                match native.query_rowset(&cold_sql).await {
                                                    Ok(rowset) => {
                                                        cold_rows = Some(align_rowset_columns(
                                                            rowset, &columns,
                                                        )?);
                                                    }
                                                    Err(_) => {
                                                        if let Some(ch) = &self.clickhouse {
                                                            let cold_sql =
                                                                ensure_csv_with_names(&cold_sql);
                                                            let text =
                                                                ch.execute_raw(&cold_sql).await?;
                                                            cold_rows = Some(parse_csv_rows(
                                                                &text,
                                                                &columns,
                                                                &column_types,
                                                            )?);
                                                        } else {
                                                            plan_failed = true;
                                                        }
                                                    }
                                                }
                                            } else if let Some(ch) = &self.clickhouse {
                                                let cold_sql = ensure_csv_with_names(&cold_sql);
                                                let text = ch.execute_raw(&cold_sql).await?;
                                                cold_rows = Some(parse_csv_rows(
                                                    &text,
                                                    &columns,
                                                    &column_types,
                                                )?);
                                            } else {
                                                plan_failed = true;
                                            }
                                        }
                                    }
                                } else {
                                    plan_failed = true;
                                }
                            }

                            if !plan_failed && (end_u > cutoff || !need_cold) {
                                let hot_lo = if need_cold {
                                    if start_u > cutoff {
                                        start_u
                                    } else {
                                        cutoff + 1
                                    }
                                } else {
                                    start_u
                                };
                                let mut select = plan.select.clone();
                                select.start = micros_to_system_time(hot_lo);
                                let rows = load_hot_rows(hot, &select, &plan.table).await?;
                                if !rows.is_empty() {
                                    hot_rows = Some(project_hot_rows(rows, &plan));
                                }
                            }
                        } else {
                            let rows = load_hot_rows(hot, &plan.select, &plan.table).await?;
                            if !rows.is_empty() {
                                hot_rows = Some(project_hot_rows(rows, &plan));
                            }
                        }

                        if !plan_failed && (cold_rows.is_some() || hot_rows.is_some()) {
                            let cold_sorted = plan.order_by.first().is_some();
                            let merged = merge_rows(cold_rows, hot_rows, &plan, cold_sorted)?;
                            let sliced = apply_offset_limit(merged, plan.offset, plan.limit);
                            return Ok(SqlResult::TypedRows(sliced));
                        }
                    }
                }
            }

            if let Some(ch) = &self.clickhouse {
                let wire_format = detect_wire_format(sql);
                let body = ch.execute_raw(sql).await?;
                let wire = if matches!(wire_format, ClickhouseWireFormat::JsonEachRow) {
                    ClickhouseWire::jsoneachrow(body)
                } else {
                    ClickhouseWire::with_unknown(body)
                };
                return Ok(SqlResult::Clickhouse(wire));
            }
        }

        if normalized.starts_with("create")
            || normalized.starts_with("insert")
            || normalized.starts_with("update")
            || normalized.starts_with("delete")
        {
            if let Some(pg) = &self.postgres {
                return pg.execute(sql).await;
            }
        }

        if let Some(sqlite) = &self.sqlite {
            return sqlite.execute(sql).await;
        }

        anyhow::bail!("no executor available for query")
    }
}

struct SqliteExecutor {
    store: SqliteMetadataStore,
}

impl SqliteExecutor {
    async fn connect(path: &str) -> anyhow::Result<Self> {
        let store = SqliteMetadataStore::connect(path).await?;
        Ok(Self { store })
    }
}

#[async_trait]
impl SqlExecutor for SqliteExecutor {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult> {
        let pool = self.store.pool();
        match sqlx::query(sql).fetch_all(pool).await {
            Ok(rows) => {
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut map = Map::new();
                    for col in row.columns() {
                        let name = col.name();
                        match row.try_get::<String, _>(name) {
                            Ok(v) => {
                                map.insert(name.to_string(), JsonValue::String(v));
                            }
                            Err(_) => {
                                map.insert(name.to_string(), JsonValue::Null);
                            }
                        }
                    }
                    out.push(map);
                }
                Ok(SqlResult::Rows(out))
            }
            Err(_) => {
                sqlx::query(sql).execute(pool).await?;
                Ok(SqlResult::Text("OK".into()))
            }
        }
    }
}

pub struct PostgresExecutor;

#[async_trait]
impl SqlExecutor for PostgresExecutor {
    async fn execute(&self, _sql: &str) -> anyhow::Result<SqlResult> {
        anyhow::bail!("postgres executor not enabled at compile time")
    }
}

impl PostgresExecutor {
    #[allow(unused_variables)]
    pub async fn maybe_connect(url: Option<&str>) -> anyhow::Result<Option<Self>> {
        #[cfg(feature = "postgres")]
        {
            if let Some(u) = url {
                let (_client, connection) =
                    tokio_postgres::connect(u, tokio_postgres::NoTls).await?;
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return Ok(Some(Self));
            }
            Ok(None)
        }
        #[cfg(not(feature = "postgres"))]
        {
            let _ = url;
            Ok(None)
        }
    }

    #[allow(dead_code)]
    pub async fn execute(&self, _sql: &str) -> anyhow::Result<SqlResult> {
        anyhow::bail!("postgres executor not enabled at compile time")
    }
}

#[derive(Clone)]
struct HotRowFlat {
    key0: Bytes,
    key1: Bytes,
    ord_micros: i64,
    values: Vec<Bytes>,
}

#[derive(Clone)]
struct SimpleSelect {
    key0: String,
    key1_list: Vec<String>,
    start: SystemTime,
    end: SystemTime,
}

#[derive(Clone)]
struct PlanAnalysis {
    table: String,
    cfg: TableConfig,
    select: SimpleSelect,
    order_by: Vec<OrderItem>,
    select_cols: Vec<String>,
    has_wildcard: bool,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
struct HotStats {
    first_micros: Option<i64>,
    last_micros: Option<i64>,
}

fn analyze_select(sql: &str, registry: &TableRegistry) -> Option<PlanAnalysis> {
    let plan = build_table_plan(sql)?;
    if !plan.supports_hot {
        return None;
    }

    let cfg = registry.table_config(&plan.table)?;

    let filter0 = cfg.filter_cols.get(0)?.to_ascii_lowercase();
    let filter1 = cfg.filter_cols.get(1)?.to_ascii_lowercase();
    let order_col = cfg.order_col.to_ascii_lowercase();

    let mut key0 = None;
    let mut key1_values = Vec::new();
    let mut lower: Option<i64> = None;
    let mut upper: Option<i64> = None;

    for predicate in &plan.filters {
        match predicate {
            Predicate::Eq { column, value } => {
                let col = column.to_ascii_lowercase();
                if col == filter0 {
                    key0 = Some(value.clone());
                } else if col == filter1 {
                    key1_values = vec![value.clone()];
                }
            }
            Predicate::In { column, values } => {
                if column.to_ascii_lowercase() == filter1 {
                    key1_values = values.clone();
                }
            }
            Predicate::RangeLower {
                column,
                inclusive,
                bound,
            } => {
                if column.to_ascii_lowercase() == order_col {
                    let adjusted = if *inclusive { *bound } else { bound + 1 };
                    lower = Some(lower.map_or(adjusted, |current| current.max(adjusted)));
                }
            }
            Predicate::RangeUpper {
                column,
                inclusive,
                bound,
            } => {
                if column.to_ascii_lowercase() == order_col {
                    let adjusted = if *inclusive { *bound } else { bound - 1 };
                    upper = Some(upper.map_or(adjusted, |current| current.min(adjusted)));
                }
            }
            Predicate::Between { column, low, high } => {
                if column.to_ascii_lowercase() == order_col {
                    lower = Some(lower.map_or(*low, |current| current.max(*low)));
                    upper = Some(upper.map_or(*high, |current| current.min(*high)));
                }
            }
        }
    }

    let key0 = key0?;
    if key1_values.is_empty() {
        return None;
    }

    let start = lower
        .map(micros_to_system_time)
        .unwrap_or_else(|| micros_to_system_time(i64::MIN));
    let end = upper
        .map(micros_to_system_time)
        .unwrap_or_else(|| micros_to_system_time(i64::MAX));

    if let Some(order) = plan.order_by.first() {
        if order.column != order_col {
            return None;
        }
    }

    let select_cols = plan.select_cols.clone();
    let has_wildcard = plan.has_wildcard;

    if !has_wildcard {
        if select_cols.is_empty() {
            return None;
        }
        for col in &select_cols {
            if !cfg.columns.contains(col) {
                return None;
            }
        }
        if !plan.order_by.is_empty() && !select_cols.contains(&order_col) {
            return None;
        }
    }

    Some(PlanAnalysis {
        table: plan.table,
        cfg,
        select: SimpleSelect {
            key0,
            key1_list: key1_values,
            start,
            end,
        },
        order_by: plan.order_by,
        select_cols,
        has_wildcard,
        offset: plan.offset,
        limit: plan.limit,
    })
}

async fn load_hot_rows(
    hot: &Arc<dyn HotColumnStore>,
    sel: &SimpleSelect,
    table: &str,
) -> anyhow::Result<Vec<HotRowFlat>> {
    let range = QueryRange::new(sel.start, sel.end);
    let key0_bytes = Bytes::copy_from_slice(sel.key0.as_bytes());
    let key1_list: Vec<Bytes> = sel
        .key1_list
        .iter()
        .map(|k| Bytes::copy_from_slice(k.as_bytes()))
        .collect();
    let rows = hot
        .scan_range(table, &key0_bytes, &range, &key1_list)
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(HotRowFlat {
            key0: key0_bytes.clone(),
            key1: row.key1,
            ord_micros: row.order_micros,
            values: row.values,
        });
    }
    Ok(out)
}

fn system_time_to_micros(ts: SystemTime) -> i64 {
    ts.duration_since(SystemTime::UNIX_EPOCH)
        .map(|dur| dur.as_micros() as i64)
        .unwrap_or_else(|err| {
            let dur = err.duration();
            -(dur.as_micros() as i64)
        })
}

fn micros_to_system_time(micros: i64) -> SystemTime {
    if micros >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_micros(micros as u64)
    } else {
        SystemTime::UNIX_EPOCH - Duration::from_micros((-micros) as u64)
    }
}

fn parse_csv_rows(
    text: &str,
    columns: &[String],
    column_types: &[ColumnType],
) -> anyhow::Result<RowSet> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    let headers = reader
        .headers()
        .map(|h| h.iter().map(|v| v.to_string()).collect::<Vec<_>>())?;
    let mut positions = Vec::with_capacity(columns.len());
    for col in columns {
        let idx = headers
            .iter()
            .position(|h| h.eq_ignore_ascii_case(col));
        positions.push(idx);
    }
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        let mut row = Vec::with_capacity(columns.len());
        for (pos_idx, idx) in positions.iter().enumerate() {
            let value = idx
                .and_then(|pos| record.get(pos))
                .unwrap_or("");
            let ty = column_types
                .get(pos_idx)
                .copied()
                .unwrap_or(ColumnType::Text);
            row.push(parse_scalar_value(value, ty));
        }
        rows.push(row);
    }
    Ok(RowSet {
        columns: columns.to_vec(),
        column_types: column_types.to_vec(),
        rows,
    })
}

fn parse_json_rows(
    text: &str,
    columns: &[String],
    column_types: &[ColumnType],
) -> anyhow::Result<RowSet> {
    let mut rows = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: JsonValue = serde_json::from_str(trimmed)?;
        match value {
            JsonValue::Object(map) => {
                let mut row = Vec::with_capacity(columns.len());
                for (idx, col) in columns.iter().enumerate() {
                    let value = map.get(col).cloned().unwrap_or(JsonValue::Null);
                    let ty = column_types
                        .get(idx)
                        .copied()
                        .unwrap_or(ColumnType::Text);
                    row.push(json_to_scalar(value, ty));
                }
                rows.push(row);
            }
            other => {
                let mut row = Vec::with_capacity(columns.len());
                if columns.len() == 1 {
                    let ty = column_types
                        .get(0)
                        .copied()
                        .unwrap_or(ColumnType::Text);
                    row.push(json_to_scalar(other, ty));
                } else {
                    row.resize(columns.len(), ScalarValue::Null);
                }
                rows.push(row);
            }
        }
    }
    Ok(RowSet {
        columns: columns.to_vec(),
        column_types: column_types.to_vec(),
        rows,
    })
}

fn bytes_to_scalar(value: &Bytes, ty: ColumnType) -> ScalarValue {
    if value.is_empty() {
        return ScalarValue::Null;
    }
    let text = unsafe { std::str::from_utf8_unchecked(value) };
    match ty {
        ColumnType::Int64 => text
            .parse::<i64>()
            .map(ScalarValue::Int64)
            .unwrap_or_else(|_| ScalarValue::String(text.to_string())),
        ColumnType::Float64 => text
            .parse::<f64>()
            .map(ScalarValue::Float64)
            .unwrap_or_else(|_| ScalarValue::String(text.to_string())),
        ColumnType::Bool => {
            if text.eq_ignore_ascii_case("true") {
                ScalarValue::Bool(true)
            } else if text.eq_ignore_ascii_case("false") {
                ScalarValue::Bool(false)
            } else {
                ScalarValue::String(text.to_string())
            }
        }
        ColumnType::Text => ScalarValue::String(text.to_string()),
    }
}

fn parse_scalar_value(value: &str, ty: ColumnType) -> ScalarValue {
    if value.is_empty() {
        return ScalarValue::Null;
    }
    match ty {
        ColumnType::Int64 => value
            .parse::<i64>()
            .map(ScalarValue::Int64)
            .unwrap_or_else(|_| ScalarValue::String(value.to_string())),
        ColumnType::Float64 => value
            .parse::<f64>()
            .map(ScalarValue::Float64)
            .unwrap_or_else(|_| ScalarValue::String(value.to_string())),
        ColumnType::Bool => {
            if value.eq_ignore_ascii_case("true") {
                ScalarValue::Bool(true)
            } else if value.eq_ignore_ascii_case("false") {
                ScalarValue::Bool(false)
            } else {
                ScalarValue::String(value.to_string())
            }
        }
        ColumnType::Text => ScalarValue::String(value.to_string()),
    }
}

fn json_to_scalar(value: JsonValue, ty: ColumnType) -> ScalarValue {
    match value {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::Bool(v) => match ty {
            ColumnType::Bool => ScalarValue::Bool(v),
            _ => ScalarValue::String(v.to_string()),
        },
        JsonValue::Number(v) => match ty {
            ColumnType::Float64 => v
                .as_f64()
                .map(ScalarValue::Float64)
                .unwrap_or(ScalarValue::Null),
            ColumnType::Int64 => v
                .as_i64()
                .map(ScalarValue::Int64)
                .or_else(|| v.as_f64().map(|f| ScalarValue::Int64(f as i64)))
                .unwrap_or(ScalarValue::Null),
            _ => ScalarValue::String(v.to_string()),
        },
        JsonValue::String(v) => match ty {
            ColumnType::Int64 => v
                .parse::<i64>()
                .map(ScalarValue::Int64)
                .unwrap_or_else(|_| ScalarValue::String(v)),
            ColumnType::Float64 => v
                .parse::<f64>()
                .map(ScalarValue::Float64)
                .unwrap_or_else(|_| ScalarValue::String(v)),
            ColumnType::Bool => {
                if v.eq_ignore_ascii_case("true") {
                    ScalarValue::Bool(true)
                } else if v.eq_ignore_ascii_case("false") {
                    ScalarValue::Bool(false)
                } else {
                    ScalarValue::String(v)
                }
            }
            _ => ScalarValue::String(v),
        },
        JsonValue::Array(_) | JsonValue::Object(_) => ScalarValue::String(value.to_string()),
    }
}


fn project_hot_rows(rows: Vec<HotRowFlat>, plan: &PlanAnalysis) -> RowSet {
    let columns = expected_columns(plan);
    let column_types = expected_column_types(plan);
    let mut indices = Vec::with_capacity(columns.len());
    for col in &columns {
        let idx = plan
            .cfg
            .columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case(col));
        indices.push(idx);
    }

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut row_values = Vec::with_capacity(columns.len());
        for (pos, col) in indices.iter().enumerate() {
            let ty = column_types
                .get(pos)
                .copied()
                .unwrap_or(ColumnType::Text);
            let value = col
                .and_then(|idx| row.values.get(idx))
                .map(|v| bytes_to_scalar(v, ty))
                .unwrap_or(ScalarValue::Null);
            row_values.push(value);
        }
        out.push(row_values);
    }
    RowSet {
        columns,
        column_types,
        rows: out,
    }
}

fn expected_columns(plan: &PlanAnalysis) -> Vec<String> {
    if plan.has_wildcard || plan.select_cols.is_empty() {
        plan.cfg.columns.clone()
    } else {
        plan.select_cols.clone()
    }
}

fn expected_column_types(plan: &PlanAnalysis) -> Vec<ColumnType> {
    let columns = expected_columns(plan);
    columns
        .iter()
        .map(|col| {
            plan.cfg
                .columns
                .iter()
                .position(|name| name.eq_ignore_ascii_case(col))
                .and_then(|idx| plan.cfg.column_types.get(idx).copied())
                .unwrap_or(ColumnType::Text)
        })
        .collect()
}

fn align_rowset_columns(mut rowset: RowSet, columns: &[String]) -> anyhow::Result<RowSet> {
    if rowset.columns.is_empty() {
        rowset.columns = columns.to_vec();
        if rowset.column_types.len() != columns.len() {
            rowset.column_types = vec![ColumnType::Text; columns.len()];
        }
        return Ok(rowset);
    }
    if rowset.columns == columns {
        return Ok(rowset);
    }
    let mut indices = Vec::with_capacity(columns.len());
    for col in columns {
        let idx = rowset
            .columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case(col));
        indices.push(idx);
    }
    let mut types = Vec::with_capacity(columns.len());
    for idx in &indices {
        let ty = idx
            .and_then(|pos| rowset.column_types.get(pos).copied())
            .unwrap_or(ColumnType::Text);
        types.push(ty);
    }
    let mut rows = Vec::with_capacity(rowset.rows.len());
    for row in rowset.rows.into_iter() {
        let mut aligned = Vec::with_capacity(columns.len());
        for idx in &indices {
            let value = idx
                .and_then(|pos| row.get(pos).cloned())
                .unwrap_or(ScalarValue::Null);
            aligned.push(value);
        }
        rows.push(aligned);
    }
    Ok(RowSet {
        columns: columns.to_vec(),
        column_types: types,
        rows,
    })
}

fn merge_rows(
    cold: Option<RowSet>,
    hot: Option<RowSet>,
    plan: &PlanAnalysis,
    cold_sorted: bool,
) -> anyhow::Result<RowSet> {
    let expected = expected_columns(plan);
    let expected_types = expected_column_types(plan);
    let mut cold = cold.unwrap_or_else(|| RowSet {
        columns: expected.clone(),
        column_types: expected_types.clone(),
        rows: Vec::new(),
    });
    let mut hot = hot.unwrap_or_else(|| RowSet {
        columns: expected.clone(),
        column_types: expected_types.clone(),
        rows: Vec::new(),
    });

    if cold.columns.is_empty() {
        cold.columns = expected.clone();
        cold.column_types = expected_types.clone();
    }
    if hot.columns.is_empty() {
        hot.columns = expected.clone();
        hot.column_types = expected_types.clone();
    }
    if cold.columns == expected && cold.column_types != expected_types {
        cold.column_types = expected_types.clone();
    }
    if hot.columns == expected && hot.column_types != expected_types {
        hot.column_types = expected_types.clone();
    }
    if cold.columns != hot.columns || cold.column_types != hot.column_types {
        anyhow::bail!("hot/cold column mismatch");
    }

    if let Some(order) = plan.order_by.first() {
        let descending = order.descending;
        let order_idx = cold
            .columns
            .iter()
            .position(|col| col == &plan.cfg.order_col)
            .ok_or_else(|| anyhow!("missing order column in result set"))?;

        if !cold_sorted {
            cold.rows.sort_by(|a, b| {
                let ka = extract_order_key(a, order_idx).unwrap_or_default();
                let kb = extract_order_key(b, order_idx).unwrap_or_default();
                if descending {
                    kb.cmp(&ka)
                } else {
                    ka.cmp(&kb)
                }
            });
        }

        hot.rows.sort_by(|a, b| {
            let ka = extract_order_key(a, order_idx).unwrap_or_default();
            let kb = extract_order_key(b, order_idx).unwrap_or_default();
            if descending {
                kb.cmp(&ka)
            } else {
                ka.cmp(&kb)
            }
        });

        let mut merged = Vec::with_capacity(cold.rows.len() + hot.rows.len());
        let mut i = 0;
        let mut j = 0;
        while i < cold.rows.len() && j < hot.rows.len() {
            let kc = extract_order_key(&cold.rows[i], order_idx)?;
            let kh = extract_order_key(&hot.rows[j], order_idx)?;
            let take_cold = if descending { kc >= kh } else { kc <= kh };
            if take_cold {
                merged.push(cold.rows[i].clone());
                i += 1;
            } else {
                merged.push(hot.rows[j].clone());
                j += 1;
            }
        }
        merged.extend_from_slice(&cold.rows[i..]);
        merged.extend_from_slice(&hot.rows[j..]);
        Ok(RowSet {
            columns: cold.columns,
            column_types: cold.column_types,
            rows: merged,
        })
    } else {
        let mut merged = cold.rows;
        merged.extend(hot.rows.into_iter());
        Ok(RowSet {
            columns: cold.columns,
            column_types: cold.column_types,
            rows: merged,
        })
    }
}

fn apply_offset_limit(rows: RowSet, offset: Option<usize>, limit: Option<usize>) -> RowSet {
    let iter = rows.rows.into_iter().skip(offset.unwrap_or(0));
    let sliced = if let Some(limit) = limit {
        iter.take(limit).collect()
    } else {
        iter.collect()
    };
    RowSet {
        columns: rows.columns,
        column_types: rows.column_types,
        rows: sliced,
    }
}

fn extract_order_key(row: &[ScalarValue], index: usize) -> anyhow::Result<i64> {
    let value = row
        .get(index)
        .ok_or_else(|| anyhow!("missing order column in result row"))?;
    match value {
        ScalarValue::Int64(v) => Ok(*v),
        ScalarValue::Float64(v) => Ok(*v as i64),
        ScalarValue::String(s) => s
            .parse::<i64>()
            .map_err(|_| anyhow!("unable to parse order column value as integer")),
        _ => Err(anyhow!("unsupported order column type")),
    }
}

fn detect_wire_format(sql: &str) -> ClickhouseWireFormat {
    let lower = sql.to_ascii_lowercase();
    if let Some(idx) = lower.rfind("format") {
        let mut tail = lower[idx + "format".len()..].trim_start();
        if let Some(end) = tail.find(';') {
            tail = &tail[..end];
        }
        for token in tail.split_whitespace() {
            if token.is_empty() {
                continue;
            }
            if token == "jsoneachrow" {
                return ClickhouseWireFormat::JsonEachRow;
            }
            return ClickhouseWireFormat::Other;
        }
    }
    ClickhouseWireFormat::Unknown
}

fn ensure_jsoneachrow(sql: &str) -> String {
    match detect_wire_format(sql) {
        ClickhouseWireFormat::JsonEachRow => sql.to_string(),
        ClickhouseWireFormat::Other => sql.to_string(),
        ClickhouseWireFormat::Unknown => {
            if let Some(stripped) = sql.strip_suffix(';') {
                format!("{} FORMAT JSONEachRow;", stripped.trim_end())
            } else {
                format!("{} FORMAT JSONEachRow", sql.trim_end())
            }
        }
    }
}

fn ensure_csv_with_names(sql: &str) -> String {
    match detect_wire_format(sql) {
        ClickhouseWireFormat::JsonEachRow => sql.to_string(),
        ClickhouseWireFormat::Other => sql.to_string(),
        ClickhouseWireFormat::Unknown => {
            if let Some(stripped) = sql.strip_suffix(';') {
                format!("{} FORMAT CSVWithNames;", stripped.trim_end())
            } else {
                format!("{} FORMAT CSVWithNames", sql.trim_end())
            }
        }
    }
}

struct TableAnnotation {
    order_col: String,
    filter_cols: Vec<String>,
    seq_col: Option<String>,
}

fn parse_proxist_table_config(sql: &str) -> anyhow::Result<Option<TableAnnotation>> {
    let lower = sql.to_ascii_lowercase();
    let Some(idx) = lower.find("proxist:") else {
        return Ok(None);
    };
    let tail = &sql[idx + "proxist:".len()..];
    let end = tail
        .find('\n')
        .or_else(|| tail.find("*/"))
        .unwrap_or(tail.len());
    let body = tail[..end].trim();

    let mut order_col = None;
    let mut filter_cols = Vec::new();
    let mut seq_col = None;

    for part in body.split(|c| c == ',' || c == ';') {
        let mut kv = part.split('=').map(|s| s.trim());
        let key = kv.next().unwrap_or("").to_ascii_lowercase();
        let value = kv.next().unwrap_or("");
        match key.as_str() {
            "order_col" => order_col = Some(value.to_string()),
            "filter_cols" => {
                filter_cols = value
                    .split(|c| c == ',' || c == ' ')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
            }
            "seq_col" => seq_col = Some(value.to_string()),
            _ => {}
        }
    }

    let order_col = order_col.ok_or_else(|| anyhow!("proxist annotation missing order_col"))?;
    if filter_cols.len() < 2 {
        anyhow::bail!(
            "proxist annotation must specify at least two filter_cols (group key and entity key)"
        );
    }

    Ok(Some(TableAnnotation {
        order_col,
        filter_cols,
        seq_col,
    }))
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;

    #[test]
    fn detect_wire_format_identifies_json() {
        let sql = "SELECT * FROM foo FORMAT JSONEachRow";
        assert!(matches!(
            detect_wire_format(sql),
            ClickhouseWireFormat::JsonEachRow
        ));
    }

    #[test]
    fn detect_wire_format_handles_absent_format() {
        let sql = "SELECT * FROM foo";
        assert!(matches!(
            detect_wire_format(sql),
            ClickhouseWireFormat::Unknown
        ));
    }

    #[test]
    fn detect_wire_format_handles_other_format() {
        let sql = "SELECT * FROM foo FORMAT CSV";
        assert!(matches!(
            detect_wire_format(sql),
            ClickhouseWireFormat::Other
        ));
    }

    #[tokio::test]
    async fn hot_stats_cover_window() {
        let scheduler = ProxistScheduler {
            sqlite: None,
            clickhouse: None,
            clickhouse_native: None,
            postgres: None,
            hot_store: None,
            registry: Arc::new(TableRegistry::new()),
            persisted_cutoff_micros: AtomicI64::new(-1),
            hot_stats: Arc::new(RwLock::new(HashMap::new())),
        };

        let stats = vec![
            HotSymbolSummary {
                table: "ticks".to_string(),
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"SYM1"),
                rows: 10,
                first_micros: Some(100),
                last_micros: Some(200),
            },
            HotSymbolSummary {
                table: "ticks".to_string(),
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"SYM2"),
                rows: 10,
                first_micros: Some(100),
                last_micros: Some(200),
            },
        ];

        scheduler.update_hot_stats(&stats).await;

        assert!(
            scheduler
                .hot_covers_window("alpha", &vec!["SYM1".to_string()], 120, 180)
                .await
        );
        assert!(
            !scheduler
                .hot_covers_window("alpha", &vec!["SYM1".to_string()], 90, 180)
                .await
        );
        assert!(
            !scheduler
                .hot_covers_window("alpha", &vec!["SYM3".to_string()], 120, 180)
                .await
        );
    }

    #[test]
    fn project_hot_rows_projects_columns() {
        let cfg = TableConfig {
            order_col: "ts_micros".to_string(),
            filter_cols: vec!["tenant".to_string(), "symbol".to_string()],
            seq_col: Some("seq".to_string()),
            columns: vec![
                "tenant".to_string(),
                "symbol".to_string(),
                "ts_micros".to_string(),
                "payload".to_string(),
                "seq".to_string(),
            ],
            column_types: vec![
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Int64,
                ColumnType::Text,
                ColumnType::Int64,
            ],
        };
        let plan = PlanAnalysis {
            table: "ticks".to_string(),
            cfg,
            select: SimpleSelect {
                key0: "alpha".to_string(),
                key1_list: vec!["SYM1".to_string()],
                start: SystemTime::UNIX_EPOCH,
                end: SystemTime::UNIX_EPOCH,
            },
            order_by: vec![OrderItem {
                column: "ts_micros".to_string(),
                descending: false,
            }],
            select_cols: vec!["symbol".to_string(), "ts_micros".to_string()],
            has_wildcard: false,
            offset: None,
            limit: None,
        };

        let rows = vec![
            HotRowFlat {
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"SYM1"),
                ord_micros: 2,
                values: vec![
                    Bytes::copy_from_slice(b"alpha"),
                    Bytes::copy_from_slice(b"SYM1"),
                    Bytes::copy_from_slice(b"2"),
                    Bytes::copy_from_slice(b"b"),
                    Bytes::copy_from_slice(b"1"),
                ],
            },
            HotRowFlat {
                key0: Bytes::copy_from_slice(b"alpha"),
                key1: Bytes::copy_from_slice(b"SYM1"),
                ord_micros: 1,
                values: vec![
                    Bytes::copy_from_slice(b"alpha"),
                    Bytes::copy_from_slice(b"SYM1"),
                    Bytes::copy_from_slice(b"1"),
                    Bytes::copy_from_slice(b"a"),
                    Bytes::copy_from_slice(b"0"),
                ],
            },
        ];

        let projected = project_hot_rows(rows, &plan);
        assert_eq!(projected.rows.len(), 2);
        assert_eq!(
            projected.columns,
            vec!["symbol".to_string(), "ts_micros".to_string()]
        );
        let first = &projected.rows[0];
        assert_eq!(first[0], ScalarValue::String("SYM1".to_string()));
        assert_eq!(first[1], ScalarValue::Int64(2));
    }

    #[test]
    fn parse_csv_rows_with_names() {
        let input = "symbol,ts_micros\nSYM1,42\nSYM2,43\n";
        let columns = vec!["symbol".to_string(), "ts_micros".to_string()];
        let types = vec![ColumnType::Text, ColumnType::Int64];
        let rows = parse_csv_rows(input, &columns, &types).expect("parse csv");
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.columns, columns);
        assert_eq!(rows.column_types, types);
        assert_eq!(rows.rows[0][0], ScalarValue::String("SYM1".to_string()));
        assert_eq!(rows.rows[0][1], ScalarValue::Int64(42));
    }

    #[test]
    fn merge_rows_keeps_cold_order_and_sorts_hot() {
        let plan = PlanAnalysis {
            table: "ticks".to_string(),
            cfg: TableConfig {
                order_col: "ts_micros".to_string(),
                filter_cols: vec!["tenant".to_string(), "symbol".to_string()],
                seq_col: Some("seq".to_string()),
                columns: vec![
                    "symbol".to_string(),
                    "ts_micros".to_string(),
                    "payload".to_string(),
                ],
                column_types: vec![
                    ColumnType::Text,
                    ColumnType::Int64,
                    ColumnType::Text,
                ],
            },
            select: SimpleSelect {
                key0: "alpha".to_string(),
                key1_list: vec!["SYM1".to_string()],
                start: SystemTime::UNIX_EPOCH,
                end: SystemTime::UNIX_EPOCH,
            },
            order_by: vec![OrderItem {
                column: "ts_micros".to_string(),
                descending: false,
            }],
            select_cols: vec![],
            has_wildcard: true,
            offset: None,
            limit: None,
        };

        let columns = vec![
            "symbol".to_string(),
            "ts_micros".to_string(),
            "payload".to_string(),
        ];
        let types = vec![ColumnType::Text, ColumnType::Int64, ColumnType::Text];
        let cold = RowSet {
            columns: columns.clone(),
            column_types: types.clone(),
            rows: vec![
                vec![
                    ScalarValue::String("SYM1".to_string()),
                    ScalarValue::Int64(1),
                    ScalarValue::String("a".to_string()),
                ],
                vec![
                    ScalarValue::String("SYM1".to_string()),
                    ScalarValue::Int64(3),
                    ScalarValue::String("c".to_string()),
                ],
            ],
        };
        let hot = RowSet {
            columns,
            column_types: types,
            rows: vec![
                vec![
                    ScalarValue::String("SYM1".to_string()),
                    ScalarValue::Int64(4),
                    ScalarValue::String("d".to_string()),
                ],
                vec![
                    ScalarValue::String("SYM1".to_string()),
                    ScalarValue::Int64(2),
                    ScalarValue::String("b".to_string()),
                ],
            ],
        };

        let merged = merge_rows(Some(cold), Some(hot), &plan, true).expect("merge");
        let ords: Vec<i64> = merged
            .rows
            .iter()
            .map(|row| match row[1] {
                ScalarValue::Int64(v) => v,
                _ => 0,
            })
            .collect();
        assert_eq!(ords, vec![1, 2, 3, 4]);
    }
}
