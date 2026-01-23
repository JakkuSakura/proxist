use crate::clickhouse::ClickhouseHttpClient;
use crate::metadata_sqlite::SqliteMetadataStore;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use base64::Engine as _;
mod plan;
use plan::{
    build_table_plan, parse_table_schema_from_ddl, rewrite_with_bounds, strip_limit_offset,
    OrderItem, Predicate,
};
use proxist_core::query::QueryRange;
use proxist_mem::{HotColumnStore, HotSymbolSummary};
#[cfg(feature = "duckdb")]
use serde_json::Number as JsonNumber;
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
    pub duckdb_path: Option<String>,
    pub pg_url: Option<String>,
}

#[async_trait]
pub trait SqlExecutor: Send + Sync {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickhouseWireFormat {
    JsonEachRow,
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
}

#[cfg_attr(not(feature = "duckdb"), allow(dead_code))]
#[derive(Debug, Clone)]
pub struct TableConfig {
    pub ddl: String,
    pub order_col: String,
    pub payload_col: String,
    pub filter_cols: Vec<String>,
    pub seq_col: Option<String>,
    pub columns: Vec<String>,
}

pub struct ProxistScheduler {
    sqlite: Option<SqliteExecutor>,
    clickhouse: Option<ClickhouseHttpClient>,
    duckdb: Option<DuckDbExecutor>,
    postgres: Option<PostgresExecutor>,
    hot_store: Option<Arc<dyn HotColumnStore>>,
    tables: Arc<Mutex<HashMap<String, TableConfig>>>,
    persisted_cutoff_micros: AtomicI64,
    hot_stats: Arc<RwLock<HashMap<(String, String), HotStats>>>,
}

impl ProxistScheduler {
    pub async fn new(
        cfg: ExecutorConfig,
        clickhouse: Option<ClickhouseHttpClient>,
        hot_store: Option<Arc<dyn HotColumnStore>>,
    ) -> anyhow::Result<Self> {
        let sqlite = match cfg.sqlite_path {
            Some(path) => Some(
                SqliteExecutor::connect(&path)
                    .await
                    .context("connect sqlite")?,
            ),
            None => None,
        };
        let duckdb = DuckDbExecutor::maybe_connect(cfg.duckdb_path.as_deref()).await?;
        let postgres = PostgresExecutor::maybe_connect(cfg.pg_url.as_deref()).await?;

        Ok(Self {
            sqlite,
            clickhouse,
            duckdb,
            postgres,
            hot_store,
            tables: Arc::new(Mutex::new(HashMap::new())),
            persisted_cutoff_micros: AtomicI64::new(-1),
            hot_stats: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn register_table(&self, table: impl Into<String>, cfg: TableConfig) {
        let key = table.into().to_ascii_lowercase();
        self.tables.lock().unwrap().insert(key, cfg);
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
        self.register_table(
            table,
            TableConfig {
                ddl: ddl_sql.to_string(),
                order_col: annotation.order_col,
                payload_col: annotation.payload_col,
                filter_cols: annotation.filter_cols,
                seq_col: annotation.seq_col,
                columns,
            },
        );
        Ok(())
    }

    pub fn table_config(&self, table: &str) -> Option<TableConfig> {
        let key = table.to_ascii_lowercase();
        self.tables.lock().unwrap().get(&key).cloned()
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
            map.insert(
                (entry.tenant.clone(), entry.symbol.clone()),
                HotStats {
                    first_micros: entry.first_timestamp.map(system_time_to_micros),
                    last_micros: entry.last_timestamp.map(system_time_to_micros),
                },
            );
        }
    }

    #[cfg(feature = "duckdb")]
    pub fn clear_duckdb_table(&self, table: &str) -> anyhow::Result<()> {
        let sanitized = table.replace('"', "");
        unsafe {
            let exec = DUCK_EXEC
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("duckdb connection not initialized"))?;
            let sql = format!("DELETE FROM \"{}\"", sanitized);
            let _ = exec.conn.execute(&sql, []);
        }
        Ok(())
    }

    #[cfg(not(feature = "duckdb"))]
    pub fn clear_duckdb_table(&self, _table: &str) -> anyhow::Result<()> {
        anyhow::bail!("duckdb executor not enabled at compile time")
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
            if let (Some(duck), Some(hot)) = (&self.duckdb, &self.hot_store) {
                let plan = {
                    let guard = self.tables.lock().unwrap();
                    analyze_select(sql, &*guard)
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

                        if let Some(hot_sql) = strip_limit_offset(sql, limit_hint) {
                            let cutoff = self.persisted_cutoff_micros.load(Ordering::SeqCst);
                            let mut cold_rows = Vec::new();
                            let mut hot_rows = Vec::new();
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
                                        if let Some(ch) = &self.clickhouse {
                                            let text = ch.execute_raw(&cold_sql).await?;
                                            cold_rows = parse_json_rows(&text)?;
                                        } else {
                                            plan_failed = true;
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
                                    let rows = load_hot_rows(hot, &select).await?;
                                    if !rows.is_empty() {
                                        hot_rows = duck.load_table_and_execute(
                                            &plan.cfg,
                                            &select.table,
                                            &rows,
                                            &hot_sql,
                                        )?;
                                    }
                                }
                            } else {
                                let rows = load_hot_rows(hot, &plan.select).await?;
                                if !rows.is_empty() {
                                    hot_rows = duck.load_table_and_execute(
                                        &plan.cfg,
                                        &plan.select.table,
                                        &rows,
                                        &hot_sql,
                                    )?;
                                }
                            }

                            if !plan_failed && (!cold_rows.is_empty() || !hot_rows.is_empty()) {
                                let merged = merge_rows(cold_rows, hot_rows, &plan)?;
                                let wire = format_json_rows_as_clickhouse(
                                    &merged,
                                    plan.offset,
                                    plan.limit,
                                    ClickhouseWireFormat::JsonEachRow,
                                );
                                return Ok(SqlResult::Clickhouse(wire));
                            }
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

struct DuckDbExecutor;

#[async_trait]
impl SqlExecutor for DuckDbExecutor {
    async fn execute(&self, _sql: &str) -> anyhow::Result<SqlResult> {
        anyhow::bail!("duckdb executor not enabled at compile time")
    }
}

#[cfg(feature = "duckdb")]
struct DuckDbExecutorImpl {
    conn: duckdb::Connection,
}

#[cfg(feature = "duckdb")]
static mut DUCK_EXEC: Option<DuckDbExecutorImpl> = None;

#[cfg(feature = "duckdb")]
impl DuckDbExecutorImpl {
    fn new(path: &str) -> anyhow::Result<Self> {
        Ok(Self {
            conn: duckdb::Connection::open(path)?,
        })
    }

    fn ensure_table(&mut self, ddl: &str) -> anyhow::Result<()> {
        self.conn.execute(ddl, [])?;
        Ok(())
    }

    fn insert_rows(
        &mut self,
        table: &str,
        cfg: &TableConfig,
        rows: &[HotRowFlat],
    ) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        let mut cols = Vec::new();
        if let Some(c0) = cfg.filter_cols.get(0) {
            cols.push(c0.as_str());
        }
        if let Some(c1) = cfg.filter_cols.get(1) {
            cols.push(c1.as_str());
        }
        cols.push(&cfg.order_col);
        cols.push(&cfg.payload_col);
        if let Some(seq) = &cfg.seq_col {
            cols.push(seq.as_str());
        }

        let placeholders: Vec<_> = (1..=cols.len()).map(|idx| format!("?{}", idx)).collect();
        let sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            table.replace('"', ""),
            cols.iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", "),
        );

        let mut stmt = tx.prepare(&sql)?;
        for row in rows {
            if cfg.seq_col.is_some() {
                stmt.execute(duckdb::params![
                    &row.k0,
                    &row.k1,
                    &row.ord_micros,
                    &row.payload_base64,
                    &row.seq
                ])?;
            } else {
                stmt.execute(duckdb::params![
                    &row.k0,
                    &row.k1,
                    &row.ord_micros,
                    &row.payload_base64
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn query_rows(&self, sql: &str) -> anyhow::Result<Vec<JsonValue>> {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        let column_names: Vec<String> = rows
            .as_ref()
            .map(|stmt| {
                (0..stmt.column_count())
                    .map(|idx| {
                        stmt.column_name(idx)
                            .map(|name| name.to_string())
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let mut map = Map::new();
            for (idx, name) in column_names.iter().enumerate() {
                if let Ok(v) = row.get::<usize, String>(idx) {
                    map.insert(name.clone(), JsonValue::String(v));
                } else if let Ok(v) = row.get::<usize, i64>(idx) {
                    map.insert(name.clone(), JsonValue::Number(v.into()));
                } else if let Ok(v) = row.get::<usize, f64>(idx) {
                    if let Some(num) = JsonNumber::from_f64(v) {
                        map.insert(name.clone(), JsonValue::Number(num));
                    } else {
                        map.insert(name.clone(), JsonValue::Null);
                    }
                } else {
                    map.insert(name.clone(), JsonValue::Null);
                }
            }
            out.push(JsonValue::Object(map));
        }
        Ok(out)
    }
}

#[cfg(feature = "duckdb")]
impl DuckDbExecutor {
    fn load_table_and_execute(
        &self,
        cfg: &TableConfig,
        table: &str,
        rows: &[HotRowFlat],
        sql: &str,
    ) -> anyhow::Result<Vec<JsonValue>> {
        unsafe {
            let exec = DUCK_EXEC
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("duckdb connection not initialized"))?;
            exec.ensure_table(&cfg.ddl)?;
            exec.insert_rows(table, cfg, rows)?;
            exec.query_rows(sql)
        }
    }

    async fn maybe_connect(path: Option<&str>) -> anyhow::Result<Option<Self>> {
        if let Some(p) = path {
            unsafe {
                DUCK_EXEC = Some(DuckDbExecutorImpl::new(p)?);
            }
            Ok(Some(Self))
        } else {
            Ok(None)
        }
    }
}

#[cfg(not(feature = "duckdb"))]
impl DuckDbExecutor {
    fn load_table_and_execute(
        &self,
        _cfg: &TableConfig,
        _table: &str,
        _rows: &[HotRowFlat],
        _sql: &str,
    ) -> anyhow::Result<Vec<JsonValue>> {
        anyhow::bail!("duckdb executor not enabled at compile time")
    }

    async fn maybe_connect(_path: Option<&str>) -> anyhow::Result<Option<Self>> {
        Ok(None)
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

#[cfg_attr(not(feature = "duckdb"), allow(dead_code))]
#[derive(Clone)]
struct HotRowFlat {
    k0: String,
    k1: String,
    ord_micros: i64,
    payload_base64: String,
    seq: i64,
}

#[derive(Clone)]
struct SimpleSelect {
    table: String,
    key0: String,
    key1_list: Vec<String>,
    start: SystemTime,
    end: SystemTime,
}

#[derive(Clone)]
struct PlanAnalysis {
    cfg: TableConfig,
    select: SimpleSelect,
    order_by: Vec<OrderItem>,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
struct HotStats {
    first_micros: Option<i64>,
    last_micros: Option<i64>,
}

fn analyze_select(sql: &str, tables: &HashMap<String, TableConfig>) -> Option<PlanAnalysis> {
    let plan = build_table_plan(sql)?;
    if !plan.supports_hot {
        return None;
    }

    let cfg = tables.get(&plan.table)?.clone();

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

    Some(PlanAnalysis {
        cfg,
        select: SimpleSelect {
            table: plan.table,
            key0,
            key1_list: key1_values,
            start,
            end,
        },
        order_by: plan.order_by,
        offset: plan.offset,
        limit: plan.limit,
    })
}

async fn load_hot_rows(
    hot: &Arc<dyn HotColumnStore>,
    sel: &SimpleSelect,
) -> anyhow::Result<Vec<HotRowFlat>> {
    let range = QueryRange::new(sel.start, sel.end);
    let rows = hot.scan_range(&sel.key0, &range, &sel.key1_list).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(HotRowFlat {
            k0: sel.key0.clone(),
            k1: row.symbol,
            ord_micros: system_time_to_micros(row.timestamp),
            payload_base64: base64::engine::general_purpose::STANDARD.encode(&row.payload),
            seq: 0,
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

fn parse_json_rows(text: &str) -> anyhow::Result<Vec<JsonValue>> {
    let mut rows = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        rows.push(serde_json::from_str(trimmed)?);
    }
    Ok(rows)
}


fn merge_rows(
    mut cold: Vec<JsonValue>,
    mut hot: Vec<JsonValue>,
    plan: &PlanAnalysis,
) -> anyhow::Result<Vec<JsonValue>> {
    if let Some(order) = plan.order_by.first() {
        let descending = order.descending;
        let column = &plan.cfg.order_col;

        cold.sort_by(|a, b| {
            let ka = extract_order_key(a, column).unwrap_or_default();
            let kb = extract_order_key(b, column).unwrap_or_default();
            if descending {
                kb.cmp(&ka)
            } else {
                ka.cmp(&kb)
            }
        });

        hot.sort_by(|a, b| {
            let ka = extract_order_key(a, column).unwrap_or_default();
            let kb = extract_order_key(b, column).unwrap_or_default();
            if descending {
                kb.cmp(&ka)
            } else {
                ka.cmp(&kb)
            }
        });

        let mut merged = Vec::with_capacity(cold.len() + hot.len());
        let mut i = 0;
        let mut j = 0;
        while i < cold.len() && j < hot.len() {
            let kc = extract_order_key(&cold[i], column)?;
            let kh = extract_order_key(&hot[j], column)?;
            let take_cold = if descending { kc >= kh } else { kc <= kh };
            if take_cold {
                merged.push(cold[i].clone());
                i += 1;
            } else {
                merged.push(hot[j].clone());
                j += 1;
            }
        }
        merged.extend_from_slice(&cold[i..]);
        merged.extend_from_slice(&hot[j..]);
        Ok(merged)
    } else {
        cold.extend(hot.into_iter());
        Ok(cold)
    }
}

fn format_json_rows_as_clickhouse(
    rows: &[JsonValue],
    offset: Option<usize>,
    limit: Option<usize>,
    format: ClickhouseWireFormat,
) -> ClickhouseWire {
    let iter = rows.iter().skip(offset.unwrap_or(0));
    let rows: Vec<_> = if let Some(limit) = limit {
        iter.take(limit).cloned().collect()
    } else {
        iter.cloned().collect()
    };

    let mut body = String::new();
    for value in rows {
        if let Ok(line) = serde_json::to_string(&value) {
            body.push_str(&line);
            body.push('\n');
        }
    }
    match format {
        ClickhouseWireFormat::JsonEachRow => ClickhouseWire::jsoneachrow(body),
        ClickhouseWireFormat::Unknown => ClickhouseWire::with_unknown(body),
    }
}

fn extract_order_key(row: &JsonValue, column: &str) -> anyhow::Result<i64> {
    let value = row
        .get(column)
        .ok_or_else(|| anyhow!("missing order column {} in result row", column))?;
    match value {
        JsonValue::Number(num) => num
            .as_i64()
            .or_else(|| num.as_f64().map(|f| f as i64))
            .ok_or_else(|| anyhow!("unable to interpret numeric order column")),
        JsonValue::String(s) => s
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
            break;
        }
    }
    ClickhouseWireFormat::Unknown
}

struct TableAnnotation {
    order_col: String,
    payload_col: String,
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
    let mut payload_col = None;
    let mut filter_cols = Vec::new();
    let mut seq_col = None;

    for part in body.split(|c| c == ',' || c == ';') {
        let mut kv = part.split('=').map(|s| s.trim());
        let key = kv.next().unwrap_or("").to_ascii_lowercase();
        let value = kv.next().unwrap_or("");
        match key.as_str() {
            "order_col" => order_col = Some(value.to_string()),
            "payload_col" => payload_col = Some(value.to_string()),
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
    let payload_col =
        payload_col.ok_or_else(|| anyhow!("proxist annotation missing payload_col"))?;
    if filter_cols.len() < 2 {
        anyhow::bail!(
            "proxist annotation must specify at least two filter_cols (group key and entity key)"
        );
    }

    Ok(Some(TableAnnotation {
        order_col,
        payload_col,
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

    #[tokio::test]
    async fn hot_stats_cover_window() {
        let scheduler = ProxistScheduler {
            sqlite: None,
            clickhouse: None,
            duckdb: None,
            postgres: None,
            hot_store: None,
            tables: Arc::new(Mutex::new(HashMap::new())),
            persisted_cutoff_micros: AtomicI64::new(-1),
            hot_stats: Arc::new(RwLock::new(HashMap::new())),
        };

        let stats = vec![
            HotSymbolSummary {
                tenant: "alpha".to_string(),
                symbol: "SYM1".to_string(),
                rows: 10,
                first_timestamp: Some(micros_to_system_time(100)),
                last_timestamp: Some(micros_to_system_time(200)),
            },
            HotSymbolSummary {
                tenant: "alpha".to_string(),
                symbol: "SYM2".to_string(),
                rows: 10,
                first_timestamp: Some(micros_to_system_time(100)),
                last_timestamp: Some(micros_to_system_time(200)),
            },
        ];

        scheduler.update_hot_stats(&stats).await;

        assert!(scheduler
            .hot_covers_window("alpha", &vec!["SYM1".to_string()], 120, 180)
            .await);
        assert!(!scheduler
            .hot_covers_window("alpha", &vec!["SYM1".to_string()], 90, 180)
            .await);
        assert!(!scheduler
            .hot_covers_window("alpha", &vec!["SYM3".to_string()], 120, 180)
            .await);
    }
}
