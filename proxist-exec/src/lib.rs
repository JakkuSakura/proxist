use anyhow::{anyhow, Context};
use async_trait::async_trait;
use proxist_ch::ClickhouseHttpClient;
use proxist_mem::HotColumnStore;
use proxist_metadata_sqlite::SqliteMetadataStore;
use serde_json::Value as JsonValue;
use sqlx::{Column, Row};
pub mod sql;
use proxist_core::query::QueryRange;
use base64::Engine as _;
use std::time::{Duration, SystemTime};
use std::collections::HashMap;
use std::sync::Mutex;

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

#[derive(Debug, Clone)]
pub enum SqlResult {
    Text(String),
    Rows(Vec<serde_json::Map<String, JsonValue>>),
}

pub struct ProxistScheduler {
    sqlite: Option<SqliteExecutor>,
    clickhouse: Option<ClickhouseHttpClient>,
    duckdb: Option<DuckDbExecutor>,
    postgres: Option<PostgresExecutor>,
    hot_store: Option<std::sync::Arc<dyn HotColumnStore>>,
    routing: RoutingConfig,
    tables: std::sync::Arc<Mutex<HashMap<String, TableConfig>>>,
    persisted_cutoff_micros: std::sync::atomic::AtomicI64,
}

#[derive(Debug, Clone)]
pub struct RoutingConfig {
    pub time_column: String,
}

#[derive(Debug, Clone)]
pub struct TableConfig {
    pub ddl: String,
    pub order_col: String,
    pub payload_col: String,
    pub filter_cols: Vec<String>,
    pub seq_col: Option<String>,
}

impl ProxistScheduler {
    pub async fn new(
        cfg: ExecutorConfig,
        ch: Option<ClickhouseHttpClient>,
        hot_store: Option<std::sync::Arc<dyn HotColumnStore>>,
    ) -> anyhow::Result<Self> {
        let sqlite = match cfg.sqlite_path {
            Some(path) => Some(SqliteExecutor::connect(&path).await.context("connect sqlite")?),
            None => None,
        };
        let duckdb = DuckDbExecutor::maybe_connect(cfg.duckdb_path.as_deref()).await?;
        let postgres = PostgresExecutor::maybe_connect(cfg.pg_url.as_deref()).await?;
        Ok(Self {
            sqlite,
            clickhouse: ch,
            duckdb,
            postgres,
            hot_store,
            routing: RoutingConfig {
                time_column: std::env::var("PROXIST_TIME_COLUMN").unwrap_or_else(|_| "ts_micros".into()),
            },
            tables: std::sync::Arc::new(Mutex::new(HashMap::new())),
            persisted_cutoff_micros: std::sync::atomic::AtomicI64::new(-1),
        })
    }

    /// Register or update table config to support hot execution.
    pub fn register_table(&self, table: impl Into<String>, cfg: TableConfig) {
        let mut guard = self.tables.lock().unwrap();
        guard.insert(table.into(), cfg);
    }
    /// Parse DDL and register mapping if found via `proxist:` annotation comment.
    pub fn register_ddl(&self, ddl_sql: &str) -> anyhow::Result<()> {
        if let Some(cfg) = parse_proxist_table_config(ddl_sql) {
            if let Some(table) = parse_table_name_from_ddl(ddl_sql) {
                self.register_table(table, cfg);
            }
        }
        Ok(())
    }

    /// Update global persisted cutoff watermark used for hot/cold routing decisions.
    pub fn set_persisted_cutoff(&self, cutoff: Option<SystemTime>) {
        let v = cutoff
            .map(|ts| ts.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(-1))
            .unwrap_or(-1);
        self.persisted_cutoff_micros.store(v, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl SqlExecutor for ProxistScheduler {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult> {
        let normalized = sql.trim().to_ascii_lowercase();
        if normalized.starts_with("select") || normalized.starts_with("with") {
            if let (Some(duck), Some(hot)) = (&self.duckdb, &self.hot_store) {
                let cfg_sel = {
                    let guards = self.tables.lock().unwrap();
                    analyze_simple_select_generic(sql, &*guards)
                };
                if let Some((cfg, sel)) = cfg_sel {
                    // If persisted cutoff exists and the requested window is entirely cold, prefer ClickHouse.
                    let cutoff = self.persisted_cutoff_micros.load(std::sync::atomic::Ordering::SeqCst);
                    if cutoff >= 0 {
                        let start_u = system_time_to_micros(sel.start);
                        let end_u = system_time_to_micros(sel.end);
                        if end_u <= cutoff {
                            // fully cold, skip DuckDB
                        } else {
                            // Load only hot portion (greater than cutoff)
                            let hot_start = if start_u > cutoff { sel.start } else { micros_to_system_time(cutoff + 1) };
                            let hot_sel = SimpleSelect { start: hot_start, ..sel.clone() };
                            let rows = load_hot_rows(hot, &hot_sel).await?;
                            if !rows.is_empty() {
                                let out = duck.load_table_and_execute_with_cfg(&cfg, &hot_sel.table, &rows, sql)?;
                                return Ok(SqlResult::Text(out));
                            }
                        }
                    } else {
                    let rows = load_hot_rows(hot, &sel).await?;
                    if !rows.is_empty() {
                        let out = duck.load_table_and_execute_with_cfg(&cfg, &sel.table, &rows, sql)?;
                        return Ok(SqlResult::Text(out));
                    }
                    }
                }
            }
            if let Some(ch) = &self.clickhouse {
                let body = ch.execute_raw(sql).await?;
                return Ok(SqlResult::Text(body));
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

pub struct SqliteExecutor {
    store: SqliteMetadataStore,
}

impl SqliteExecutor {
    pub async fn connect(path: &str) -> anyhow::Result<Self> {
        let store = SqliteMetadataStore::connect(path).await?;
        Ok(Self { store })
    }
}

#[async_trait]
impl SqlExecutor for SqliteExecutor {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult> {
        // Best-effort: run as execute or try fetch-all into JSON-like rows.
        // Note: sqlx generic row extraction requires DB-specific APIs; metadata store embeds pool.
        let pool = self.store.pool();
        // Try to fetch rows
        let rows = sqlx::query(sql).fetch_all(pool).await;
        match rows {
            Ok(rows) => {
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut map = serde_json::Map::new();
                    for col in row.columns() {
                        let name = col.name().to_string();
                        let v: Result<String, _> = row.try_get(name.as_str());
                        match v {
                            Ok(s) => {
                                map.insert(name, JsonValue::String(s));
                            }
                            Err(_) => {
                                // Fallback: represent as null when not stringly typed
                                map.insert(name, JsonValue::Null);
                            }
                        }
                    }
                    out.push(map);
                }
                Ok(SqlResult::Rows(out))
            }
            Err(_) => {
                // Execute as statement
                sqlx::query(sql).execute(pool).await?;
                Ok(SqlResult::Text("OK".into()))
            }
        }
    }
}

pub struct DuckDbExecutor;

#[async_trait]
impl SqlExecutor for DuckDbExecutor {
    async fn execute(&self, _sql: &str) -> anyhow::Result<SqlResult> {
        anyhow::bail!("duckdb executor not enabled at compile time")
    }
}

#[cfg(feature = "duckdb")]
pub struct DuckDbExecutorImpl {
    conn: duckdb::Connection,
}

#[cfg(feature = "duckdb")]
static mut DUCK_EXEC: Option<DuckDbExecutorImpl> = None;

#[cfg(feature = "duckdb")]
impl DuckDbExecutorImpl {
    fn new(path: &str) -> anyhow::Result<Self> {
        let conn = duckdb::Connection::open(path)?;
        Ok(Self { conn })
    }
    fn ensure_table_with_ddl(&self, ddl: &str) -> anyhow::Result<()> {
        self.conn.execute(ddl, [])?;
        Ok(())
    }
    fn insert_rows(&self, table: &str, cfg: &TableConfig, rows: &[HotRowFlat]) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        let sql = format!(
            "INSERT INTO \"{}\" (\"{}\", \"{}\", \"{}\", \"{}\"{} ) VALUES (?1, ?2, ?3, ?4{} )",
            table.replace('"', ""),
            cfg.filter_cols.get(0).cloned().unwrap_or_default(),
            cfg.filter_cols.get(1).cloned().unwrap_or_default(),
            cfg.order_col,
            cfg.payload_col,
            if cfg.seq_col.is_some() { ", \"".to_string() + cfg.seq_col.as_ref().unwrap() + "\"" 
            } else { String::new() },
            if cfg.seq_col.is_some() { ", ?5".to_string() } else { String::new() }
        );
        let mut stmt = tx.prepare(&sql)?;
        for r in rows {
            if cfg.seq_col.is_some() {
                stmt.execute((&r.k0, &r.k1, &r.ord_micros, &r.payload_base64, &r.seq))?;
            } else {
                stmt.execute((&r.k0, &r.k1, &r.ord_micros, &r.payload_base64))?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    fn query_text(&self, sql: &str) -> anyhow::Result<String> {
        // Return raw text by joining JSONEachRow if possible. For now, rely on DuckDB's default text output.
        let mut out = String::new();
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            // Coerce all columns to strings for a simple text output
            let mut cols = Vec::new();
            for idx in 0..row.column_count() {
                let v: Result<String, _> = row.get(idx);
                cols.push(v.unwrap_or_default());
            }
            Ok(cols.join("\t"))
        })?;
        for r in rows { out.push_str(&r?); out.push('\n'); }
        Ok(out)
    }
}

#[cfg(feature = "duckdb")]
impl DuckDbExecutor {
    fn load_table_and_execute_with_cfg(&self, cfg: &TableConfig, table: &str, rows: &[HotRowFlat], sql: &str) -> anyhow::Result<String> {
        // Use a single global connection hidden inside a static cell.
        unsafe {
            let exec = DUCK_EXEC.as_ref().ok_or_else(|| anyhow!("duckdb connection not initialized"))?;
            exec.ensure_table_with_ddl(&cfg.ddl)?;
            exec.insert_rows(table, cfg, rows)?;
            exec.query_text(sql)
        }
    }
}

#[cfg(not(feature = "duckdb"))]
impl DuckDbExecutor {
    fn load_table_and_execute(&self, _table: &str, _rows: &[HotRowFlat], _sql: &str) -> anyhow::Result<String> {
        anyhow::bail!("duckdb executor not enabled at compile time")
    }
    fn load_table_and_execute_with_cfg(&self, _cfg: &TableConfig, _table: &str, _rows: &[HotRowFlat], _sql: &str) -> anyhow::Result<String> {
        anyhow::bail!("duckdb executor not enabled at compile time")
    }
}

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

fn analyze_simple_select_generic(sql: &str, tables: &HashMap<String, TableConfig>) -> Option<(TableConfig, SimpleSelect)> {
    let dialect = fp_sql::dialect::ClickHouseDialect {};
    let stmts = fp_sql::parser::Parser::parse_sql(&dialect, sql).ok()?;
    let stmt = stmts.first()?;
    use fp_sql::ast::{Expr, Ident, Query, Select, SetExpr, Statement, TableFactor, Value};
    let (table, selection) = match stmt {
        Statement::Query(q) => {
            let Query { body, .. } = &**q;
            match &**body {
                SetExpr::Select(s) => {
                    let Select { from, selection, .. } = &**s;
                    if from.len() != 1 { return None; }
                    let tf = &from[0].relation;
                    let table_name = match tf {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => return None,
                    };
                    (table_name, selection)
                }
                _ => return None,
            }
        }
        _ => return None,
    };
    let sel = selection.as_ref()?;
    let cfg = tables.get(&table)?.clone();
    fn extract_string(e: &Expr) -> Option<String> {
        match e { Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()), _ => None }
    }
    let mut key0_val = None;
    let mut key1_vals: Vec<String> = Vec::new();
    let mut start = None;
    let mut end = None;
    fn as_micros(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(Value::Number(n, _)) => n.parse::<i64>().ok(),
            Expr::Value(Value::SingleQuotedString(s)) => s.parse::<i64>().ok(),
            _ => None,
        }
    }
    use fp_sql::ast::BinaryOperator as Op;
    fn walk(expr: &fp_sql::ast::Expr, f: &mut dyn FnMut(&fp_sql::ast::Expr)) { f(expr); if let fp_sql::ast::Expr::BinaryOp { left, right, .. } = expr { walk(left, f); walk(right, f); } }
    walk(sel, &mut |e| match e {
        fp_sql::ast::Expr::BinaryOp { left, op, right } => {
            match (&**left, op, &**right) {
                (fp_sql::ast::Expr::Identifier(Ident { value, .. }), Op::Eq, rhs)
                    if cfg.filter_cols.get(0).map(|c| value.eq_ignore_ascii_case(c)).unwrap_or(false) => {
                    key0_val = extract_string(rhs).or_else(|| extract_string(left));
                }
                (fp_sql::ast::Expr::BinaryOp { .. }, _, _) => {}
                (fp_sql::ast::Expr::Between { expr, low, high, .. }, _, _) => {
                    if let fp_sql::ast::Expr::Identifier(Ident { value, .. }) = &**expr {
                        if value.eq_ignore_ascii_case(&cfg.order_col) {
                            start = as_micros(low).map(micros_to_system_time);
                            end = as_micros(high).map(micros_to_system_time);
                        }
                    }
                }
                (fp_sql::ast::Expr::Identifier(Ident { value, .. }), Op::GtEq, rhs)
                    if value.eq_ignore_ascii_case(&cfg.order_col) => {
                    start = as_micros(rhs).map(micros_to_system_time);
                }
                (fp_sql::ast::Expr::Identifier(Ident { value, .. }), Op::Gt, rhs)
                    if value.eq_ignore_ascii_case(&cfg.order_col) => {
                    start = as_micros(rhs).map(micros_to_system_time);
                }
                (fp_sql::ast::Expr::Identifier(Ident { value, .. }), Op::Lt, rhs)
                    if value.eq_ignore_ascii_case(&cfg.order_col) => {
                    end = as_micros(rhs).map(micros_to_system_time);
                }
                (fp_sql::ast::Expr::Identifier(Ident { value, .. }), Op::LtEq, rhs)
                    if value.eq_ignore_ascii_case(&cfg.order_col) => {
                    end = as_micros(rhs).map(micros_to_system_time);
                }
                _ => {}
            }
        }
        fp_sql::ast::Expr::InList { expr, list, .. } => {
            if let fp_sql::ast::Expr::Identifier(Ident { value, .. }) = &**expr {
                if cfg.filter_cols.get(1).map(|c| value.eq_ignore_ascii_case(c)).unwrap_or(false) {
                    let list_vals: Vec<String> = list.iter().filter_map(extract_string).collect();
                    if !list_vals.is_empty() { key1_vals = list_vals; }
                }
            }
        }
        fp_sql::ast::Expr::BinaryOp { left, op: Op::Eq, right } => {
            // also handle key1 equality (single value)
            if let fp_sql::ast::Expr::Identifier(Ident { value, .. }) = &**left {
                if cfg.filter_cols.get(1).map(|c| value.eq_ignore_ascii_case(c)).unwrap_or(false) {
                    if let Some(s) = extract_string(right) { key1_vals = vec![s]; }
                }
            }
        }
        _ => {}
    });
    let key0 = key0_val?;
    if start.is_none() || end.is_none() || key1_vals.is_empty() { return None; }
    Some((cfg, SimpleSelect { table, key0, key1_list: key1_vals, start: start.unwrap(), end: end.unwrap() }))
}

async fn load_hot_rows(hot: &std::sync::Arc<dyn HotColumnStore>, sel: &SimpleSelect) -> anyhow::Result<Vec<HotRowFlat>> {
    let range = QueryRange::new(sel.start, sel.end);
    let rows = hot.scan_range(&sel.key0, &range, &sel.key1_list).await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(HotRowFlat {
            k0: sel.key0.clone(),
            k1: r.symbol,
            ord_micros: system_time_to_micros(r.timestamp),
            payload_base64: base64::engine::general_purpose::STANDARD.encode(&r.payload),
            seq: 0,
        });
    }
    Ok(out)
}

fn system_time_to_micros(ts: SystemTime) -> i64 {
    ts.duration_since(std::time::UNIX_EPOCH)
        .map(|dur| dur.as_micros() as i64)
        .unwrap_or_else(|err| {
            let dur = err.duration();
            -(dur.as_micros() as i64)
        })
}

fn micros_to_system_time(micros: i64) -> SystemTime {
    if micros >= 0 {
        std::time::UNIX_EPOCH + Duration::from_micros(micros as u64)
    } else {
        std::time::UNIX_EPOCH - Duration::from_micros((-micros) as u64)
    }
}

fn parse_table_name_from_ddl(sql: &str) -> Option<String> {
    let dialect = fp_sql::dialect::ClickHouseDialect {};
    let stmts = fp_sql::parser::Parser::parse_sql(&dialect, sql).ok()?;
    for stmt in stmts {
        if let fp_sql::ast::Statement::CreateTable { name, .. } = stmt { return Some(name.to_string()); }
    }
    None
}

fn parse_proxist_table_config(sql: &str) -> Option<TableConfig> {
    // Look for a comment containing `proxist:` and parse key=value pairs separated by commas
    let lower = sql.to_ascii_lowercase();
    let idx = lower.find("proxist:")?;
    let tail = &sql[idx + "proxist:".len()..];
    // up to end of line or comment terminator
    let end = tail.find('\n').or_else(|| tail.find("*/")).unwrap_or(tail.len());
    let body = tail[..end].trim();
    let mut order_col = None;
    let mut payload_col = None;
    let mut filter_cols: Vec<String> = Vec::new();
    let mut seq_col: Option<String> = None;
    for part in body.split(|c| c == ',' || c == ';') {
        let kv: Vec<&str> = part.split('=').map(|s| s.trim()).collect();
        if kv.len() != 2 { continue; }
        match kv[0].to_ascii_lowercase().as_str() {
            "order_col" => order_col = Some(kv[1].to_string()),
            "payload_col" => payload_col = Some(kv[1].to_string()),
            "filter_cols" => {
                filter_cols = kv[1].split(|c| c==',' || c==' ').map(|s| s.trim()).filter(|s|!s.is_empty()).map(|s| s.to_string()).collect();
            }
            "seq_col" => seq_col = Some(kv[1].to_string()),
            _ => {}
        }
    }
    Some(TableConfig {
        ddl: sql.to_string(),
        order_col: order_col?,
        payload_col: payload_col?,
        filter_cols,
        seq_col,
    })
}

pub struct PostgresExecutor;

#[async_trait]
impl SqlExecutor for PostgresExecutor {
    async fn execute(&self, _sql: &str) -> anyhow::Result<SqlResult> {
        anyhow::bail!("postgres executor not enabled at compile time")
    }
}

impl DuckDbExecutor {
    #[allow(unused_variables)]
    pub async fn maybe_connect(path: Option<&str>) -> anyhow::Result<Option<Self>> {
        #[cfg(feature = "duckdb")]
        {
            if path.is_none() {
                return Ok(None);
            }
            // Initialize a global connection for the executor.
            unsafe { DUCK_EXEC = Some(DuckDbExecutorImpl::new(path.unwrap())?); }
            return Ok(Some(Self));
        }
        #[cfg(not(feature = "duckdb"))]
        {
            let _ = path; // silence unused
            Ok(None)
        }
    }
}

impl PostgresExecutor {
    #[allow(unused_variables)]
    pub async fn maybe_connect(url: Option<&str>) -> anyhow::Result<Option<Self>> {
        #[cfg(feature = "postgres")]
        {
            if let Some(u) = url {
                let (client, connection) = tokio_postgres::connect(u, tokio_postgres::NoTls).await?;
                // Spawn connection driver; client is not stored since we only verify connectivity here.
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                drop(client);
                return Ok(Some(Self));
            }
            Ok(None)
        }
        #[cfg(not(feature = "postgres"))]
        {
            let _ = url; // silence unused
            Ok(None)
        }
    }
}
