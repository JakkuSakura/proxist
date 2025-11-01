use anyhow::Context;
use async_trait::async_trait;
use base64::Engine as _;
use fp_sql::{
    ast::{BinaryOperator as BinOp, Expr, Ident, Query, Select, SetExpr, Statement, TableFactor, Value},
    dialect::ClickHouseDialect,
    parser::Parser,
};
use crate::clickhouse::ClickhouseHttpClient;
use crate::metadata_sqlite::SqliteMetadataStore;
use proxist_core::query::QueryRange;
use proxist_mem::HotColumnStore;
use serde_json::{Map, Value as JsonValue};
use sqlx::{Column, Row};
use std::collections::HashMap;
use std::sync::{atomic::{AtomicI64, Ordering}, Arc, Mutex};
use std::time::{Duration, SystemTime};

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

#[derive(Debug, Clone)]
pub enum SqlResult {
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
}

pub struct ProxistScheduler {
    sqlite: Option<SqliteExecutor>,
    clickhouse: Option<ClickhouseHttpClient>,
    duckdb: Option<DuckDbExecutor>,
    postgres: Option<PostgresExecutor>,
    hot_store: Option<Arc<dyn HotColumnStore>>,
    tables: Arc<Mutex<HashMap<String, TableConfig>>>,
    persisted_cutoff_micros: AtomicI64,
}

impl ProxistScheduler {
    pub async fn new(
        cfg: ExecutorConfig,
        clickhouse: Option<ClickhouseHttpClient>,
        hot_store: Option<Arc<dyn HotColumnStore>>,
    ) -> anyhow::Result<Self> {
        let sqlite = match cfg.sqlite_path {
            Some(path) => Some(SqliteExecutor::connect(&path).await.context("connect sqlite")?),
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
        })
    }

    pub fn register_table(&self, table: impl Into<String>, cfg: TableConfig) {
        self.tables.lock().unwrap().insert(table.into(), cfg);
    }

    pub fn register_ddl(&self, ddl_sql: &str) -> anyhow::Result<()> {
        if let Some(cfg) = parse_proxist_table_config(ddl_sql) {
            if let Some(table) = parse_table_name_from_ddl(ddl_sql) {
                self.register_table(table, cfg);
            }
        }
        Ok(())
    }

    pub fn set_persisted_cutoff(&self, cutoff: Option<SystemTime>) {
        let micros = cutoff
            .and_then(|ts| ts.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|dur| dur.as_micros() as i64)
            .unwrap_or(-1);
        self.persisted_cutoff_micros.store(micros, Ordering::SeqCst);
    }

    fn rewrite_with_bounds(
        &self,
        sql: &str,
        order_col: &str,
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Option<String> {
        let mut stmts = Parser::parse_sql(&ClickHouseDialect {}, sql).ok()?;
        if stmts.is_empty() {
            return None;
        }
        let mut stmt = stmts.remove(0);

        let append_bounds = |selection: &mut Option<Expr>| {
            let ident = |name: &str| Expr::Identifier(Ident { value: name.to_string(), quote_style: None });
            let mut clauses = Vec::new();

            if let Some(lo) = lower {
                clauses.push(Expr::BinaryOp {
                    left: Box::new(ident(order_col)),
                    op: BinOp::GtEq,
                    right: Box::new(Expr::Value(Value::Number(lo.to_string(), false))),
                });
            }
            if let Some(hi) = upper {
                clauses.push(Expr::BinaryOp {
                    left: Box::new(ident(order_col)),
                    op: BinOp::LtEq,
                    right: Box::new(Expr::Value(Value::Number(hi.to_string(), false))),
                });
            }
            if clauses.is_empty() {
                return;
            }

            let extra = clauses.into_iter().reduce(|a, b| Expr::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            })
            .unwrap();

            if let Some(existing) = selection.take() {
                *selection = Some(Expr::BinaryOp {
                    left: Box::new(existing),
                    op: BinOp::And,
                    right: Box::new(extra),
                });
            } else {
                *selection = Some(extra);
            }
        };

        match &mut stmt {
            Statement::Query(q) => {
                if let SetExpr::Select(sel) = &mut *q.body {
                    append_bounds(&mut sel.selection);
                } else {
                    return None;
                }
            }
            _ => return None,
        }

        Some(stmt.to_string())
    }
}

#[async_trait]
impl SqlExecutor for ProxistScheduler {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult> {
        let normalized = sql.trim().to_ascii_lowercase();

        if normalized.starts_with("select") || normalized.starts_with("with") {
            if let (Some(duck), Some(hot)) = (&self.duckdb, &self.hot_store) {
                let cfg_sel = {
                    let guard = self.tables.lock().unwrap();
                    analyze_simple_select(sql, &*guard)
                };

                if let Some((cfg, sel)) = cfg_sel {
                    let cutoff = self.persisted_cutoff_micros.load(Ordering::SeqCst);
                    if cutoff >= 0 {
                        let start_u = system_time_to_micros(sel.start);
                        let end_u = system_time_to_micros(sel.end);
                        let mut outputs = Vec::new();

                        if start_u <= cutoff {
                            if let Some(cold_sql) = self.rewrite_with_bounds(sql, &cfg.order_col, Some(start_u), Some(cutoff.min(end_u))) {
                                if let Some(ch) = &self.clickhouse {
                                    outputs.push(ch.execute_raw(&cold_sql).await?);
                                }
                            }
                        }

                        if end_u > cutoff {
                            let hot_lo = if start_u > cutoff { start_u } else { cutoff + 1 };
                            let mut hot_sel = sel.clone();
                            hot_sel.start = micros_to_system_time(hot_lo);
                            let rows = load_hot_rows(hot, &hot_sel).await?;
                            if !rows.is_empty() {
                                outputs.push(duck.load_table_and_execute(&cfg, &hot_sel.table, &rows, sql)?);
                            }
                        }

                        if !outputs.is_empty() {
                            return Ok(SqlResult::Text(outputs.join("")));
                        }
                    } else {
                        let rows = load_hot_rows(hot, &sel).await?;
                        if !rows.is_empty() {
                            return Ok(SqlResult::Text(duck.load_table_and_execute(&cfg, &sel.table, &rows, sql)?));
                        }
                    }
                }
            }

            if let Some(ch) = &self.clickhouse {
                return Ok(SqlResult::Text(ch.execute_raw(sql).await?));
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
                            Ok(v) => { map.insert(name.to_string(), JsonValue::String(v)); }
                            Err(_) => { map.insert(name.to_string(), JsonValue::Null); }
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
        Ok(Self { conn: duckdb::Connection::open(path)? })
    }

    fn ensure_table(&self, ddl: &str) -> anyhow::Result<()> {
        self.conn.execute(ddl, [])?;
        Ok(())
    }

    fn insert_rows(&self, table: &str, cfg: &TableConfig, rows: &[HotRowFlat]) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        let mut cols = Vec::new();
        if let Some(c0) = cfg.filter_cols.get(0) { cols.push(c0.as_str()); }
        if let Some(c1) = cfg.filter_cols.get(1) { cols.push(c1.as_str()); }
        cols.push(&cfg.order_col);
        cols.push(&cfg.payload_col);
        if let Some(seq) = &cfg.seq_col { cols.push(seq.as_str()); }

        let placeholders: Vec<_> = (1..=cols.len()).map(|idx| format!("?{}", idx)).collect();
        let sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            table.replace('"', ""),
            cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", "),
            placeholders.join(", "),
        );

        let mut stmt = tx.prepare(&sql)?;
        for row in rows {
            if cfg.seq_col.is_some() {
                stmt.execute((&row.k0, &row.k1, &row.ord_micros, &row.payload_base64, &row.seq))?;
            } else {
                stmt.execute((&row.k0, &row.k1, &row.ord_micros, &row.payload_base64))?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn query_text(&self, sql: &str) -> anyhow::Result<String> {
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            let mut cols = Vec::new();
            for idx in 0..row.column_count() {
                match row.get::<_, String>(idx) {
                    Ok(v) => cols.push(v),
                    Err(_) => cols.push(String::new()),
                }
            }
            Ok(cols.join("\t"))
        })?;
        let mut out = String::new();
        for row in rows { out.push_str(&row?); out.push('\n'); }
        Ok(out)
    }
}

#[cfg(feature = "duckdb")]
impl DuckDbExecutor {
    fn load_table_and_execute(&self, cfg: &TableConfig, table: &str, rows: &[HotRowFlat], sql: &str) -> anyhow::Result<String> {
        unsafe {
            let exec = DUCK_EXEC.as_ref().ok_or_else(|| anyhow::anyhow("duckdb connection not initialized"))?;
            exec.ensure_table(&cfg.ddl)?;
            exec.insert_rows(table, cfg, rows)?;
            exec.query_text(sql)
        }
    }

    async fn maybe_connect(path: Option<&str>) -> anyhow::Result<Option<Self>> {
        if let Some(p) = path {
            unsafe { DUCK_EXEC = Some(DuckDbExecutorImpl::new(p)?); }
            Ok(Some(Self))
        } else {
            Ok(None)
        }
    }
}

#[cfg(not(feature = "duckdb"))]
impl DuckDbExecutor {
    fn load_table_and_execute(&self, _cfg: &TableConfig, _table: &str, _rows: &[HotRowFlat], _sql: &str) -> anyhow::Result<String> {
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
                let (_client, connection) = tokio_postgres::connect(u, tokio_postgres::NoTls).await?;
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

fn analyze_simple_select(sql: &str, tables: &HashMap<String, TableConfig>) -> Option<(TableConfig, SimpleSelect)> {
    let stmts = Parser::parse_sql(&ClickHouseDialect {}, sql).ok()?;
    let stmt = stmts.first()?;

    let (table, selection) = match stmt {
        Statement::Query(q) => {
            let Query { body, .. } = &**q;
            match &**body {
                SetExpr::Select(sel) => {
                    let Select { from, selection, .. } = &**sel;
                    if from.len() != 1 { return None; }
                    match &from[0].relation {
                        TableFactor::Table { name, .. } => (name.to_string(), selection),
                        _ => return None,
                    }
                }
                _ => return None,
            }
        }
        _ => return None,
    };

    let cfg = tables.get(&table)?.clone();
    let predicate = selection.as_ref()?;

    let mut key0_val = None;
    let mut key1_vals = Vec::new();
    let mut start = None;
    let mut end = None;

    fn extract_string(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn as_micros(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(Value::Number(n, _)) => n.parse::<i64>().ok(),
            Expr::Value(Value::SingleQuotedString(s)) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    fn walk(expr: &Expr, f: &mut dyn FnMut(&Expr)) {
        f(expr);
        if let Expr::BinaryOp { left, right, .. } = expr {
            walk(left, f);
            walk(right, f);
        }
    }

    let filter0 = cfg.filter_cols.get(0).map(|s| s.to_ascii_lowercase());
    let filter1 = cfg.filter_cols.get(1).map(|s| s.to_ascii_lowercase());
    let order_col = cfg.order_col.to_ascii_lowercase();

    walk(predicate, &mut |expr| {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_ident = matches_ident(left);
                match (left_ident, op) {
                    (Some(col), BinOp::Eq) => {
                        if filter0.as_ref().map(|c| c == &col).unwrap_or(false) {
                            key0_val = extract_string(right);
                        } else if filter1.as_ref().map(|c| c == &col).unwrap_or(false) {
                            if let Some(value) = extract_string(right) {
                                key1_vals = vec![value];
                            }
                        }
                    }
                    (Some(col), BinOp::Gt | BinOp::GtEq) if col == order_col => {
                        start = as_micros(right).map(micros_to_system_time);
                    }
                    (Some(col), BinOp::Lt | BinOp::LtEq) if col == order_col => {
                        end = as_micros(right).map(micros_to_system_time);
                    }
                    _ => {}
                }
            }
            Expr::Between { expr, low, high, .. } => {
                if let Some(col) = matches_ident(expr) {
                    if col == order_col {
                        start = as_micros(low).map(micros_to_system_time);
                        end = as_micros(high).map(micros_to_system_time);
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Expr::Identifier(Ident { value, .. }) = &**expr {
                    let col = value.to_ascii_lowercase();
                    if filter1.as_ref().map(|c| c == &col).unwrap_or(false) {
                        let vals: Vec<String> = list.iter().filter_map(extract_string).collect();
                        if !vals.is_empty() { key1_vals = vals; }
                    }
                }
            }
            _ => {}
        }
    });

    let key0 = key0_val?;
    if key1_vals.is_empty() {
        return None;
    }
    let start = start.unwrap_or(micros_to_system_time(i64::MIN));
    let end = end.unwrap_or(micros_to_system_time(i64::MAX));

    Some((cfg, SimpleSelect { table, key0, key1_list: key1_vals, start, end }))
}

fn matches_ident(expr: &Expr) -> Option<String> {
    if let Expr::Identifier(Ident { value, .. }) = expr {
        Some(value.to_ascii_lowercase())
    } else {
        None
    }
}

async fn load_hot_rows(hot: &Arc<dyn HotColumnStore>, sel: &SimpleSelect) -> anyhow::Result<Vec<HotRowFlat>> {
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

fn parse_table_name_from_ddl(sql: &str) -> Option<String> {
    let mut stmts = Parser::parse_sql(&ClickHouseDialect {}, sql).ok()?;
    for stmt in stmts.drain(..) {
        if let Statement::CreateTable { name, .. } = stmt {
            return Some(name.to_string());
        }
    }
    None
}

fn parse_proxist_table_config(sql: &str) -> Option<TableConfig> {
    let lower = sql.to_ascii_lowercase();
    let idx = lower.find("proxist:")?;
    let tail = &sql[idx + "proxist:".len()..];
    let end = tail.find('\n').or_else(|| tail.find("*/")).unwrap_or(tail.len());
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

    Some(TableConfig {
        ddl: sql.to_string(),
        order_col: order_col?,
        payload_col: payload_col?,
        filter_cols,
        seq_col,
    })
}
