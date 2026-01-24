mod clickhouse;
mod ingest;
mod metadata_sqlite;
mod pgwire_server;
mod scheduler;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::clickhouse::{
    ClickhouseConfig, ClickhouseHttpClient, ClickhouseHttpSink, ClickhouseQueryRow, ClickhouseSink,
};
use crate::metadata_sqlite::SqliteMetadataStore;
use crate::scheduler::{
    ClickhouseWire, ClickhouseWireFormat, ExecutorConfig, ProxistScheduler, SqlExecutor, SqlResult,
};
use anyhow::{anyhow, bail, Context};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::NaiveDateTime;
use ingest::{HotColdSummaryRow, IngestService};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use proxist_core::api::{
    DiagnosticsBundle, DiagnosticsHotSummaryRow, QueryRequest, QueryResponse, QueryRow,
    StatusResponse, SymbolDictionarySpec,
};
use proxist_core::{
    ingest::IngestRecord,
    metadata::ClusterMetadata,
    query::{QueryOperation, RollingAggregation},
    MetadataStore, ShardAssignment, ShardHealth,
};
use proxist_mem::{HotColumnStore, InMemoryHotColumnStore, MemConfig};
use proxist_wal::{WalConfig, WalManager};
use fp_sql::{
    ast::{Expr, FunctionArg, FunctionArgExpr, Ident, SetExpr, Statement, Value, Values},
    ensure_engine_clause,
    parser::Parser,
    parse_sql_dialect, replace_engine_case_insensitive, split_statements, sqlparser_dialect,
    strip_leading_sql_comments, SqlDialect,
};
use fp_prql::PrqlFrontend;
use serde_bytes::ByteBuf;
use serde_json::json;
use tokio::signal;
use tracing::{error, info, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let metrics_handle = init_metrics()?;
    let config = DaemonConfig::load()?;

    info!(?config, "starting proxistd");

    let metadata_store = SqliteMetadataStore::connect(&config.metadata_path).await?;
    let daemon = ProxistDaemon::new(config, metadata_store, metrics_handle).await?;
    daemon.run().await?;

    Ok(())
}

fn init_tracing() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .map_err(|err| anyhow::anyhow!(err))
}

fn init_metrics() -> anyhow::Result<Option<PrometheusHandle>> {
    let builder = PrometheusBuilder::new();
    match builder.install_recorder() {
        Ok(handle) => Ok(Some(handle)),
        Err(err) => {
            tracing::warn!(error = %err, "metrics recorder already installed; continuing without duplicate");
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
struct DaemonConfig {
    metadata_path: String,
    http_addr: SocketAddr,
    http_dialect: DialectMode,
    pg_addr: Option<SocketAddr>,
    pg_dialect: DialectMode,
    clickhouse: Option<ClickhouseConfig>,
    api_token: Option<String>,
    duckdb_path: Option<String>,
    pg_url: Option<String>,
    wal_dir: Option<String>,
    wal_segment_bytes: u64,
    wal_snapshot_rows: u64,
    wal_fsync: bool,
    wal_replay_persist: bool,
    persisted_cutoff_override: Option<SystemTime>,
}

impl DaemonConfig {
    fn load() -> anyhow::Result<Self> {
        let metadata_path = std::env::var("PROXIST_METADATA_SQLITE_PATH")
            .unwrap_or_else(|_| "./proxist-meta.db".into());
        let http_addr = std::env::var("PROXIST_HTTP_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".into())
            .parse()?;
        let http_dialect =
            parse_dialect_mode(std::env::var("PROXIST_HTTP_DIALECT").ok(), DialectDefault::Http)?;
        let pg_addr = std::env::var("PROXIST_PG_ADDR").ok().map(|value| value.parse()).transpose()?;
        let pg_dialect =
            parse_dialect_mode(std::env::var("PROXIST_PG_DIALECT").ok(), DialectDefault::Pg)?;
        let clickhouse = load_clickhouse_config();
        let api_token = if let Ok(token_path) = std::env::var("PROXIST_API_TOKEN_FILE") {
            let contents = std::fs::read_to_string(&token_path)
                .with_context(|| format!("read API token file at {}", token_path))?;
            let trimmed = contents.trim();
            if trimmed.is_empty() {
                bail!("PROXIST_API_TOKEN_FILE points to an empty token");
            }
            Some(trimmed.to_string())
        } else {
            std::env::var("PROXIST_API_TOKEN").ok()
        };
        Ok(Self {
            metadata_path,
            http_addr,
            http_dialect,
            pg_addr,
            pg_dialect,
            clickhouse,
            api_token,
            duckdb_path: std::env::var("PROXIST_DUCKDB_PATH").ok(),
            pg_url: std::env::var("PROXIST_PG_URL").ok(),
            wal_dir: std::env::var("PROXIST_WAL_DIR").ok(),
            wal_segment_bytes: std::env::var("PROXIST_WAL_SEGMENT_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256 * 1024 * 1024),
            wal_snapshot_rows: std::env::var("PROXIST_WAL_SNAPSHOT_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5_000_000),
            wal_fsync: std::env::var("PROXIST_WAL_FSYNC")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            wal_replay_persist: std::env::var("PROXIST_WAL_REPLAY_PERSIST")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            persisted_cutoff_override: std::env::var("PROXIST_PERSISTED_CUTOFF_OVERRIDE_MICROS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .map(micros_to_system_time),
        })
    }
}

#[derive(Debug, Clone)]
enum DialectMode {
    Sql(SqlDialect),
    Prql(SqlDialect),
}

#[derive(Debug, Clone, Copy)]
enum DialectDefault {
    Http,
    Pg,
}

impl DialectMode {
    fn sql_dialect(&self) -> SqlDialect {
        match self {
            DialectMode::Sql(dialect) | DialectMode::Prql(dialect) => dialect.clone(),
        }
    }

    fn is_clickhouse(&self) -> bool {
        matches!(self.sql_dialect(), SqlDialect::ClickHouse)
    }
}

fn parse_dialect_mode(
    value: Option<String>,
    default_kind: DialectDefault,
) -> anyhow::Result<DialectMode> {
    let default_sql = match default_kind {
        DialectDefault::Http => SqlDialect::ClickHouse,
        DialectDefault::Pg => SqlDialect::Postgres,
    };
    let Some(raw) = value else {
        return Ok(DialectMode::Sql(default_sql));
    };
    let lowered = raw.trim().to_ascii_lowercase();
    if lowered == "prql" {
        return Ok(DialectMode::Prql(default_sql));
    }
    let Some(parsed) = parse_sql_dialect(&lowered) else {
        bail!("unsupported dialect {raw}");
    };
    Ok(DialectMode::Sql(parsed))
}


fn load_clickhouse_config() -> Option<ClickhouseConfig> {
    let endpoint = std::env::var("PROXIST_CLICKHOUSE_ENDPOINT").ok()?;
    let database =
        std::env::var("PROXIST_CLICKHOUSE_DATABASE").unwrap_or_else(|_| "proxist".into());
    let table = std::env::var("PROXIST_CLICKHOUSE_TABLE").unwrap_or_else(|_| "ticks".into());
    let username = std::env::var("PROXIST_CLICKHOUSE_USER").ok();
    let password = std::env::var("PROXIST_CLICKHOUSE_PASSWORD").ok();
    let insert_batch_rows = std::env::var("PROXIST_CLICKHOUSE_BATCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let timeout_secs = std::env::var("PROXIST_CLICKHOUSE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let max_retries = std::env::var("PROXIST_CLICKHOUSE_MAX_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let retry_backoff_ms = std::env::var("PROXIST_CLICKHOUSE_RETRY_BACKOFF_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let query_timeout_secs = std::env::var("PROXIST_CLICKHOUSE_QUERY_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok());

    Some(ClickhouseConfig {
        endpoint,
        database,
        table,
        username,
        password,
        insert_batch_rows,
        timeout_secs,
        max_retries,
        retry_backoff_ms,
        query_timeout_secs,
    })
}

struct ProxistDaemon {
    config: DaemonConfig,
    metadata_cache: Arc<tokio::sync::Mutex<ClusterMetadata>>,
    metadata_store: SqliteMetadataStore,
    hot_store: Arc<dyn HotColumnStore>,
    ingest_service: Arc<IngestService>,
    scheduler: Arc<ProxistScheduler>,
    metrics_handle: Option<PrometheusHandle>,
    wal: Option<Arc<WalManager>>,
}

#[derive(Clone)]
struct AppState {
    metadata: SqliteMetadataStore,
    metadata_cache: Arc<tokio::sync::Mutex<ClusterMetadata>>,
    hot_store: Arc<dyn HotColumnStore>,
    ingest: Arc<IngestService>,
    scheduler: Arc<ProxistScheduler>,
    metrics: Option<PrometheusHandle>,
    api_token: Option<String>,
    persisted_cutoff_override: Option<SystemTime>,
    http_dialect: DialectMode,
}

#[derive(Debug)]
struct AppError(anyhow::Error);

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        AppError(err.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!(error = ?self.0, "HTTP request failed");
        let payload = Json(json!({ "error": self.0.to_string() }));
        (StatusCode::INTERNAL_SERVER_ERROR, payload).into_response()
    }
}

async fn compose_status(state: &AppState) -> Result<StatusResponse, AppError> {
    let metadata = state.metadata_cache.lock().await.clone();
    let shard_health = state.metadata.list_shard_health().await?;
    let clickhouse = state.ingest.clickhouse_status();
    Ok(StatusResponse {
        metadata,
        shard_health,
        clickhouse,
    })
}

async fn compose_diagnostics(state: &AppState) -> Result<DiagnosticsBundle, AppError> {
    let status = compose_status(state).await?;
    let metrics = state.metrics.as_ref().map(|handle| handle.render());
    let hot_summary = state
        .ingest
        .hot_cold_summary()
        .await?
        .into_iter()
        .map(convert_summary_row)
        .collect();
    let persistence = state.ingest.tracker_snapshot().await;
    Ok(DiagnosticsBundle {
        captured_at: SystemTime::now(),
        status,
        metrics,
        persistence,
        hot_summary,
    })
}

fn convert_summary_row(row: HotColdSummaryRow) -> DiagnosticsHotSummaryRow {
    DiagnosticsHotSummaryRow {
        tenant: row.tenant,
        symbol: row.symbol,
        shard_id: row.shard_id,
        hot_rows: row.hot_rows,
        hot_first_micros: option_time_to_micros(row.hot_first),
        hot_last_micros: option_time_to_micros(row.hot_last),
        persisted_through_micros: option_time_to_micros(row.persisted_through),
        wal_high_micros: option_time_to_micros(row.wal_high),
    }
}

fn option_time_to_micros(ts: Option<SystemTime>) -> Option<i64> {
    ts.map(system_time_to_micros)
}

fn hot_row_to_query_row(row: proxist_mem::HotRow) -> QueryRow {
    QueryRow {
        symbol: row.symbol,
        timestamp: row.timestamp,
        payload: ByteBuf::from(row.payload),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Default,
    TabSeparated,
    TabSeparatedWithNames,
    JsonEachRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemSummaryView {
    Legacy,
    Neutral,
}

fn match_system_summary_query(sql: &str) -> Option<SystemSummaryView> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select") {
        return None;
    }

    let normalized = lower.replace('"', "").replace('`', "");
    if normalized.contains("system.proxist_hot_summary") {
        Some(SystemSummaryView::Legacy)
    } else if normalized.contains("system.proxist_ingest_summary")
        || normalized.contains("proxist.__system_ingest_summary")
    {
        Some(SystemSummaryView::Neutral)
    } else {
        None
    }
}

fn detect_output_format(sql: &str) -> OutputFormat {
    let lower = sql.to_ascii_lowercase();
    if let Some(idx) = lower.rfind("format") {
        let mut remainder = sql[idx + "format".len()..].trim_start();
        if let Some(semicolon_idx) = remainder.find(';') {
            remainder = &remainder[..semicolon_idx];
        }
        if let Some(token) = remainder.split_whitespace().find(|part| !part.is_empty()) {
            let upper = token.to_ascii_uppercase();
            return match upper.as_str() {
                "TSVWITHNAMES" | "TABSEPARATEDWITHNAMES" => OutputFormat::TabSeparatedWithNames,
                "TSV" | "TABSEPARATED" => OutputFormat::TabSeparated,
                "JSONEACHROW" => OutputFormat::JsonEachRow,
                _ => OutputFormat::Default,
            };
        }
    }
    OutputFormat::Default
}

fn render_system_summary(
    rows: &[HotColdSummaryRow],
    format: OutputFormat,
    view: SystemSummaryView,
) -> String {
    let headers: [&str; 8] = match view {
        SystemSummaryView::Legacy => [
            "tenant",
            "symbol",
            "shard_id",
            "hot_rows",
            "hot_first_micros",
            "hot_last_micros",
            "persisted_through_micros",
            "wal_high_micros",
        ],
        SystemSummaryView::Neutral => [
            "group_key",
            "entity_key",
            "route_key",
            "memory_rows",
            "memory_first_micros",
            "memory_last_micros",
            "durable_through_micros",
            "wal_high_micros",
        ],
    };

    if matches!(format, OutputFormat::JsonEachRow) {
        return render_system_summary_json(rows, view);
    }

    let mut output = String::new();
    if matches!(format, OutputFormat::TabSeparatedWithNames) {
        output.push_str(&headers.join("\t"));
        output.push('\n');
    }

    for row in rows {
        let shard = row.shard_id.as_ref().map(|s| s.as_str()).unwrap_or("\\N");
        let values: [String; 8] = match view {
            SystemSummaryView::Legacy => [
                row.tenant.clone(),
                row.symbol.clone(),
                shard.to_string(),
                row.hot_rows.to_string(),
                format_opt_micros(row.hot_first),
                format_opt_micros(row.hot_last),
                format_opt_micros(row.persisted_through),
                format_opt_micros(row.wal_high),
            ],
            SystemSummaryView::Neutral => [
                row.tenant.clone(),
                row.symbol.clone(),
                shard.to_string(),
                row.hot_rows.to_string(),
                format_opt_micros(row.hot_first),
                format_opt_micros(row.hot_last),
                format_opt_micros(row.persisted_through),
                format_opt_micros(row.wal_high),
            ],
        };

        output.push_str(&values.join("\t"));
        output.push('\n');
    }

    output
}

fn format_opt_micros(value: Option<SystemTime>) -> String {
    match value {
        Some(ts) => system_time_to_micros(ts).to_string(),
        None => "\\N".to_string(),
    }
}

fn render_system_summary_json(rows: &[HotColdSummaryRow], view: SystemSummaryView) -> String {
    let mut output = String::new();
    for row in rows {
        let payload = match view {
            SystemSummaryView::Legacy => serde_json::json!({
                "tenant": row.tenant,
                "symbol": row.symbol,
                "shard_id": row.shard_id,
                "hot_rows": row.hot_rows,
                "hot_first_micros": row.hot_first.map(system_time_to_micros),
                "hot_last_micros": row.hot_last.map(system_time_to_micros),
                "persisted_through_micros": row.persisted_through.map(system_time_to_micros),
                "wal_high_micros": row.wal_high.map(system_time_to_micros),
            }),
            SystemSummaryView::Neutral => serde_json::json!({
                "group_key": row.tenant,
                "entity_key": row.symbol,
                "route_key": row.shard_id,
                "memory_rows": row.hot_rows,
                "memory_first_micros": row.hot_first.map(system_time_to_micros),
                "memory_last_micros": row.hot_last.map(system_time_to_micros),
                "durable_through_micros": row.persisted_through.map(system_time_to_micros),
                "wal_high_micros": row.wal_high.map(system_time_to_micros),
            }),
        };
        output.push_str(&payload.to_string());
        output.push('\n');
    }
    output
}

fn render_rows_as_jsoneachrow(rows: Vec<serde_json::Map<String, serde_json::Value>>) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str(&serde_json::Value::Object(row).to_string());
        out.push('\n');
    }
    out
}

enum SqlBatchResult {
    Text(String),
    Scheduler(SqlResult),
}

async fn forward_sql_to_scheduler(state: &AppState, sql: &str) -> Result<String, AppError> {
    let result = state.scheduler.execute(sql).await?;
    let body = match result {
        SqlResult::Clickhouse(ClickhouseWire { format, body }) => {
            if let ClickhouseWireFormat::Unknown = format {
                // Future formats will be exposed once the API supports them.
            }
            body
        }
        SqlResult::Text(s) => s,
        SqlResult::Rows(rows) => render_rows_as_jsoneachrow(rows),
    };
    Ok(body)
}

fn compile_statements(
    dialect: &DialectMode,
    sql_text: &str,
) -> Result<Vec<String>, AppError> {
    match dialect {
        DialectMode::Sql(_) => Ok(split_statements(sql_text)),
        DialectMode::Prql(target) => {
            let frontend = PrqlFrontend::new();
            let compiled = frontend
                .compile(sql_text, Some(target.clone()))
                .map_err(|err| AppError(anyhow!(err)))?;
            Ok(compiled.statements)
        }
    }
}

fn parse_sql_with_dialect(
    dialect: &DialectMode,
    sql: &str,
) -> Result<Vec<Statement>, AppError> {
    let dialect_impl = sqlparser_dialect(dialect.sql_dialect());
    Parser::parse_sql(dialect_impl.as_ref(), sql)
        .map_err(|err| AppError(anyhow!("failed to parse SQL: {err}")))
}

async fn execute_sql_batch(
    state: &AppState,
    dialect: DialectMode,
    sql_text: &str,
) -> Result<Vec<SqlBatchResult>, AppError> {
    let mut outputs = Vec::new();
    let mut seq_counter: u64 = 0;

    for stmt_text in compile_statements(&dialect, sql_text)? {
        let normalized = strip_leading_sql_comments(&stmt_text);
        if normalized.is_empty() {
            continue;
        }

        if normalized.to_ascii_lowercase().starts_with("create") {
            let mut parsed = parse_sql_with_dialect(&dialect, normalized)?;
            if parsed.is_empty() {
                continue;
            }
            if parsed.len() != 1 {
                let raw = forward_sql_to_scheduler(state, normalized).await?;
                outputs.push(SqlBatchResult::Text(raw));
                continue;
            }
            let mut stmt = parsed.remove(0);
            if let Statement::CreateTable { engine, .. } = &mut stmt {
                if dialect.is_clickhouse() {
                    let mut forwarded = normalized.to_string();
                    if let Some(e) = engine {
                        if e.eq_ignore_ascii_case("mixedmergetree") {
                            forwarded = replace_engine_case_insensitive(&forwarded, "MergeTree");
                        }
                    } else {
                        forwarded = ensure_engine_clause(&forwarded, "MergeTree");
                    }
                    state.scheduler.register_ddl(&forwarded)?;
                    if let Some(client) = state.ingest.clickhouse_client() {
                        let raw = client.execute_raw(&forwarded).await?;
                        outputs.push(SqlBatchResult::Text(raw));
                    } else {
                        let raw = forward_sql_to_scheduler(state, &forwarded).await?;
                        outputs.push(SqlBatchResult::Text(raw));
                    }
                    continue;
                }
            }
        }

        if !normalized.to_ascii_lowercase().starts_with("insert") {
            if let Some(view) = match_system_summary_query(normalized) {
                let format = detect_output_format(normalized);
                let summary = state.ingest.hot_cold_summary().await?;
                let rendered = render_system_summary(&summary, format, view);
                outputs.push(SqlBatchResult::Text(rendered));
                continue;
            }
            let result = state.scheduler.execute(normalized).await?;
            outputs.push(SqlBatchResult::Scheduler(result));
            continue;
        }

        let parsed = parse_sql_with_dialect(&dialect, normalized)?;
        if parsed.is_empty() {
            continue;
        }
        if parsed.len() != 1 {
            return Err(AppError(anyhow!(
                "multiple statements detected; please separate with semicolons"
            )));
        }

        match parsed.into_iter().next().unwrap() {
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let table_name_lower = table_name.to_string().to_ascii_lowercase();
                let table_cfg = state.scheduler.table_config(&table_name_lower);
                tracing::debug!(table = %table_name_lower, ingestable = table_cfg.is_some(), "processing INSERT statement");
                if let Some(query) = source {
                    match *query.body {
                        SetExpr::Values(values) => {
                            if let Some(cfg) = table_cfg {
                                let records = build_ingest_records(
                                    &cfg,
                                    &columns,
                                    values,
                                    &mut seq_counter,
                                )?;
                                if !records.is_empty() {
                                    state.ingest.ingest_records(records).await?;
                                }
                                outputs.push(SqlBatchResult::Text("Ok.\n".to_string()));
                            } else {
                                let raw = forward_sql_to_scheduler(state, normalized).await?;
                                outputs.push(SqlBatchResult::Text(raw));
                            }
                        }
                        _ => {
                            let raw = forward_sql_to_scheduler(state, normalized).await?;
                            outputs.push(SqlBatchResult::Text(raw));
                        }
                    }
                } else {
                    let raw = forward_sql_to_scheduler(state, normalized).await?;
                    outputs.push(SqlBatchResult::Text(raw));
                }
            }
            _ => {
                let result = state.scheduler.execute(normalized).await?;
                outputs.push(SqlBatchResult::Scheduler(result));
            }
        }
    }

    Ok(outputs)
}

fn build_ingest_records(
    cfg: &crate::scheduler::TableConfig,
    columns: &[Ident],
    values: Values,
    seq_counter: &mut u64,
) -> anyhow::Result<Vec<IngestRecord>> {
    let column_names: Vec<String> = if columns.is_empty() {
        cfg.columns.clone()
    } else {
        columns.iter().map(|c| c.value.to_lowercase()).collect()
    };

    let mut records = Vec::new();
    let tenant_col = cfg.filter_cols.get(0).map(|s| s.to_ascii_lowercase());
    let symbol_col = cfg.filter_cols.get(1).map(|s| s.to_ascii_lowercase());
    let order_col = cfg.order_col.to_ascii_lowercase();
    let payload_col = cfg.payload_col.to_ascii_lowercase();
    let seq_col = cfg.seq_col.as_ref().map(|s| s.to_ascii_lowercase());

    let tenant_col =
        tenant_col.ok_or_else(|| anyhow!("proxist filter_cols missing tenant column"))?;
    let symbol_col =
        symbol_col.ok_or_else(|| anyhow!("proxist filter_cols missing symbol column"))?;

    for row in values.rows {
        if row.len() != column_names.len() {
            anyhow::bail!(
                "column/value count mismatch: expected {} values, got {}",
                column_names.len(),
                row.len()
            );
        }

        let mut tenant: Option<String> = None;
        let mut symbol: Option<String> = None;
        let mut timestamp: Option<SystemTime> = None;
        let mut seq: Option<u64> = None;
        let mut payload_bytes: Option<Vec<u8>> = None;

        for (col, expr) in column_names.iter().zip(row.iter()) {
            let col_lower = col.to_ascii_lowercase();
            if col_lower == tenant_col {
                tenant = Some(expr_to_string(expr)?);
            } else if col_lower == symbol_col {
                symbol = Some(expr_to_string(expr)?);
            } else if col_lower == order_col {
                timestamp = Some(expr_to_timestamp(expr)?);
            } else if col_lower == payload_col {
                payload_bytes = Some(expr_to_base64_bytes(expr)?);
            } else if seq_col.as_deref() == Some(&col_lower) {
                seq = Some(expr_to_u64(expr)?);
            }
        }

        let tenant = tenant.ok_or_else(|| anyhow!("tenant column required"))?;
        let symbol = symbol.ok_or_else(|| anyhow!("symbol column required"))?;
        let timestamp = timestamp.ok_or_else(|| anyhow!("timestamp column required"))?;
        let seq = seq.unwrap_or_else(|| {
            let current = *seq_counter;
            *seq_counter += 1;
            current
        });

        records.push(IngestRecord {
            tenant,
            shard_id: String::new(),
            symbol,
            timestamp,
            payload: payload_bytes.unwrap_or_default(),
            seq,
        });
    }

    Ok(records)
}

fn expr_to_string(expr: &Expr) -> anyhow::Result<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        _ => Err(anyhow!("expected string literal")),
    }
}

fn expr_to_base64_bytes(expr: &Expr) -> anyhow::Result<Vec<u8>> {
    let text = expr_to_string(expr)?;
    BASE64_STANDARD
        .decode(text.as_bytes())
        .map_err(|err| anyhow!("invalid base64 payload: {err}"))
}

fn expr_to_u64(expr: &Expr) -> anyhow::Result<u64> {
    match expr {
        Expr::Value(Value::Number(num, _)) => num
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid integer literal: {err}")),
        _ => Err(anyhow!("expected numeric literal for seq")),
    }
}

fn expr_to_timestamp(expr: &Expr) -> anyhow::Result<SystemTime> {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            if name == "todatetime64" || name == "todatetime" {
                if func.args.is_empty() {
                    anyhow::bail!("toDateTime requires arguments");
                }
                let first = match &func.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => inner,
                    _ => anyhow::bail!("unsupported function argument"),
                };
                let text = expr_to_string(first)?;
                parse_datetime_string(&text)
            } else if name == "tounixtimestamp" || name == "tounixtimestamp64micro" {
                if func.args.is_empty() {
                    anyhow::bail!("toUnixTimestamp requires arguments");
                }
                let first = match &func.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => inner,
                    _ => anyhow::bail!("unsupported function argument"),
                };
                expr_to_timestamp(first)
            } else {
                anyhow::bail!("unsupported function {name}")
            }
        }
        Expr::Value(Value::SingleQuotedString(text)) => parse_datetime_string(text),
        Expr::Value(Value::Number(num, _)) => {
            let micros: i64 = num
                .parse()
                .map_err(|err| anyhow!("invalid microsecond value: {err}"))?;
            if micros >= 0 {
                Ok(SystemTime::UNIX_EPOCH + Duration::from_micros(micros as u64))
            } else {
                Ok(SystemTime::UNIX_EPOCH - Duration::from_micros((-micros) as u64))
            }
        }
        _ => Err(anyhow!("unsupported timestamp expression")),
    }
}

fn parse_datetime_string(text: &str) -> anyhow::Result<SystemTime> {
    let naive = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
        .map_err(|err| anyhow!("failed to parse datetime: {err}"))?;
    let datetime: chrono::DateTime<chrono::Utc> =
        chrono::DateTime::from_naive_utc_and_offset(naive, chrono::Utc);
    let seconds = datetime.timestamp();
    let nanos = datetime.timestamp_subsec_nanos();
    if seconds < 0 {
        anyhow::bail!("timestamps before UNIX epoch are not supported")
    }
    Ok(SystemTime::UNIX_EPOCH
        + Duration::from_secs(seconds as u64)
        + Duration::from_nanos(nanos as u64))
}

fn query_operation_label(op: &QueryOperation) -> &'static str {
    match op {
        QueryOperation::Range => "range",
        QueryOperation::LastBy => "last_by",
        QueryOperation::AsOf => "asof",
        QueryOperation::RollingWindow => "rolling_window",
    }
}

fn clickhouse_row_to_query_row(row: ClickhouseQueryRow) -> anyhow::Result<QueryRow> {
    let payload = BASE64_STANDARD
        .decode(row.payload_base64.as_bytes())
        .context("decode ClickHouse payload")?;
    Ok(QueryRow {
        symbol: row.symbol,
        timestamp: micros_to_system_time(row.ts_micros),
        payload: ByteBuf::from(payload),
    })
}

fn system_time_to_micros(ts: SystemTime) -> i64 {
    ts.duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_micros() as i64)
        .unwrap_or_else(|err| {
            let dur = err.duration();
            -(dur.as_micros() as i64)
        })
}

fn micros_to_system_time(micros: i64) -> SystemTime {
    if micros >= 0 {
        UNIX_EPOCH + Duration::from_micros(micros as u64)
    } else {
        UNIX_EPOCH - Duration::from_micros((-micros) as u64)
    }
}

fn apply_persisted_override(
    persisted: Option<SystemTime>,
    override_ts: Option<SystemTime>,
) -> Option<SystemTime> {
    match (persisted, override_ts) {
        (Some(persisted), Some(override_ts)) => Some(std::cmp::min(persisted, override_ts)),
        (None, Some(override_ts)) => Some(override_ts),
        (Some(persisted), None) => Some(persisted),
        (None, None) => None,
    }
}

impl ProxistDaemon {
    async fn new(
        config: DaemonConfig,
        metadata_store: SqliteMetadataStore,
        metrics_handle: Option<PrometheusHandle>,
    ) -> anyhow::Result<Self> {
        let cache = Arc::new(tokio::sync::Mutex::new(ClusterMetadata::default()));
        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));

        let wal = config.wal_dir.as_ref().map(|dir| {
            let cfg = WalConfig {
                dir: PathBuf::from(dir),
                segment_max_bytes: config.wal_segment_bytes,
                snapshot_every_rows: config.wal_snapshot_rows,
                fsync: config.wal_fsync,
            };
            WalManager::open(cfg)
        });
        let wal = match wal {
            Some(Ok(manager)) => Some(Arc::new(manager)),
            Some(Err(err)) => {
                tracing::error!(error = %err, "failed to initialize WAL; continuing without disk WAL");
                None
            }
            None => None,
        };

        let clickhouse_bundle = config.clickhouse.clone().and_then(|cfg| {
            let sink = ClickhouseHttpSink::new(cfg.clone())
                .map(|sink| Arc::new(sink) as Arc<dyn ClickhouseSink>);
            let client = ClickhouseHttpClient::new(cfg.clone()).map(|client| Arc::new(client));
            match (sink, client) {
                (Ok(sink), Ok(client)) => {
                    let target = client.target();
                    Some((sink, target, client))
                }
                (Err(err), _) | (_, Err(err)) => {
                    tracing::error!(error = %err, "failed to initialize ClickHouse components");
                    None
                }
            }
        });

        let ingest_clickhouse = clickhouse_bundle
            .as_ref()
            .map(|(sink, target, client)| (Arc::clone(sink), target.clone(), Arc::clone(client)));

        let ingest_service = Arc::new(IngestService::new(
            metadata_store.clone(),
            hot_store.clone(),
            ingest_clickhouse,
            wal.clone(),
        ));

        // Build in-memory scheduler with SQLite + optional ClickHouse
        let ch_client = clickhouse_bundle.as_ref().map(|(_, _, c)| Arc::clone(c));
        let scheduler = ProxistScheduler::new(
            ExecutorConfig {
                sqlite_path: Some(config.metadata_path.clone()),
                duckdb_path: config.duckdb_path.clone(),
                pg_url: config.pg_url.clone(),
            },
            ch_client.map(|c| (*c).clone()),
            Some(Arc::clone(&hot_store)),
        )
        .await?;
        let scheduler = Arc::new(scheduler);

        if let Some(wal) = wal.as_ref() {
            replay_wal(
                wal,
                &hot_store,
                &ingest_service,
                &metadata_store,
                config.wal_replay_persist,
            )
            .await?;
        }

        Ok(Self {
            config,
            metadata_cache: cache,
            metadata_store,
            hot_store,
            ingest_service,
            scheduler,
            metrics_handle,
            wal,
        })
    }

    #[instrument(skip(self))]
    async fn run(&self) -> anyhow::Result<()> {
        tokio::select! {
            result = self.control_loop() => { result?; },
            result = self.serve_http() => { result?; },
            result = self.serve_pg() => { result?; },
            _ = shutdown_signal() => {
                info!("shutdown signal received");
            }
        }

        Ok(())
    }

    async fn control_loop(&self) -> anyhow::Result<()> {
        info!("control loop started");
        loop {
            let snapshot = self.metadata_store.get_cluster_metadata().await?;
            self.ingest_service
                .apply_metadata(&snapshot)
                .await
                .context("apply metadata snapshot to ingest service")?;
            {
                let mut cache = self.metadata_cache.lock().await;
                *cache = snapshot.clone();
            }

            // Update scheduler's persisted cutoff based on ingest watermark.
            let persisted = self.ingest_service.last_persisted_timestamp();
            let effective = apply_persisted_override(persisted, self.config.persisted_cutoff_override);
            self.scheduler.set_persisted_cutoff(effective);
            if let Ok(summary) = self.ingest_service.hot_cold_summary().await {
                let stats: Vec<_> = summary
                    .into_iter()
                    .map(|row| proxist_mem::HotSymbolSummary {
                        tenant: row.tenant,
                        symbol: row.symbol,
                        rows: row.hot_rows,
                        first_timestamp: row.hot_first,
                        last_timestamp: row.hot_last,
                    })
                    .collect();
                self.scheduler.update_hot_stats(&stats).await;
            }

            let assigned = snapshot.assignments.len();
            let total_symbols: usize = snapshot
                .symbol_dictionaries
                .values()
                .map(|symbols| symbols.len())
                .sum();
            info!(
                path = %self.config.metadata_path,
                assignments = assigned,
                symbols = total_symbols,
                "metadata snapshot refreshed"
            );

            let tracker_snapshot = self.ingest_service.tracker_snapshot().await;
            let wal_backlog_bytes = self.wal.as_ref().map(|wal| wal.backlog_bytes()).unwrap_or(0);
            for tracker in tracker_snapshot {
                let health = ShardHealth {
                    shard_id: tracker.shard_id.clone(),
                    is_leader: true,
                    wal_backlog_bytes,
                    clickhouse_lag_ms: 0,
                    watermark: tracker.watermark,
                    persistence_state: tracker.state.clone(),
                };
                self.metadata_store.record_shard_health(health).await?;
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn serve_http(&self) -> anyhow::Result<()> {
        let state = AppState {
            metadata: self.metadata_store.clone(),
            metadata_cache: Arc::clone(&self.metadata_cache),
            hot_store: Arc::clone(&self.hot_store),
            ingest: Arc::clone(&self.ingest_service),
            scheduler: Arc::clone(&self.scheduler),
            metrics: self.metrics_handle.clone(),
            api_token: self.config.api_token.clone(),
            persisted_cutoff_override: self.config.persisted_cutoff_override,
            http_dialect: self.config.http_dialect.clone(),
        };

        async fn status_handler(
            State(state): State<AppState>,
        ) -> Result<Json<StatusResponse>, AppError> {
            let status = compose_status(&state).await?;
            Ok(Json(status))
        }

        async fn upsert_assignments_handler(
            State(state): State<AppState>,
            Json(assignments): Json<Vec<ShardAssignment>>,
        ) -> Result<StatusCode, AppError> {
            for assignment in &assignments {
                state
                    .metadata
                    .put_shard_assignment(assignment.clone())
                    .await?;
            }

            let snapshot = state.metadata.get_cluster_metadata().await?;
            {
                let mut cache = state.metadata_cache.lock().await;
                *cache = snapshot;
            }

            Ok(StatusCode::NO_CONTENT)
        }

        async fn upsert_symbols_handler(
            State(state): State<AppState>,
            Json(specs): Json<Vec<SymbolDictionarySpec>>,
        ) -> Result<StatusCode, AppError> {
            for spec in &specs {
                state
                    .metadata
                    .upsert_symbols(&spec.tenant, &spec.symbols)
                    .await?;
            }

            let snapshot = state.metadata.get_cluster_metadata().await?;
            {
                let mut cache = state.metadata_cache.lock().await;
                *cache = snapshot;
            }

            Ok(StatusCode::NO_CONTENT)
        }

        async fn metrics_handler(State(state): State<AppState>) -> Result<Response, AppError> {
            if let Some(handle) = &state.metrics {
                let body = handle.render();
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
                    .body(body.into())
                    .unwrap();
                Ok(response)
            } else {
                let response = Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body("metrics disabled".into())
                    .unwrap();
                Ok(response)
            }
        }

        async fn diagnostics_handler(
            State(state): State<AppState>,
        ) -> Result<Json<DiagnosticsBundle>, AppError> {
            let bundle = compose_diagnostics(&state).await?;
            Ok(Json(bundle))
        }

        async fn clickhouse_sql_handler(
            State(state): State<AppState>,
            body: Bytes,
        ) -> Result<Response, AppError> {
            let sql_text = String::from_utf8(body.to_vec())?.trim().to_string();
            if sql_text.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/plain; charset=UTF-8")
                    .body(Body::from(String::new()))
                    .unwrap());
            }

            let outputs = execute_sql_batch(&state, state.http_dialect.clone(), &sql_text).await?;
            let mut body = String::new();
            for output in outputs {
                let chunk = match output {
                    SqlBatchResult::Text(text) => text,
                    SqlBatchResult::Scheduler(result) => match result {
                        SqlResult::Clickhouse(ClickhouseWire { format, body }) => {
                            if let ClickhouseWireFormat::Unknown = format {
                                // Future formats will be exposed once the API supports them.
                            }
                            body
                        }
                        SqlResult::Text(text) => text,
                        SqlResult::Rows(rows) => render_rows_as_jsoneachrow(rows),
                    },
                };
                body.push_str(&chunk);
            }
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/plain; charset=UTF-8")
                .body(Body::from(body))
                .unwrap())
        }

        async fn auth_middleware(
            State(token): State<Option<String>>,
            req: Request<Body>,
            next: Next,
        ) -> Result<Response, AppError> {
            if let Some(expected) = token {
                let authorized = req
                    .headers()
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|header| header.strip_prefix("Bearer "))
                    .map(|bearer| bearer == expected)
                    .unwrap_or(false);

                if !authorized {
                    let response = Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(json!({ "error": "unauthorized" }).to_string().into())
                        .unwrap();
                    return Ok(response);
                }
            }

            Ok(next.run(req).await)
        }

        async fn query_handler(
            State(state): State<AppState>,
            Json(request): Json<QueryRequest>,
        ) -> Result<Json<QueryResponse>, AppError> {
            let include_cold = request.include_cold;
            let op_kind = request.op.clone();
            let span = tracing::info_span!(
                "query",
                op = ?op_kind,
                include_cold,
                symbols = request.symbols.len()
            );
            let _guard = span.enter();
            let start = Instant::now();

            let response = execute_query(&state, request).await?;

            let op_label = query_operation_label(&op_kind);
            let include_cold_label = if include_cold { "true" } else { "false" };
            metrics::counter!(
                "proxist_query_requests_total",
                1,
                "op" => op_label,
                "include_cold" => include_cold_label
            );
            metrics::histogram!(
                "proxist_query_latency_usec",
                start.elapsed().as_secs_f64() * 1_000_000.0,
                "op" => op_label
            );

            Ok(Json(response))
        }

        async fn health_handler(
            State(state): State<AppState>,
            Json(health): Json<ShardHealth>,
        ) -> Result<StatusCode, AppError> {
            state.metadata.record_shard_health(health).await?;
            Ok(StatusCode::ACCEPTED)
        }

        let auth_token = state.api_token.clone();

        let app = Router::new()
            .route("/", post(clickhouse_sql_handler))
            .route("/status", get(status_handler))
            .route("/query", post(query_handler))
            .route("/assignments", post(upsert_assignments_handler))
            .route("/metrics", get(metrics_handler))
            .route("/diagnostics", get(diagnostics_handler))
            .route("/symbols", post(upsert_symbols_handler))
            .route("/health", post(health_handler))
            .with_state(state.clone());

        let app = if auth_token.is_some() {
            app.layer(middleware::from_fn_with_state(auth_token, auth_middleware))
        } else {
            app
        };

        let listener = tokio::net::TcpListener::bind(self.config.http_addr).await?;
        info!(addr = %self.config.http_addr, "HTTP server listening");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        Ok(())
    }

    async fn serve_pg(&self) -> anyhow::Result<()> {
        let Some(pg_addr) = self.config.pg_addr else {
            std::future::pending::<()>().await;
            return Ok(());
        };
        let state = AppState {
            metadata: self.metadata_store.clone(),
            metadata_cache: Arc::clone(&self.metadata_cache),
            hot_store: Arc::clone(&self.hot_store),
            ingest: Arc::clone(&self.ingest_service),
            scheduler: Arc::clone(&self.scheduler),
            metrics: self.metrics_handle.clone(),
            api_token: self.config.api_token.clone(),
            persisted_cutoff_override: self.config.persisted_cutoff_override,
            http_dialect: self.config.http_dialect.clone(),
        };
        pgwire_server::serve(pg_addr, Arc::new(state), self.config.pg_dialect.clone()).await
    }
}

async fn replay_wal(
    wal: &Arc<WalManager>,
    hot_store: &Arc<dyn HotColumnStore>,
    ingest: &Arc<IngestService>,
    metadata: &SqliteMetadataStore,
    persist_replay: bool,
) -> anyhow::Result<()> {
    let snapshot = wal.load_snapshot()?;
    let mut after_seq = 0_u64;

    if let Some(snapshot) = snapshot {
        tracing::info!(
            last_seq = snapshot.last_seq,
            bytes = snapshot.bytes.len(),
            "restoring hot store from WAL snapshot"
        );
        hot_store.restore_snapshot(&snapshot.bytes).await?;
        ingest.hydrate_from_hot_store().await?;
        after_seq = snapshot.last_seq;
    }

    let records = wal.replay_from(after_seq)?;
    if records.is_empty() {
        return Ok(());
    }

    tracing::info!(rows = records.len(), "replaying WAL records into hot store");
    ingest.warm_from_records(&records).await?;

    if persist_replay {
        let persisted = metadata.list_shard_health().await?;
        let mut persisted_map: HashMap<String, SystemTime> = HashMap::new();
        for health in persisted {
            if let Some(ts) = health.watermark.persisted {
                persisted_map.insert(health.shard_id, ts);
            }
        }

        let filtered: Vec<IngestRecord> = records
            .into_iter()
            .filter(|record| match persisted_map.get(&record.shard_id) {
                Some(cutoff) => record.timestamp > *cutoff,
                None => true,
            })
            .collect();

        if !filtered.is_empty() {
            tracing::info!(rows = filtered.len(), "persisting replayed WAL rows");
            ingest.persist_replayed(&filtered).await?;
        }
    }

    Ok(())
}

async fn execute_query(state: &AppState, request: QueryRequest) -> Result<QueryResponse, AppError> {
    let op_kind = request.op.clone();
    let persisted_at = apply_persisted_override(
        state.ingest.last_persisted_timestamp(),
        state.persisted_cutoff_override,
    );
    let include_cold = request.include_cold;

    let rows = match &op_kind {
        QueryOperation::Range => {
            let mut hot_rows = state
                .hot_store
                .scan_range(&request.tenant, &request.range, &request.symbols)
                .await?;
            if include_cold {
                if let Some(persisted) = persisted_at {
                    hot_rows.retain(|row| row.timestamp > persisted);
                }
            }
            let mut rows: Vec<QueryRow> = hot_rows.into_iter().map(hot_row_to_query_row).collect();

            if include_cold {
                if let Some(client) = state.ingest.clickhouse_client() {
                    let cold_end = persisted_at
                        .map(|persisted| std::cmp::min(persisted, request.range.end))
                        .unwrap_or(request.range.end);

                    if !request.symbols.is_empty() && cold_end >= request.range.start {
                        let cold_rows = client
                            .fetch_range(
                                &request.tenant,
                                &request.symbols,
                                system_time_to_micros(request.range.start),
                                system_time_to_micros(cold_end),
                            )
                            .await?
                            .into_iter()
                            .map(clickhouse_row_to_query_row)
                            .collect::<anyhow::Result<Vec<_>>>()?;

                        rows.extend(cold_rows);
                    }
                }
            }

            rows.sort_by_key(|row| (system_time_to_micros(row.timestamp), row.symbol.clone()));
            rows.dedup_by(|a, b| {
                a.symbol == b.symbol
                    && a.timestamp == b.timestamp
                    && a.payload.as_ref() == b.payload.as_ref()
            });

            Ok::<_, AppError>(rows)
        }
        QueryOperation::LastBy | QueryOperation::AsOf => {
            let mut hot_rows = match op_kind {
                QueryOperation::LastBy => {
                    state
                        .hot_store
                        .last_by(&request.tenant, &request.symbols, request.range.end)
                        .await?
                }
                QueryOperation::AsOf => {
                    state
                        .hot_store
                        .asof(&request.tenant, &request.symbols, request.range.end)
                        .await?
                }
                _ => unreachable!(),
            };

            if include_cold {
                if let Some(persisted) = persisted_at {
                    hot_rows.retain(|row| row.timestamp > persisted);
                }
            }

            let mut map: HashMap<String, QueryRow> = HashMap::new();

            let mut upsert = |row: QueryRow| match map.entry(row.symbol.clone()) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(row);
                }
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    if system_time_to_micros(row.timestamp)
                        > system_time_to_micros(slot.get().timestamp)
                    {
                        slot.insert(row);
                    }
                }
            };

            for row in hot_rows {
                upsert(hot_row_to_query_row(row));
            }

            if include_cold {
                if let Some(client) = state.ingest.clickhouse_client() {
                    let cutoff = persisted_at
                        .map(|persisted| std::cmp::min(persisted, request.range.end))
                        .unwrap_or(request.range.end);

                    if !request.symbols.is_empty() {
                        let cold_rows = client
                            .fetch_last_by(
                                &request.tenant,
                                &request.symbols,
                                system_time_to_micros(cutoff),
                            )
                            .await?
                            .into_iter()
                            .map(clickhouse_row_to_query_row)
                            .collect::<anyhow::Result<Vec<_>>>()?;

                        for row in cold_rows {
                            upsert(row);
                        }
                    }
                }

                if let Some(seam_ts) = persisted_at {
                    let seam_rows = state
                        .hot_store
                        .seam_rows_at(&request.tenant, seam_ts)
                        .await?;
                    for seam in seam_rows {
                        if !request.symbols.is_empty() && !request.symbols.contains(&seam.symbol) {
                            continue;
                        }
                        if seam.timestamp > request.range.end {
                            continue;
                        }
                        upsert(QueryRow {
                            symbol: seam.symbol,
                            timestamp: seam.timestamp,
                            payload: ByteBuf::from(seam.payload),
                        });
                    }
                }
            }

            let mut rows: Vec<QueryRow> = if request.symbols.is_empty() {
                map.into_values().collect()
            } else {
                request
                    .symbols
                    .iter()
                    .filter_map(|symbol| map.get(symbol).cloned())
                    .collect()
            };
            rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));

            Ok::<_, AppError>(rows)
        }
        QueryOperation::RollingWindow => {
            if request.symbols.is_empty() {
                return Err(AppError(anyhow!(
                    "rolling_window queries require at least one symbol"
                )));
            }
            let config = request.rolling.clone().ok_or_else(|| {
                AppError(anyhow!(
                    "rolling configuration is required for rolling_window queries"
                ))
            })?;
            if !matches!(config.aggregation, RollingAggregation::Count) {
                return Err(AppError(anyhow!(
                    "unsupported rolling aggregation: {:?}",
                    config.aggregation
                )));
            }

            let window = config.window();
            let hot_lower_bound = if include_cold { persisted_at } else { None };
            let mut counts: HashMap<String, u64> = HashMap::new();
            for symbol in &request.symbols {
                counts.entry(symbol.clone()).or_insert(0);
            }

            let hot_counts = state
                .hot_store
                .rolling_window(
                    &request.tenant,
                    &request.symbols,
                    request.range.end,
                    window,
                    hot_lower_bound,
                )
                .await?;
            for entry in hot_counts {
                *counts.entry(entry.symbol).or_insert(0) += entry.count;
            }

            if include_cold {
                if let (Some(client), Some(persisted)) =
                    (state.ingest.clickhouse_client(), persisted_at)
                {
                    let cold_end = std::cmp::min(persisted, request.range.end);
                    let window_start = request
                        .range
                        .end
                        .checked_sub(window)
                        .unwrap_or(SystemTime::UNIX_EPOCH);
                    if cold_end >= window_start {
                        let cold_rows = client
                            .fetch_range(
                                &request.tenant,
                                &request.symbols,
                                system_time_to_micros(window_start),
                                system_time_to_micros(cold_end),
                            )
                            .await?;
                        for row in cold_rows {
                            *counts.entry(row.symbol.clone()).or_insert(0) += 1;
                        }
                    }
                }
            }

            let mut rows = Vec::with_capacity(counts.len().max(request.symbols.len()).max(1));
            for symbol in request.symbols.clone() {
                let count = *counts.get(&symbol).unwrap_or(&0);
                let payload = serde_json::to_vec(&json!({
                    "aggregation": "count",
                    "window_micros": config.length_micros,
                    "count": count,
                }))
                .map_err(|err| AppError(anyhow!(err)))?;
                rows.push(QueryRow {
                    symbol,
                    timestamp: request.range.end,
                    payload: ByteBuf::from(payload),
                });
            }
            rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));

            Ok::<_, AppError>(rows)
        }
    }?;

    Ok(QueryResponse { rows })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod system_summary_tests {
    use super::*;

    #[test]
    fn neutral_alias_detects_ingest_summary() {
        let sql = "SELECT group_key, entity_key FROM proxist.__system_ingest_summary";
        match match_system_summary_query(sql) {
            Some(SystemSummaryView::Neutral) => {}
            other => panic!("expected neutral view, got {:?}", other),
        }
    }

    #[test]
    fn legacy_alias_detects_hot_summary() {
        let sql = "SELECT * FROM system.proxist_hot_summary";
        match match_system_summary_query(sql) {
            Some(SystemSummaryView::Legacy) => {}
            other => panic!("expected legacy view, got {:?}", other),
        }
    }
}

#[cfg(test)]
mod ingest_tests {
    use super::*;
    use proxist_core::query::{QueryOperation, QueryRange};
    use std::time::Duration;
    use tempfile::NamedTempFile;

    #[test]
    fn build_ingest_records_maps_annotation_columns() -> anyhow::Result<()> {
        let sql = "INSERT INTO ticks (tenant_key, symbol_key, ts_micros, payload_b64, seq_no) \
                   VALUES ('alpha', 'AAPL', 42, 'AQID', 7)";
        let dialect = DialectMode::Sql(SqlDialect::ClickHouse);
        let mut parsed = parse_sql_with_dialect(&dialect, sql)?;
        let stmt = parsed.remove(0);
        let (columns, values) = match stmt {
            Statement::Insert { columns, source, .. } => match source.map(|q| *q.body) {
                Some(SetExpr::Values(values)) => (columns, values),
                _ => bail!("expected VALUES insert"),
            },
            _ => bail!("expected insert"),
        };

        let cfg = crate::scheduler::TableConfig {
            ddl: "CREATE TABLE ticks (tenant_key String, symbol_key String, ts_micros Int64, payload_b64 String, seq_no UInt64)".to_string(),
            order_col: "ts_micros".to_string(),
            payload_col: "payload_b64".to_string(),
            filter_cols: vec!["tenant_key".to_string(), "symbol_key".to_string()],
            seq_col: Some("seq_no".to_string()),
            columns: vec![
                "tenant_key".to_string(),
                "symbol_key".to_string(),
                "ts_micros".to_string(),
                "payload_b64".to_string(),
                "seq_no".to_string(),
            ],
        };

        let mut seq = 0;
        let records = build_ingest_records(&cfg, &columns, values, &mut seq)?;
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.tenant, "alpha");
        assert_eq!(record.symbol, "AAPL");
        assert_eq!(record.payload, vec![1, 2, 3]);
        assert_eq!(record.seq, 7);
        assert_eq!(system_time_to_micros(record.timestamp), 42);

        Ok(())
    }

    #[tokio::test]
    async fn last_by_include_cold_uses_seam_rows() -> anyhow::Result<()> {
        let metadata_file = NamedTempFile::new()?;
        let metadata_path = metadata_file.path().to_str().unwrap().to_string();
        let metadata = SqliteMetadataStore::connect(&metadata_path).await?;
        metadata
            .put_shard_assignment(ShardAssignment {
                shard_id: "alpha-shard".into(),
                tenant_id: "alpha".into(),
                node_id: "node-a".into(),
                symbol_range: ("A".into(), "Z".into()),
            })
            .await?;

        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let service = Arc::new(IngestService::new(
            metadata.clone(),
            hot_store.clone(),
            None,
            None,
        ));

        let snapshot = metadata.get_cluster_metadata().await?;
        service.apply_metadata(&snapshot).await?;
        let scheduler = Arc::new(
            ProxistScheduler::new(
                ExecutorConfig {
                    sqlite_path: None,
                    duckdb_path: None,
                    pg_url: None,
                },
                None,
                Some(hot_store.clone()),
            )
            .await?,
        );

        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let records = vec![IngestRecord {
            tenant: "alpha".into(),
            shard_id: String::new(),
            symbol: "AAPL".into(),
            timestamp: ts,
            payload: vec![1],
            seq: 1,
        }];
        service.ingest_records(records).await?;

        let state = AppState {
            metadata: metadata.clone(),
            metadata_cache: Arc::new(tokio::sync::Mutex::new(snapshot)),
            hot_store: hot_store.clone(),
            ingest: service.clone(),
            scheduler,
            metrics: None,
            api_token: None,
            persisted_cutoff_override: None,
            http_dialect: DialectMode::Sql(SqlDialect::ClickHouse),
        };

        let range = QueryRange::new(ts - Duration::from_millis(1), ts + Duration::from_secs(1));
        let request = QueryRequest {
            tenant: "alpha".into(),
            symbols: vec!["AAPL".into()],
            range,
            include_cold: true,
            op: QueryOperation::LastBy,
            rolling: None,
        };

        let response = execute_query(&state, request).await.map_err(|err| err.0)?;
        assert_eq!(response.rows.len(), 1);
        let row = &response.rows[0];
        assert_eq!(row.symbol, "AAPL");
        assert_eq!(row.timestamp, ts);
        assert_eq!(row.payload.as_ref(), &[1]);

        Ok(())
    }

    #[tokio::test]
    async fn diagnostics_bundle_includes_hot_summary() -> anyhow::Result<()> {
        let metadata_file = NamedTempFile::new()?;
        let metadata_path = metadata_file.path().to_str().unwrap().to_string();
        let metadata = SqliteMetadataStore::connect(&metadata_path).await?;
        metadata
            .put_shard_assignment(ShardAssignment {
                shard_id: "alpha-shard".into(),
                tenant_id: "alpha".into(),
                node_id: "node-a".into(),
                symbol_range: ("A".into(), "Z".into()),
            })
            .await?;

        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let service = Arc::new(IngestService::new(
            metadata.clone(),
            hot_store.clone(),
            None,
            None,
        ));
        let snapshot = metadata.get_cluster_metadata().await?;
        service.apply_metadata(&snapshot).await?;
        let scheduler = Arc::new(
            ProxistScheduler::new(
                ExecutorConfig {
                    sqlite_path: None,
                    duckdb_path: None,
                    pg_url: None,
                },
                None,
                Some(hot_store.clone()),
            )
            .await?,
        );

        let ts = SystemTime::UNIX_EPOCH + Duration::from_millis(500);
        service
            .ingest_records(vec![IngestRecord {
                tenant: "alpha".into(),
                shard_id: String::new(),
                symbol: "AAPL".into(),
                timestamp: ts,
                payload: vec![7],
                seq: 1,
            }])
            .await?;

        let state = AppState {
            metadata: metadata.clone(),
            metadata_cache: Arc::new(tokio::sync::Mutex::new(snapshot)),
            hot_store: hot_store.clone(),
            ingest: service.clone(),
            scheduler,
            metrics: None,
            api_token: None,
            persisted_cutoff_override: None,
            http_dialect: DialectMode::Sql(SqlDialect::ClickHouse),
        };

        let bundle = compose_diagnostics(&state).await.map_err(|err| err.0)?;
        assert!(!bundle.hot_summary.is_empty());
        let row = &bundle.hot_summary[0];
        assert_eq!(row.tenant, "alpha");
        assert_eq!(row.symbol, "AAPL");
        assert_eq!(row.hot_rows, 1);
        assert_eq!(row.hot_last_micros, Some(system_time_to_micros(ts)));
        assert!(bundle
            .persistence
            .iter()
            .any(|tracker| tracker.shard_id == "alpha-shard"));
        Ok(())
    }
}

#[cfg(test)]
mod wal_replay_tests {
    use super::*;
    use tempfile::{NamedTempFile, TempDir};

    #[tokio::test]
    async fn wal_replay_restores_hot_store() -> anyhow::Result<()> {
        let wal_dir = TempDir::new()?;
        let wal = Arc::new(WalManager::open(WalConfig {
            dir: wal_dir.path().to_path_buf(),
            segment_max_bytes: 1024 * 1024,
            snapshot_every_rows: 10,
            fsync: false,
        })?);

        let metadata_file = NamedTempFile::new()?;
        let metadata_path = metadata_file.path().to_str().unwrap().to_string();
        let metadata = SqliteMetadataStore::connect(&metadata_path).await?;
        metadata
            .put_shard_assignment(ShardAssignment {
                shard_id: "alpha-shard".into(),
                tenant_id: "alpha".into(),
                node_id: "node-a".into(),
                symbol_range: ("A".into(), "Z".into()),
            })
            .await?;

        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let service = Arc::new(IngestService::new(
            metadata.clone(),
            hot_store.clone(),
            None,
            Some(wal.clone()),
        ));

        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        service
            .ingest_records(vec![IngestRecord {
                tenant: "alpha".into(),
                shard_id: String::new(),
                symbol: "AAPL".into(),
                timestamp: ts,
                payload: vec![9],
                seq: 1,
            }])
            .await?;

        let replay_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let replay_service = Arc::new(IngestService::new(
            metadata.clone(),
            replay_store.clone(),
            None,
            Some(wal.clone()),
        ));

        replay_wal(&wal, &replay_store, &replay_service, &metadata, false).await?;
        let summary = replay_service.hot_cold_summary().await?;
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].symbol, "AAPL");
        assert_eq!(summary[0].hot_rows, 1);

        Ok(())
    }
}
