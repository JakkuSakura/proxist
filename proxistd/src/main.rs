mod ingest;

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
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
use ingest::IngestService;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use proxist_api::{
    DiagnosticsBundle, IngestBatchRequest, IngestTick, QueryRequest, QueryResponse, StatusResponse,
    SymbolDictionarySpec,
};
use proxist_ch::{
    ClickhouseConfig, ClickhouseHttpClient, ClickhouseHttpSink, ClickhouseQueryRow, ClickhouseSink,
};
use proxist_core::{
    metadata::ClusterMetadata, query::QueryOperation, MetadataStore, ShardAssignment, ShardHealth,
    ShardPersistenceTracker,
};
use proxist_mem::{HotColumnStore, InMemoryHotColumnStore, MemConfig};
use proxist_metadata_sqlite::SqliteMetadataStore;
use proxist_wal::{FileWal, InMemoryWal, WalConfig, WalRecord, WalWriter};
use serde_bytes::ByteBuf;
use serde_json::json;
use sqlparser::{
    ast::{Expr, FunctionArg, FunctionArgExpr, Ident, SetExpr, Statement, Value},
    dialect::ClickHouseDialect,
    parser::Parser,
};
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
    clickhouse: Option<ClickhouseConfig>,
    api_token: Option<String>,
}

impl DaemonConfig {
    fn load() -> anyhow::Result<Self> {
        let metadata_path = std::env::var("PROXIST_METADATA_SQLITE_PATH")
            .unwrap_or_else(|_| "./proxist-meta.db".into());
        let http_addr = std::env::var("PROXIST_HTTP_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".into())
            .parse()?;
        let clickhouse = load_clickhouse_config();
        let api_token = std::env::var("PROXIST_API_TOKEN").ok();

        Ok(Self {
            metadata_path,
            http_addr,
            clickhouse,
            api_token,
        })
    }
}

struct WalBootstrap {
    writer: Arc<dyn WalWriter>,
    records: Vec<WalRecord>,
}

async fn wal_from_env() -> anyhow::Result<Option<WalBootstrap>> {
    if let Ok(dir) = env::var("PROXIST_WAL_DIR") {
        let mut config = WalConfig::default();
        config.directory = PathBuf::from(dir);
        let wal = FileWal::open(config.clone()).await?;
        let records = wal
            .load_snapshot_and_replay()
            .await
            .context("load WAL snapshot and tail records")?;
        let writer: Arc<dyn WalWriter> = Arc::new(wal);
        return Ok(Some(WalBootstrap { writer, records }));
    }
    Ok(None)
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
    metrics_handle: Option<PrometheusHandle>,
}

#[derive(Clone)]
struct AppState {
    metadata: SqliteMetadataStore,
    metadata_cache: Arc<tokio::sync::Mutex<ClusterMetadata>>,
    hot_store: Arc<dyn HotColumnStore>,
    ingest: Arc<IngestService>,
    metrics: Option<PrometheusHandle>,
    api_token: Option<String>,
}

fn hot_row_to_query_row(row: proxist_mem::HotRow) -> proxist_api::QueryRow {
    proxist_api::QueryRow {
        symbol: row.symbol,
        timestamp: row.timestamp,
        payload: ByteBuf::from(row.payload),
    }
}

fn split_sql_statements(input: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut prev_char: Option<char> = None;

    for ch in input.chars() {
        match ch {
            '\'' if !in_double && !matches!(prev_char, Some('\\')) => {
                in_single = !in_single;
                current.push(ch);
            }
            '"' if !in_single && !matches!(prev_char, Some('\\')) => {
                in_double = !in_double;
                current.push(ch);
            }
            ';' if !in_single && !in_double => {
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
        prev_char = Some(ch);
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    statements
}

fn replace_engine_case_insensitive(sql: &str, new_engine: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if let Some(pos) = lower.find("mixedmergetree") {
        let end = pos + "mixedmergetree".len();
        let mut result = String::with_capacity(sql.len() + new_engine.len());
        result.push_str(&sql[..pos]);
        result.push_str(new_engine);
        result.push_str(&sql[end..]);
        result
    } else {
        sql.to_string()
    }
}

fn ensure_engine_clause(sql: &str, engine: &str) -> String {
    if sql.to_ascii_lowercase().contains("engine") {
        return sql.to_string();
    }

    let lower = sql.to_ascii_lowercase();
    if let Some(order_pos) = lower.find("order by") {
        let (before, after) = sql.split_at(order_pos);
        format!("{} ENGINE = {} {}", before.trim_end(), engine, after)
    } else {
        let trimmed = sql.trim_end();
        let has_semicolon = trimmed.ends_with(';');
        let base = trimmed.trim_end_matches(';').trim_end();
        if has_semicolon {
            format!("{} ENGINE = {};", base, engine)
        } else {
            format!("{} ENGINE = {}", base, engine)
        }
    }
}

async fn forward_sql_to_clickhouse(state: &AppState, sql: &str) -> Result<String, AppError> {
    let client = state
        .ingest
        .clickhouse_client()
        .ok_or_else(|| AppError(anyhow!("ClickHouse client not configured")))?;
    let text = client.execute_raw(sql).await?;
    Ok(text)
}

fn build_ingest_ticks(
    columns: &[Ident],
    values: sqlparser::ast::Values,
    seq_counter: &mut u64,
) -> anyhow::Result<Vec<IngestTick>> {
    let column_names: Vec<String> = if columns.is_empty() {
        vec!["tenant", "symbol", "ts", "seq"]
            .into_iter()
            .map(String::from)
            .collect()
    } else {
        columns.iter().map(|c| c.value.to_lowercase()).collect()
    };

    let mut ticks = Vec::new();
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
            match col.as_str() {
                "tenant" => tenant = Some(expr_to_string(expr)?),
                "symbol" => symbol = Some(expr_to_string(expr)?),
                "shard_id" | "table" => {}
                "ts" | "timestamp" | "ts_micros" => timestamp = Some(expr_to_timestamp(expr)?),
                "seq" => seq = Some(expr_to_u64(expr)?),
                "payload" => payload_bytes = Some(expr_to_bytes(expr)?),
                "payload_base64" => payload_bytes = Some(expr_to_base64_bytes(expr)?),
                other => anyhow::bail!("unsupported column in INSERT: {other}"),
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

        ticks.push(IngestTick {
            tenant,
            symbol,
            timestamp,
            payload: ByteBuf::from(payload_bytes.unwrap_or_default()),
            seq,
        });
    }

    Ok(ticks)
}

fn expr_to_string(expr: &Expr) -> anyhow::Result<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        _ => Err(anyhow!("expected string literal")),
    }
}

fn expr_to_bytes(expr: &Expr) -> anyhow::Result<Vec<u8>> {
    Ok(expr_to_string(expr)?.into_bytes())
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
    }
}

fn clickhouse_row_to_query_row(row: ClickhouseQueryRow) -> anyhow::Result<proxist_api::QueryRow> {
    let payload = BASE64_STANDARD
        .decode(row.payload_base64.as_bytes())
        .context("decode ClickHouse payload")?;
    Ok(proxist_api::QueryRow {
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

impl ProxistDaemon {
    async fn new(
        config: DaemonConfig,
        metadata_store: SqliteMetadataStore,
        metrics_handle: Option<PrometheusHandle>,
    ) -> anyhow::Result<Self> {
        let cache = Arc::new(tokio::sync::Mutex::new(ClusterMetadata::default()));
        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let wal_bootstrap = wal_from_env().await?;
        let (wal, replay_records): (Arc<dyn WalWriter>, Vec<WalRecord>) = match wal_bootstrap {
            Some(b) => (b.writer, b.records),
            None => (Arc::new(InMemoryWal::new()), Vec::new()),
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
            wal.clone(),
            hot_store.clone(),
            ingest_clickhouse,
        ));

        if !replay_records.is_empty() {
            ingest_service
                .warm_from_records(&replay_records)
                .await
                .context("replay WAL records into hot store")?;
        }

        Ok(Self {
            config,
            metadata_cache: cache,
            metadata_store,
            hot_store,
            ingest_service,
            metrics_handle,
        })
    }

    #[instrument(skip(self))]
    async fn run(&self) -> anyhow::Result<()> {
        tokio::select! {
            result = self.control_loop() => { result?; },
            result = self.serve_http() => { result?; },
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
            {
                let mut cache = self.metadata_cache.lock().await;
                *cache = snapshot;
            }

            info!(path = %self.config.metadata_path, "metadata snapshot refreshed");

            // TODO: hook in metadata-driven ingest and shard supervision.
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Example seam tracker update.
            let mut tracker = ShardPersistenceTracker::new("shard-0");
            tracker
                .apply(proxist_core::PersistenceTransition::Reset)
                .context("reset tracker")?;

            let health = ShardHealth {
                shard_id: "shard-0".to_string(),
                is_leader: true,
                wal_backlog_bytes: 0,
                clickhouse_lag_ms: 0,
                watermark: tracker.watermark,
                persistence_state: tracker.state.clone(),
            };

            self.metadata_store.record_shard_health(health).await?;
        }
    }

    async fn serve_http(&self) -> anyhow::Result<()> {
        let state = AppState {
            metadata: self.metadata_store.clone(),
            metadata_cache: Arc::clone(&self.metadata_cache),
            hot_store: Arc::clone(&self.hot_store),
            ingest: Arc::clone(&self.ingest_service),
            metrics: self.metrics_handle.clone(),
            api_token: self.config.api_token.clone(),
        };

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

        async fn ingest_handler(
            State(state): State<AppState>,
            Json(request): Json<IngestBatchRequest>,
        ) -> Result<StatusCode, AppError> {
            state.ingest.ingest(request).await?;
            Ok(StatusCode::ACCEPTED)
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
            let status = compose_status(&state).await?;
            let metrics = state.metrics.as_ref().map(|handle| handle.render());
            let bundle = DiagnosticsBundle {
                captured_at: SystemTime::now(),
                status,
                metrics,
            };
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

            let dialect = ClickHouseDialect {};
            let mut outputs = Vec::new();
            let mut seq_counter: u64 = 0;
            let ingest_table = state
                .ingest
                .clickhouse_table()
                .map(|t| t.to_ascii_lowercase());

            for stmt_text in split_sql_statements(&sql_text) {
                let trimmed = stmt_text.trim_start();
                if trimmed.to_ascii_lowercase().starts_with("create") {
                    let mut parsed = Parser::parse_sql(&dialect, &stmt_text)
                        .map_err(|err| AppError(anyhow!("failed to parse SQL: {err}")))?;
                    if parsed.is_empty() {
                        continue;
                    }
                    if parsed.len() != 1 {
                        let raw = forward_sql_to_clickhouse(&state, &stmt_text).await?;
                        outputs.push(raw);
                        continue;
                    }
                    let mut stmt = parsed.remove(0);
                    if let Statement::CreateTable { engine, .. } = &mut stmt {
                        let mut forwarded = stmt_text.to_string();
                        if let Some(e) = engine {
                            if e.eq_ignore_ascii_case("mixedmergetree") {
                                forwarded =
                                    replace_engine_case_insensitive(&forwarded, "MergeTree");
                            }
                        } else {
                            forwarded = ensure_engine_clause(&forwarded, "MergeTree");
                        }
                        let raw = forward_sql_to_clickhouse(&state, &forwarded).await?;
                        outputs.push(raw);
                        continue;
                    }
                    let raw = forward_sql_to_clickhouse(&state, &stmt_text).await?;
                    outputs.push(raw);
                    continue;
                }

                if !trimmed.to_ascii_lowercase().starts_with("insert") {
                    let raw = forward_sql_to_clickhouse(&state, &stmt_text).await?;
                    outputs.push(raw);
                    continue;
                }

                let parsed = Parser::parse_sql(&dialect, &stmt_text)
                    .map_err(|err| AppError(anyhow!("failed to parse SQL: {err}")))?;
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
                        if let Some(expected) = ingest_table.as_ref() {
                            if &table_name_lower != expected {
                                let raw = forward_sql_to_clickhouse(&state, &stmt_text).await?;
                                outputs.push(raw);
                                continue;
                            }
                        }
                        if let Some(query) = source {
                            match *query.body {
                                SetExpr::Values(values) => {
                                    let ticks =
                                        build_ingest_ticks(&columns, values, &mut seq_counter)?;
                                    if !ticks.is_empty() {
                                        state.ingest.ingest(IngestBatchRequest { ticks }).await?;
                                    }
                                    outputs.push("Ok.\n".to_string());
                                }
                                _ => {
                                    let raw = forward_sql_to_clickhouse(&state, &stmt_text).await?;
                                    outputs.push(raw);
                                }
                            }
                        } else {
                            let raw = forward_sql_to_clickhouse(&state, &stmt_text).await?;
                            outputs.push(raw);
                        }
                    }
                    _ => {
                        let raw = forward_sql_to_clickhouse(&state, &stmt_text).await?;
                        outputs.push(raw);
                    }
                }
            }

            let body = outputs.join("");
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
            let op_kind = request.op.clone();
            let persisted_at = state.ingest.last_persisted_timestamp();
            let include_cold = request.include_cold;
            let span = tracing::info_span!(
                "query",
                op = ?op_kind,
                include_cold,
                symbols = request.symbols.len()
            );
            let _guard = span.enter();
            let start = Instant::now();

            let mut hot_rows = match &op_kind {
                QueryOperation::Range => {
                    state
                        .hot_store
                        .scan_range(&request.tenant, &request.range, &request.symbols)
                        .await?
                }
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
            };

            if include_cold {
                if let Some(persisted) = persisted_at {
                    hot_rows.retain(|row| row.timestamp > persisted);
                }
            }

            let rows = match &op_kind {
                QueryOperation::Range => {
                    let mut rows: Vec<proxist_api::QueryRow> =
                        hot_rows.into_iter().map(hot_row_to_query_row).collect();

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

                    rows.sort_by_key(|row| {
                        (system_time_to_micros(row.timestamp), row.symbol.clone())
                    });
                    rows.dedup_by(|a, b| {
                        a.symbol == b.symbol
                            && a.timestamp == b.timestamp
                            && a.payload.as_ref() == b.payload.as_ref()
                    });

                    Ok::<_, AppError>(rows)
                }
                QueryOperation::LastBy | QueryOperation::AsOf => {
                    let mut map: HashMap<String, proxist_api::QueryRow> = HashMap::new();

                    let mut upsert =
                        |row: proxist_api::QueryRow| match map.entry(row.symbol.clone()) {
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
                                if !request.symbols.is_empty()
                                    && !request.symbols.contains(&seam.symbol)
                                {
                                    continue;
                                }
                                if seam.timestamp > request.range.end {
                                    continue;
                                }
                                upsert(proxist_api::QueryRow {
                                    symbol: seam.symbol,
                                    timestamp: seam.timestamp,
                                    payload: ByteBuf::from(seam.payload),
                                });
                            }
                        }
                    }

                    let mut rows: Vec<proxist_api::QueryRow> = if request.symbols.is_empty() {
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
            }?;

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

            Ok(Json(QueryResponse { rows }))
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
            .route("/ingest", post(ingest_handler))
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
