mod ingest;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use ingest::IngestService;
use proxist_api::{IngestBatchRequest, QueryRequest, QueryResponse, StatusResponse};
use proxist_ch::{ClickhouseConfig, ClickhouseHttpSink, ClickhouseSink};
use proxist_core::{
    metadata::ClusterMetadata, MetadataStore, ShardAssignment, ShardHealth, ShardPersistenceTracker,
};
use proxist_mem::{HotColumnStore, InMemoryHotColumnStore, MemConfig};
use proxist_metadata_sqlite::SqliteMetadataStore;
use proxist_wal::{InMemoryWal, WalWriter};
use serde_bytes::ByteBuf;
use serde_json::json;
use tokio::signal;
use tracing::{error, info, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let config = DaemonConfig::load()?;

    info!(?config, "starting proxistd");

    let metadata_store = SqliteMetadataStore::connect(&config.metadata_path).await?;
    let daemon = ProxistDaemon::new(config, metadata_store);
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

#[derive(Debug, Clone)]
struct DaemonConfig {
    metadata_path: String,
    http_addr: SocketAddr,
    clickhouse: Option<ClickhouseConfig>,
}

impl DaemonConfig {
    fn load() -> anyhow::Result<Self> {
        let metadata_path = std::env::var("PROXIST_METADATA_SQLITE_PATH")
            .unwrap_or_else(|_| "./proxist-meta.db".into());
        let http_addr = std::env::var("PROXIST_HTTP_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".into())
            .parse()?;
        let clickhouse = load_clickhouse_config();
        Ok(Self {
            metadata_path,
            http_addr,
            clickhouse,
        })
    }
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

    Some(ClickhouseConfig {
        endpoint,
        database,
        table,
        username,
        password,
        insert_batch_rows,
        timeout_secs,
    })
}

struct ProxistDaemon {
    config: DaemonConfig,
    metadata_cache: Arc<tokio::sync::Mutex<ClusterMetadata>>,
    metadata_store: SqliteMetadataStore,
    hot_store: Arc<dyn HotColumnStore>,
    _wal: Arc<dyn WalWriter>,
    ingest_service: Arc<IngestService>,
}

impl ProxistDaemon {
    fn new(config: DaemonConfig, metadata_store: SqliteMetadataStore) -> Self {
        let cache = Arc::new(tokio::sync::Mutex::new(ClusterMetadata::default()));
        let hot_store: Arc<dyn HotColumnStore> =
            Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
        let wal: Arc<dyn WalWriter> = Arc::new(InMemoryWal::new());

        let clickhouse_pair = config.clickhouse.clone().and_then(|cfg| {
            let cfg_clone = cfg.clone();
            ClickhouseHttpSink::new(cfg_clone)
                .map(|sink| {
                    let target = proxist_api::ClickhouseTarget {
                        endpoint: cfg.endpoint.clone(),
                        database: cfg.database.clone(),
                        table: cfg.table.clone(),
                    };
                    (Arc::new(sink) as Arc<dyn ClickhouseSink>, target)
                })
                .map_err(|err| {
                    tracing::error!(error = %err, "failed to initialize ClickHouse sink");
                    err
                })
                .ok()
        });

        let ingest_service = Arc::new(IngestService::new(
            metadata_store.clone(),
            wal.clone(),
            hot_store.clone(),
            clickhouse_pair.clone(),
        ));
        Self {
            config,
            metadata_cache: cache,
            metadata_store,
            hot_store,
            _wal: wal,
            ingest_service,
        }
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
        #[derive(Clone)]
        struct AppState {
            metadata: SqliteMetadataStore,
            metadata_cache: Arc<tokio::sync::Mutex<ClusterMetadata>>,
            hot_store: Arc<dyn HotColumnStore>,
            ingest: Arc<IngestService>,
        }

        let state = AppState {
            metadata: self.metadata_store.clone(),
            metadata_cache: Arc::clone(&self.metadata_cache),
            hot_store: Arc::clone(&self.hot_store),
            ingest: Arc::clone(&self.ingest_service),
        };

        async fn status_handler(
            State(state): State<AppState>,
        ) -> Result<Json<StatusResponse>, AppError> {
            let metadata = state.metadata_cache.lock().await.clone();
            let shard_health = state.metadata.list_shard_health().await?;
            let clickhouse = state.ingest.clickhouse_status();
            Ok(Json(StatusResponse {
                metadata,
                shard_health,
                clickhouse,
            }))
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

        async fn ingest_handler(
            State(state): State<AppState>,
            Json(request): Json<IngestBatchRequest>,
        ) -> Result<StatusCode, AppError> {
            state.ingest.ingest(request).await?;
            Ok(StatusCode::ACCEPTED)
        }

        async fn query_handler(
            State(state): State<AppState>,
            Json(request): Json<QueryRequest>,
        ) -> Result<Json<QueryResponse>, AppError> {
            let rows = state
                .hot_store
                .scan_range(&request.tenant, &request.range, &request.symbols)
                .await?;
            let encoded: Vec<ByteBuf> = rows.into_iter().map(ByteBuf::from).collect();
            Ok(Json(QueryResponse { rows: encoded }))
        }

        async fn health_handler(
            State(state): State<AppState>,
            Json(health): Json<ShardHealth>,
        ) -> Result<StatusCode, AppError> {
            state.metadata.record_shard_health(health).await?;
            Ok(StatusCode::ACCEPTED)
        }

        let app = Router::new()
            .route("/status", get(status_handler))
            .route("/ingest", post(ingest_handler))
            .route("/query", post(query_handler))
            .route("/assignments", post(upsert_assignments_handler))
            .route("/health", post(health_handler))
            .with_state(state);

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
