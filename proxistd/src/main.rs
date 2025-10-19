use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use proxist_core::{metadata::ClusterMetadata, MetadataStore, ShardPersistenceTracker};
use proxist_metadata_sqlite::SqliteMetadataStore;
use tokio::signal;
use tracing::{info, instrument};
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
}

impl DaemonConfig {
    fn load() -> anyhow::Result<Self> {
        let metadata_path = std::env::var("PROXIST_METADATA_SQLITE_PATH")
            .unwrap_or_else(|_| "./proxist-meta.db".into());
        Ok(Self { metadata_path })
    }
}

struct ProxistDaemon {
    config: DaemonConfig,
    _metadata_cache: Arc<tokio::sync::Mutex<ClusterMetadata>>,
    metadata_store: SqliteMetadataStore,
}

impl ProxistDaemon {
    fn new(config: DaemonConfig, metadata_store: SqliteMetadataStore) -> Self {
        let cache = Arc::new(tokio::sync::Mutex::new(ClusterMetadata::default()));
        Self {
            config,
            _metadata_cache: cache,
            metadata_store,
        }
    }

    #[instrument(skip(self))]
    async fn run(&self) -> anyhow::Result<()> {
        let shutdown = shutdown_signal();

        tokio::select! {
            _ = self.control_loop() => {},
            _ = shutdown => {
                info!("shutdown signal received");
            }
        }

        Ok(())
    }

    async fn control_loop(&self) -> anyhow::Result<()> {
        info!("control loop placeholder started");
        loop {
            let snapshot = self.metadata_store.get_cluster_metadata().await?;
            {
                let mut cache = self._metadata_cache.lock().await;
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
        }
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
