use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use proxist_core::{metadata::ClusterMetadata, ShardPersistenceTracker};
use tokio::signal;
use tracing::{info, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let config = DaemonConfig::load()?;

    info!(?config, "starting proxistd");

    let daemon = ProxistDaemon::new(config);
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
    metadata_endpoint: String,
}

impl DaemonConfig {
    fn load() -> anyhow::Result<Self> {
        let metadata_endpoint =
            std::env::var("PROXIST_METADATA_ENDPOINT").unwrap_or_else(|_| "memory://".into());
        Ok(Self { metadata_endpoint })
    }
}

struct ProxistDaemon {
    config: DaemonConfig,
    _metadata_cache: Arc<tokio::sync::Mutex<ClusterMetadata>>,
}

impl ProxistDaemon {
    fn new(config: DaemonConfig) -> Self {
        let cache = Arc::new(tokio::sync::Mutex::new(ClusterMetadata::default()));
        Self {
            config,
            _metadata_cache: cache,
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
            info!(
                endpoint = %self.config.metadata_endpoint,
                "poll metadata endpoint placeholder"
            );
            // TODO: hook in metadata polling and ingest service wiring.
            tokio::time::sleep(Duration::from_secs(5)).await;

            let mut cache = self._metadata_cache.lock().await;
            cache.assignments.retain(|_assignment| true);

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
