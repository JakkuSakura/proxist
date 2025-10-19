use clap::{Parser, Subcommand};
use proxist_api::{ControlCommand, IngestBatchRequest, QueryRequest};
use tokio::runtime::Runtime;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "proxictl", about = "Operator CLI for Proxist clusters")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show cluster status summary.
    Status,
    /// Apply shard assignments from a JSON payload.
    ApplyAssignments {
        #[arg(long)]
        file: String,
    },
    /// Submit an ad-hoc query described via JSON.
    Query {
        #[arg(long)]
        file: String,
    },
    /// Inject a test ingest batch described via JSON.
    Ingest {
        #[arg(long)]
        file: String,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let rt = Runtime::new()?;

    match cli.command {
        Commands::Status => {
            info!("status command placeholder");
        }
        Commands::ApplyAssignments { file } => {
            let json = std::fs::read_to_string(file)?;
            let cmd: ControlCommand = serde_json::from_str(&json)?;
            info!(?cmd, "would send control command");
        }
        Commands::Query { file } => {
            let json = std::fs::read_to_string(file)?;
            let request: QueryRequest = serde_json::from_str(&json)?;
            info!(?request, "would submit query");
            rt.block_on(async move {
                // TODO: wire to gRPC once available.
                Ok::<(), anyhow::Error>(())
            })?;
        }
        Commands::Ingest { file } => {
            let json = std::fs::read_to_string(file)?;
            let batch: IngestBatchRequest = serde_json::from_str(&json)?;
            info!(?batch, "would submit ingest batch");
            rt.block_on(async move {
                // TODO: send to proxistd ingest endpoint.
                Ok::<(), anyhow::Error>(())
            })?;
        }
    }

    Ok(())
}
