use anyhow::bail;
use clap::{Parser, Subcommand};
use proxist_api::{ControlCommand, IngestBatchRequest, QueryRequest, QueryResponse};
use reqwest::Client;
use tokio::runtime::Runtime;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "proxictl", about = "Operator CLI for Proxist clusters")]
struct Cli {
    #[arg(long, global = true, default_value = "http://127.0.0.1:8080")]
    endpoint: String,
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
    let client = Client::new();
    let endpoint = cli.endpoint.trim_end_matches('/').to_string();

    match cli.command {
        Commands::Status => {
            let url = format!("{}/status", endpoint);
            rt.block_on(async {
                let response = client.get(url).send().await?.error_for_status()?;
                let value: serde_json::Value = response.json().await?;
                println!("{}", serde_json::to_string_pretty(&value)?);
                Ok::<(), anyhow::Error>(())
            })?;
        }
        Commands::ApplyAssignments { file } => {
            let json = std::fs::read_to_string(file)?;
            let cmd: ControlCommand = serde_json::from_str(&json)?;
            info!(?cmd, "apply assignments endpoint not yet implemented");
            bail!("apply-assignments command is not wired to HTTP yet");
        }
        Commands::Query { file } => {
            let json = std::fs::read_to_string(file)?;
            let request: QueryRequest = serde_json::from_str(&json)?;
            let url = format!("{}/query", endpoint);
            info!("submitting query to {}", url);
            rt.block_on(async {
                let response = client
                    .post(url)
                    .json(&request)
                    .send()
                    .await?
                    .error_for_status()?;
                let payload: QueryResponse = response.json().await?;
                println!("{}", serde_json::to_string_pretty(&payload)?);
                Ok::<(), anyhow::Error>(())
            })?;
        }
        Commands::Ingest { file } => {
            let json = std::fs::read_to_string(file)?;
            let batch: IngestBatchRequest = serde_json::from_str(&json)?;
            let url = format!("{}/ingest", endpoint);
            info!(rows = batch.ticks.len(), "submitting ingest batch");
            rt.block_on(async {
                client
                    .post(url)
                    .json(&batch)
                    .send()
                    .await?
                    .error_for_status()?;
                Ok::<(), anyhow::Error>(())
            })?;
        }
    }

    Ok(())
}
