use anyhow::bail;
use clap::{Parser, Subcommand};
use proxist_api::{
    ControlCommand, IngestBatchRequest, QueryRequest, QueryResponse, ShardAssignment,
    StatusResponse,
};
use proxist_core::query::{QueryOperation, QueryRange};
use reqwest::Client;
use tokio::runtime::Runtime;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "pxctl", about = "Operator CLI for Proxist clusters")]
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
    /// Submit an ad-hoc query described via JSON or CLI flags.
    Query {
        #[arg(long)]
        file: Option<String>,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        symbols: Vec<String>,
        #[arg(long)]
        start_micros: Option<i64>,
        #[arg(long)]
        end_micros: Option<i64>,
        #[arg(long, default_value = "range")]
        op: String,
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
                let status: StatusResponse = response.json().await?;
                println!("{}", serde_json::to_string_pretty(&status)?);
                Ok::<(), anyhow::Error>(())
            })?;
        }
        Commands::ApplyAssignments { file } => {
            let json = std::fs::read_to_string(file)?;
            let assignments: Vec<ShardAssignment> =
                match serde_json::from_str::<Vec<ShardAssignment>>(&json) {
                    Ok(list) => list,
                    Err(_) => {
                        let cmd: ControlCommand = serde_json::from_str(&json)?;
                        match cmd {
                            ControlCommand::ApplyShardAssignments(list) => list,
                            other => bail!("unexpected command payload: {:?}", other),
                        }
                    }
                };

            let url = format!("{}/assignments", endpoint);
            info!(rows = assignments.len(), "applying shard assignments");
            rt.block_on(async {
                client
                    .post(url)
                    .json(&assignments)
                    .send()
                    .await?
                    .error_for_status()?;
                Ok::<(), anyhow::Error>(())
            })?;
        }
        Commands::Query {
            file,
            tenant,
            symbols,
            start_micros,
            end_micros,
            op,
        } => {
            let request = if let Some(path) = file {
                let json = std::fs::read_to_string(&path)?;
                serde_json::from_str::<QueryRequest>(&json)?
            } else {
                build_query_request(tenant, symbols, start_micros, end_micros, op)?
            };
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

fn build_query_request(
    tenant: Option<String>,
    symbols: Vec<String>,
    start_micros: Option<i64>,
    end_micros: Option<i64>,
    op: String,
) -> anyhow::Result<QueryRequest> {
    let tenant =
        tenant.ok_or_else(|| anyhow::anyhow!("tenant is required unless --file is provided"))?;
    let op = match op.as_str() {
        "range" => QueryOperation::Range,
        "last_by" => QueryOperation::LastBy,
        "asof" => QueryOperation::AsOf,
        other => anyhow::bail!("unknown query op: {}", other),
    };

    let end_micros = end_micros.unwrap_or_else(|| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        now.as_micros() as i64
    });
    let start_micros = start_micros.unwrap_or(end_micros.saturating_sub(1_000_000));

    let range = QueryRange::new(
        micros_to_system_time(start_micros),
        micros_to_system_time(end_micros),
    );

    Ok(QueryRequest {
        tenant,
        symbols,
        range,
        include_cold: false,
        op,
    })
}

fn micros_to_system_time(micros: i64) -> std::time::SystemTime {
    if micros >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_micros(micros as u64)
    } else {
        std::time::UNIX_EPOCH - std::time::Duration::from_micros((-micros) as u64)
    }
}
