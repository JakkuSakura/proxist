use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;

use anyhow::{anyhow, bail};
use clap::{Parser, Subcommand};
use proxist_api::{
    ControlCommand, DiagnosticsBundle, IngestBatchRequest, QueryRequest, QueryResponse,
    ShardAssignment, StatusResponse, SymbolDictionarySpec,
};
use proxist_core::{
    metadata::ClusterMetadata,
    query::{QueryOperation, QueryRange},
};
use reqwest::{Client, RequestBuilder};
use serde::Serialize;
use tokio::runtime::Runtime;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "pxctl", about = "Operator CLI for Proxist clusters")]
struct Cli {
    #[arg(long, global = true, default_value = "http://127.0.0.1:8080")]
    endpoint: String,
    #[arg(long, global = true)]
    token: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show cluster status summary.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Capture diagnostics bundle (status + metrics snapshot).
    Diagnostics,
    /// Apply cluster metadata (assignments, symbol dictionaries) from a JSON payload.
    Apply {
        #[arg(long)]
        file: String,
        #[arg(long)]
        yes: bool,
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
    /// Execute raw SQL via proxist's ClickHouse-compatible endpoint.
    Sql {
        #[arg(long)]
        file: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value = "proxist")]
        database: String,
    },
    /// Display proxist's hot vs cold seam summary.
    HotSummary {
        #[arg(long, default_value = "proxist")]
        database: String,
        #[arg(long)]
        json: bool,
    },
}

const INGEST_SUMMARY_SQL: &str = "SELECT group_key, entity_key, route_key, memory_rows, memory_first_micros, memory_last_micros, durable_through_micros, wal_high_micros \
FROM proxist.__system_ingest_summary \
ORDER BY group_key, entity_key \
FORMAT TSVWithNames";

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let rt = Runtime::new()?;
    let client = Client::new();
    let endpoint = cli.endpoint.trim_end_matches('/').to_string();
    let token = cli.token.clone();

    match cli.command {
        Commands::Status { json } => {
            let status = fetch_status(&rt, &client, &endpoint, token.as_deref())?;
            if json {
                println!("{}", serde_json::to_string_pretty(&status)?);
            } else {
                render_status(&status);
            }
        }
        Commands::Apply { file, yes } => {
            let spec = load_cluster_spec(&file)?;
            let status = fetch_status(&rt, &client, &endpoint, token.as_deref())?;

            let assignment_diffs =
                compute_assignment_diffs(&status.metadata.assignments, &spec.assignments);
            let symbol_updates = compute_symbol_updates(
                &status.metadata.symbol_dictionaries,
                &spec.symbol_dictionaries,
            );

            if assignment_diffs.updates.is_empty() && symbol_updates.is_empty() {
                if assignment_diffs.missing.is_empty() {
                    println!("No metadata changes required; cluster already matches spec.");
                } else {
                    print_change_summary(
                        &assignment_diffs.updates,
                        &assignment_diffs.missing,
                        &symbol_updates,
                    );
                    println!("Removal of shards is not yet automated; update the spec once nodes are drained.");
                }
                return Ok(());
            }

            print_change_summary(
                &assignment_diffs.updates,
                &assignment_diffs.missing,
                &symbol_updates,
            );

            if !yes {
                println!("Re-run with --yes to apply these changes.");
                return Ok(());
            }

            if !assignment_diffs.updates.is_empty() {
                let url = format!("{}/assignments", endpoint);
                let payload: Vec<ShardAssignment> = assignment_diffs
                    .updates
                    .iter()
                    .map(|change| change.desired.clone())
                    .collect();
                info!(rows = payload.len(), "upserting shard assignments");
                rt.block_on(async {
                    apply_auth(client.post(&url).json(&payload), token.as_deref())
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok::<(), anyhow::Error>(())
                })?;
            }

            if !symbol_updates.is_empty() {
                let url = format!("{}/symbols", endpoint);
                info!(
                    tenants = symbol_updates.len(),
                    "upserting symbol dictionaries"
                );
                rt.block_on(async {
                    apply_auth(client.post(&url).json(&symbol_updates), token.as_deref())
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok::<(), anyhow::Error>(())
                })?;
            }

            println!("Cluster metadata updated successfully.");
        }
        Commands::Diagnostics => {
            let url = format!("{}/diagnostics", endpoint);
            rt.block_on(async {
                let response = apply_auth(client.get(&url), token.as_deref())
                    .send()
                    .await?
                    .error_for_status()?;
                let bundle: DiagnosticsBundle = response.json().await?;
                println!("{}", serde_json::to_string_pretty(&bundle)?);
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
                let response = apply_auth(client.post(&url).json(&request), token.as_deref())
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
                apply_auth(client.post(&url).json(&batch), token.as_deref())
                    .send()
                    .await?
                    .error_for_status()?;
                Ok::<(), anyhow::Error>(())
            })?;
        }
        Commands::Sql {
            file,
            query,
            database,
        } => {
            let sql = match (file, query) {
                (Some(path), None) => fs::read_to_string(&path)?,
                (None, Some(q)) => q,
                (Some(_), Some(_)) => bail!("specify either --file or --query, not both"),
                (None, None) => bail!("either --file or --query must be provided"),
            };

            let response_text =
                execute_sql(&rt, &client, &endpoint, &database, token.as_deref(), &sql)?;
            print!("{}", response_text);
        }
        Commands::HotSummary { database, json } => {
            let response_text = execute_sql(
                &rt,
                &client,
                &endpoint,
                &database,
                token.as_deref(),
                INGEST_SUMMARY_SQL,
            )?;
            let rows = parse_ingest_summary_tsv(&response_text)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&rows)?);
            } else if rows.is_empty() {
                println!("No in-memory rows tracked.");
            } else {
                render_ingest_summary_table(&rows);
            }
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

#[derive(Debug, Clone, Serialize)]
struct IngestSummaryRow {
    group_key: String,
    entity_key: String,
    route_key: Option<String>,
    memory_rows: u64,
    memory_first_micros: Option<i64>,
    memory_last_micros: Option<i64>,
    durable_through_micros: Option<i64>,
    wal_high_micros: Option<i64>,
}

fn execute_sql(
    rt: &Runtime,
    client: &Client,
    endpoint: &str,
    database: &str,
    token: Option<&str>,
    sql: &str,
) -> anyhow::Result<String> {
    let url = format!("{}/?database={}", endpoint, database);
    let payload = sql.to_owned();
    let response_text = rt.block_on(async {
        let response = apply_auth(
            client
                .post(&url)
                .body(payload)
                .header("Content-Type", "text/plain"),
            token,
        )
        .send()
        .await?
        .error_for_status()?;
        Ok::<String, anyhow::Error>(response.text().await?)
    })?;
    Ok(response_text)
}

fn parse_ingest_summary_tsv(text: &str) -> anyhow::Result<Vec<IngestSummaryRow>> {
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let header_line = lines
        .next()
        .ok_or_else(|| anyhow!("summary response did not include a header row"))?;
    let headers: Vec<&str> = header_line.split('\t').collect();
    let expected_neutral = [
        "group_key",
        "entity_key",
        "route_key",
        "memory_rows",
        "memory_first_micros",
        "memory_last_micros",
        "durable_through_micros",
        "wal_high_micros",
    ];
    let expected_legacy = [
        "tenant",
        "symbol",
        "shard_id",
        "hot_rows",
        "hot_first_micros",
        "hot_last_micros",
        "persisted_through_micros",
        "wal_high_micros",
    ];
    #[derive(Clone, Copy)]
    enum HeaderKind {
        Neutral,
        Legacy,
    }

    let (header_kind, expected_len) = if headers.as_slice() == expected_neutral.as_slice() {
        (HeaderKind::Neutral, expected_neutral.len())
    } else if headers.as_slice() == expected_legacy.as_slice() {
        (HeaderKind::Legacy, expected_legacy.len())
    } else {
        bail!(
            "unexpected summary header {:?}; expected {:?} or {:?}",
            headers,
            expected_neutral,
            expected_legacy
        );
    };

    let mut rows = Vec::new();
    for line in lines {
        let columns: Vec<&str> = line.split('\t').collect();
        if columns.len() != expected_len {
            bail!(
                "expected {} columns in summary row, got {}: {}",
                expected_len,
                columns.len(),
                line
            );
        }
        rows.push(IngestSummaryRow {
            group_key: columns[0].to_string(),
            entity_key: columns[1].to_string(),
            route_key: parse_string_value(columns[2]),
            memory_rows: columns[3].parse::<u64>().map_err(|err| {
                anyhow!(
                    "failed to parse summary row count value {:?}: {}",
                    columns[3],
                    err
                )
            })?,
            memory_first_micros: parse_opt_i64(columns[4])?,
            memory_last_micros: parse_opt_i64(columns[5])?,
            durable_through_micros: parse_opt_i64(columns[6])?,
            wal_high_micros: parse_opt_i64(columns[7])?,
        });
    }

    if matches!(header_kind, HeaderKind::Legacy) {
        tracing::warn!(
            "observed legacy proxist summary header; consider upgrading proxistd for neutral naming"
        );
    }

    Ok(rows)
}

fn parse_string_value(value: &str) -> Option<String> {
    if value == "\\N" || value.eq_ignore_ascii_case("null") {
        None
    } else {
        Some(value.to_string())
    }
}

fn parse_opt_i64(value: &str) -> anyhow::Result<Option<i64>> {
    if value == "\\N" || value.eq_ignore_ascii_case("null") {
        Ok(None)
    } else {
        Ok(Some(value.parse::<i64>().map_err(|err| {
            anyhow!("failed to parse integer {value:?}: {err}")
        })?))
    }
}

fn render_ingest_summary_table(rows: &[IngestSummaryRow]) {
    let headers = [
        "group_key",
        "entity_key",
        "route_key",
        "memory_rows",
        "memory_first_micros",
        "memory_last_micros",
        "durable_through_micros",
        "wal_high_micros",
    ];
    let align_right = [false, false, false, true, true, true, true, true];

    let formatted_rows: Vec<[String; 8]> = rows
        .iter()
        .map(|row| {
            [
                row.group_key.clone(),
                row.entity_key.clone(),
                row.route_key.clone().unwrap_or_else(|| "-".into()),
                row.memory_rows.to_string(),
                format_opt_i64(row.memory_first_micros),
                format_opt_i64(row.memory_last_micros),
                format_opt_i64(row.durable_through_micros),
                format_opt_i64(row.wal_high_micros),
            ]
        })
        .collect();

    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in &formatted_rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len());
        }
    }

    let header_line = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| {
            if align_right[idx] {
                format!("{:>width$}", header, width = widths[idx])
            } else {
                format!("{:<width$}", header, width = widths[idx])
            }
        })
        .collect::<Vec<_>>()
        .join("  ");
    println!("{}", header_line);

    for row in formatted_rows {
        let line = row
            .iter()
            .enumerate()
            .map(|(idx, value)| {
                if align_right[idx] {
                    format!("{:>width$}", value, width = widths[idx])
                } else {
                    format!("{:<width$}", value, width = widths[idx])
                }
            })
            .collect::<Vec<_>>()
            .join("  ");
        println!("{}", line);
    }
}

fn format_opt_i64(value: Option<i64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn fetch_status(
    rt: &Runtime,
    client: &Client,
    endpoint: &str,
    token: Option<&str>,
) -> anyhow::Result<StatusResponse> {
    let url = format!("{}/status", endpoint);
    let status = rt.block_on(async {
        let response = apply_auth(client.get(&url), token)
            .send()
            .await?
            .error_for_status()?;
        let status: StatusResponse = response.json().await?;
        Ok::<StatusResponse, anyhow::Error>(status)
    })?;
    Ok(status)
}

fn load_cluster_spec(path: &str) -> anyhow::Result<ClusterMetadata> {
    let json = std::fs::read_to_string(path)?;
    if let Ok(spec) = serde_json::from_str::<ClusterMetadata>(&json) {
        return Ok(spec);
    }
    if let Ok(assignments) = serde_json::from_str::<Vec<ShardAssignment>>(&json) {
        let mut spec = ClusterMetadata::default();
        spec.assignments = assignments;
        return Ok(spec);
    }
    if let Ok(cmd) = serde_json::from_str::<ControlCommand>(&json) {
        if let ControlCommand::ApplyShardAssignments(assignments) = cmd {
            let mut spec = ClusterMetadata::default();
            spec.assignments = assignments;
            return Ok(spec);
        } else {
            bail!("unsupported control command in spec");
        }
    }
    bail!("failed to parse cluster spec: expected ClusterMetadata or assignment list")
}

fn apply_auth(builder: RequestBuilder, token: Option<&str>) -> RequestBuilder {
    if let Some(token) = token {
        builder.bearer_auth(token)
    } else {
        builder
    }
}

#[derive(Debug, Clone)]
struct AssignmentChange {
    shard_id: String,
    previous: Option<ShardAssignment>,
    desired: ShardAssignment,
}

#[derive(Debug, Default)]
struct AssignmentDiffs {
    updates: Vec<AssignmentChange>,
    missing: Vec<ShardAssignment>,
}

fn compute_assignment_diffs(
    current: &[ShardAssignment],
    desired: &[ShardAssignment],
) -> AssignmentDiffs {
    let mut current_map: HashMap<&str, &ShardAssignment> = HashMap::new();
    for assignment in current {
        current_map.insert(assignment.shard_id.as_str(), assignment);
    }

    let mut desired_ids: HashSet<&str> = HashSet::new();
    let mut updates = Vec::new();

    for new in desired {
        desired_ids.insert(new.shard_id.as_str());
        if let Some(existing) = current_map.get(new.shard_id.as_str()) {
            if *existing == new {
                continue;
            }
            updates.push(AssignmentChange {
                shard_id: new.shard_id.clone(),
                previous: Some((*existing).clone()),
                desired: new.clone(),
            });
        } else {
            updates.push(AssignmentChange {
                shard_id: new.shard_id.clone(),
                previous: None,
                desired: new.clone(),
            });
        }
    }

    let mut missing = Vec::new();
    for assignment in current {
        if !desired_ids.contains(assignment.shard_id.as_str()) {
            missing.push(assignment.clone());
        }
    }

    AssignmentDiffs { updates, missing }
}

fn compute_symbol_updates(
    current: &BTreeMap<String, Vec<String>>,
    desired: &BTreeMap<String, Vec<String>>,
) -> Vec<SymbolDictionarySpec> {
    let mut updates = Vec::new();
    for (tenant, symbols) in desired {
        let existing_lookup: HashSet<&String> = current
            .get(tenant)
            .map(|list| list.iter().collect())
            .unwrap_or_else(HashSet::new);
        let mut seen = HashSet::new();
        let mut additions = Vec::new();
        for symbol in symbols {
            if !seen.insert(symbol.as_str()) {
                continue;
            }
            if !existing_lookup.contains(symbol) {
                additions.push(symbol.clone());
            }
        }
        if !additions.is_empty() {
            updates.push(SymbolDictionarySpec {
                tenant: tenant.clone(),
                symbols: additions,
            });
        }
    }
    updates
}

fn print_change_summary(
    changes: &[AssignmentChange],
    missing: &[ShardAssignment],
    symbol_updates: &[SymbolDictionarySpec],
) {
    if !changes.is_empty() {
        println!("Shard assignments to apply:");
        for change in changes {
            if let Some(prev) = &change.previous {
                println!(
                    "  {}: tenant {} on {} [{}..{}) → tenant {} on {} [{}..{})",
                    change.shard_id,
                    prev.tenant_id,
                    prev.node_id,
                    prev.symbol_range.0,
                    prev.symbol_range.1,
                    change.desired.tenant_id,
                    change.desired.node_id,
                    change.desired.symbol_range.0,
                    change.desired.symbol_range.1,
                );
            } else {
                println!(
                    "  {}: assign tenant {} to node {} [{}..{})",
                    change.shard_id,
                    change.desired.tenant_id,
                    change.desired.node_id,
                    change.desired.symbol_range.0,
                    change.desired.symbol_range.1,
                );
            }
        }
    }

    if !missing.is_empty() {
        println!("Shards present in cluster but missing from spec (not modified):");
        for shard in missing {
            println!(
                "  {}: tenant {} on {} [{}..{})",
                shard.shard_id,
                shard.tenant_id,
                shard.node_id,
                shard.symbol_range.0,
                shard.symbol_range.1
            );
        }
    }

    if !symbol_updates.is_empty() {
        println!("Symbol dictionaries additions:");
        for spec in symbol_updates {
            println!("  {}: {}", spec.tenant, spec.symbols.join(", "));
        }
    }
}

fn render_status(status: &StatusResponse) {
    println!("Shard Assignments:");
    if status.metadata.assignments.is_empty() {
        println!("  <none>");
    } else {
        for assignment in &status.metadata.assignments {
            println!(
                "  {}: tenant {} on {} [{}..{})",
                assignment.shard_id,
                assignment.tenant_id,
                assignment.node_id,
                assignment.symbol_range.0,
                assignment.symbol_range.1
            );
        }
    }

    println!("Symbol Dictionaries:");
    if status.metadata.symbol_dictionaries.is_empty() {
        println!("  <none>");
    } else {
        for (tenant, symbols) in &status.metadata.symbol_dictionaries {
            println!("  {}: {}", tenant, symbols.join(", "));
        }
    }

    println!("Shard Health:");
    if status.shard_health.is_empty() {
        println!("  <none>");
    } else {
        for health in &status.shard_health {
            println!(
                "  {} leader={} persisted={} wal={} state={:?}",
                health.shard_id,
                health.is_leader,
                format_system_time(health.watermark.persisted),
                format_system_time(health.watermark.wal_high),
                health.persistence_state
            );
        }
    }

    println!("ClickHouse:");
    println!("  enabled: {}", status.clickhouse.enabled);
    if let Some(target) = &status.clickhouse.target {
        println!(
            "  target: {}/{}/{}",
            target.endpoint, target.database, target.table
        );
    }
    println!(
        "  last_flush: {}",
        format_system_time(status.clickhouse.last_flush)
    );
    if let Some(error) = &status.clickhouse.last_error {
        println!("  last_error: {}", error);
    }
}

fn format_system_time(ts: Option<std::time::SystemTime>) -> String {
    match ts {
        Some(value) => match value.duration_since(std::time::UNIX_EPOCH) {
            Ok(dur) => format!("{} µs", dur.as_micros()),
            Err(err) => format!("-{} µs", err.duration().as_micros()),
        },
        None => "-".into(),
    }
}
