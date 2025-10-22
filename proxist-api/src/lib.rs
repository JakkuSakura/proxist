//! API models shared across CLI and daemon.

use std::time::SystemTime;

pub use proxist_core::metadata::{ShardAssignment, ShardHealth};
use proxist_core::{
    metadata::TenantId,
    query::{QueryRange, RollingWindowConfig},
};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestTick {
    pub tenant: TenantId,
    pub symbol: String,
    #[serde(with = "proxist_core::time::serde_micros")]
    pub timestamp: SystemTime,
    pub payload: ByteBuf,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestBatchRequest {
    pub ticks: Vec<IngestTick>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub tenant: TenantId,
    pub symbols: Vec<String>,
    pub range: QueryRange,
    pub include_cold: bool,
    #[serde(default = "default_query_op")]
    pub op: proxist_core::query::QueryOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolling: Option<RollingWindowConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub rows: Vec<QueryRow>,
}

fn default_query_op() -> proxist_core::query::QueryOperation {
    proxist_core::query::QueryOperation::Range
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRow {
    pub symbol: String,
    #[serde(with = "proxist_core::time::serde_micros")]
    pub timestamp: SystemTime,
    pub payload: ByteBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolDictionarySpec {
    pub tenant: TenantId,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticsBundle {
    #[serde(with = "proxist_core::time::serde_micros")]
    pub captured_at: SystemTime,
    pub status: StatusResponse,
    pub metrics: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub metadata: proxist_core::metadata::ClusterMetadata,
    pub shard_health: Vec<ShardHealth>,
    pub clickhouse: ClickhouseStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseStatus {
    pub enabled: bool,
    pub target: Option<ClickhouseTarget>,
    #[serde(with = "proxist_core::time::serde_opt_micros")]
    pub last_flush: Option<SystemTime>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseTarget {
    pub endpoint: String,
    pub database: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommand {
    ApplyShardAssignments(Vec<ShardAssignment>),
    ReportShardHealth(ShardHealth),
    TriggerSnapshot { shard_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub accepted: bool,
    pub message: Option<String>,
}
