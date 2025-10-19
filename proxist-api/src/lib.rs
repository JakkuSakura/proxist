//! API models shared across CLI and daemon.

use std::time::SystemTime;

pub use proxist_core::metadata::{ShardAssignment, ShardHealth};
use proxist_core::{metadata::TenantId, query::QueryRange};
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub rows: Vec<ByteBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub metadata: proxist_core::metadata::ClusterMetadata,
    pub shard_health: Vec<ShardHealth>,
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
