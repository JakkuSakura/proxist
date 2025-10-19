//! API models shared across CLI and daemon.

use std::time::SystemTime;

use proxist_core::{
    metadata::{ShardAssignment, ShardHealth, TenantId},
    query::QueryRange,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestTick {
    pub tenant: TenantId,
    pub symbol: String,
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
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
    pub rows: Vec<Vec<u8>>,
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
