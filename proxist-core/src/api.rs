use std::time::SystemTime;

pub use crate::metadata::{ShardAssignment, ShardHealth};
use crate::{metadata::TenantId, ShardPersistenceTracker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolDictionarySpec {
    pub tenant: TenantId,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticsBundle {
    #[serde(with = "crate::time::serde_micros")]
    pub captured_at: SystemTime,
    pub status: StatusResponse,
    pub metrics: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub persistence: Vec<ShardPersistenceTracker>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hot_summary: Vec<DiagnosticsHotSummaryRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticsHotSummaryRow {
    pub tenant: String,
    pub symbol: String,
    pub shard_id: Option<String>,
    pub hot_rows: u64,
    pub hot_first_micros: Option<i64>,
    pub hot_last_micros: Option<i64>,
    pub persisted_through_micros: Option<i64>,
    pub wal_high_micros: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub metadata: crate::metadata::ClusterMetadata,
    pub shard_health: Vec<ShardHealth>,
    pub clickhouse: ClickhouseStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseStatus {
    pub enabled: bool,
    pub target: Option<ClickhouseTarget>,
    #[serde(with = "crate::time::serde_opt_micros")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseConfigView {
    pub endpoint: String,
    pub database: String,
    pub table: String,
    pub username: Option<String>,
}
