use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{PersistenceState, Watermark};

/// Unique handle for a shard in the cluster.
pub type ShardId = String;
/// Unique identifier for a tenant/namespace.
pub type TenantId = String;

/// Minimal set of metadata the control plane uses to place shards.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShardAssignment {
    pub shard_id: ShardId,
    pub tenant_id: TenantId,
    pub node_id: String,
    pub symbol_range: (String, String),
}

/// Health envelope reported by each node for a shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShardHealth {
    pub shard_id: ShardId,
    pub is_leader: bool,
    pub wal_backlog_bytes: u64,
    pub clickhouse_lag_ms: u64,
    pub watermark: Watermark,
    pub persistence_state: PersistenceState,
}

/// Composite metadata snapshot for control-plane decisions.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ClusterMetadata {
    pub assignments: Vec<ShardAssignment>,
    pub symbol_dictionaries: BTreeMap<TenantId, Vec<String>>,
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn get_cluster_metadata(&self) -> anyhow::Result<ClusterMetadata>;
    async fn put_shard_assignment(&self, assignment: ShardAssignment) -> anyhow::Result<()>;
    async fn record_shard_health(&self, health: ShardHealth) -> anyhow::Result<()>;
    async fn alloc_symbol(&self, tenant_id: &TenantId, symbol: &str) -> anyhow::Result<u32>;
}
