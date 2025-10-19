//! In-memory columnar storage for hot tick data.

use std::time::SystemTime;

use async_trait::async_trait;
use proxist_core::{metadata::TenantId, query::QueryRange, ShardPersistenceTracker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemConfig {
    pub max_rows: u64,
    pub symbol_cache_capacity: usize,
}

impl Default for MemConfig {
    fn default() -> Self {
        Self {
            max_rows: 250_000_000,
            symbol_cache_capacity: 32_768,
        }
    }
}

/// Represents a row at the seam between hot and cold tiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeamBoundaryRow {
    pub symbol: String,
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
}

#[async_trait]
pub trait HotColumnStore: Send + Sync {
    async fn append_row(
        &self,
        tenant: &TenantId,
        symbol: &str,
        timestamp: SystemTime,
        payload: &[u8],
    ) -> anyhow::Result<()>;

    async fn scan_range(
        &self,
        tenant: &TenantId,
        range: &QueryRange,
        symbols: &[String],
    ) -> anyhow::Result<Vec<Vec<u8>>>;

    async fn seam_rows(
        &self,
        tenant: &TenantId,
        range: &QueryRange,
    ) -> anyhow::Result<Vec<SeamBoundaryRow>>;

    async fn snapshot(&self, shard_tracker: &ShardPersistenceTracker) -> anyhow::Result<Vec<u8>>;
}
