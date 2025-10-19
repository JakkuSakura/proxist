use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::metadata::ShardId;

/// Time window for a query, inclusive of `start`, exclusive of `end`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryRange {
    pub start: SystemTime,
    pub end: SystemTime,
}

impl QueryRange {
    pub fn new(start: SystemTime, end: SystemTime) -> Self {
        assert!(end >= start, "query range end must be >= start");
        Self { start, end }
    }
}

/// Plan fragment for executing a query on a single shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryShardPlan {
    pub shard_id: ShardId,
    pub range: QueryRange,
    pub symbols: Vec<String>,
    pub include_cold: bool,
}

#[async_trait]
pub trait QueryPlanner: Send + Sync {
    async fn plan_query(
        &self,
        tenant: &str,
        range: QueryRange,
        symbols: &[String],
    ) -> anyhow::Result<Vec<QueryShardPlan>>;
}
