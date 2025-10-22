use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::metadata::ShardId;

/// Time window for a query, inclusive of `start`, exclusive of `end`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryRange {
    #[serde(with = "crate::time::serde_micros")]
    pub start: SystemTime,
    #[serde(with = "crate::time::serde_micros")]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryOperation {
    Range,
    LastBy,
    AsOf,
    RollingWindow,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RollingWindowConfig {
    /// Length of the rolling window in microseconds.
    pub length_micros: u64,
    #[serde(default = "default_rolling_aggregation")]
    pub aggregation: RollingAggregation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RollingAggregation {
    Count,
}

impl RollingWindowConfig {
    pub fn window(&self) -> Duration {
        Duration::from_micros(self.length_micros)
    }
}

const fn default_rolling_aggregation() -> RollingAggregation {
    RollingAggregation::Count
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
