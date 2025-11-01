use serde::{Deserialize, Serialize};
use std::time::SystemTime;

use crate::metadata::TenantId;

/// Generic ingest record independent of any WAL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestRecord {
    pub tenant: TenantId,
    pub shard_id: String,
    pub symbol: String,
    #[serde(with = "crate::time::serde_micros")]
    pub timestamp: SystemTime,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub seq: u64,
}

/// Batch of ingest records flushed together.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestSegment {
    pub records: Vec<IngestRecord>,
}

impl IngestSegment {
    pub fn new(records: Vec<IngestRecord>) -> Self {
        Self { records }
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}
