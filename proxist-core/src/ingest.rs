use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Schema-driven ingest row independent of any WAL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestRow {
    pub table: String,
    pub key0: Bytes,
    pub key1: Bytes,
    pub order_micros: i64,
    pub seq: u64,
    pub shard_id: String,
    pub values: Vec<Bytes>,
}

/// Batch of ingest records flushed together.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestSegment {
    pub rows: Vec<IngestRow>,
}

impl IngestSegment {
    pub fn new(rows: Vec<IngestRow>) -> Self {
        Self { rows }
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}
