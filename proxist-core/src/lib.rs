//! Core domain logic shared across the Proxist workspace.

pub mod api;
pub mod ingest;
pub mod metadata;
pub mod query;
pub mod time;
pub mod watermark;

pub use metadata::{ClusterMetadata, MetadataStore, ShardAssignment, ShardHealth};
pub use query::{QueryPlanner, QueryRange, QueryShardPlan};
pub use watermark::{
    PersistenceBatch, PersistenceState, PersistenceTransition, ShardPersistenceTracker, Watermark,
};
