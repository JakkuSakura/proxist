//! Core domain logic shared across the Proxist workspace.

pub mod metadata;
pub mod query;
pub mod watermark;

pub use metadata::{ClusterMetadata, MetadataStore, ShardAssignment, ShardHealth};
pub use query::{QueryPlanner, QueryRange, QueryShardPlan};
pub use watermark::{
    PersistenceBatch, PersistenceState, PersistenceTransition, ShardPersistenceTracker, Watermark,
};
