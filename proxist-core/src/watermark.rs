use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// Watermark indicates how far data has progressed through the ingest/persist pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    /// Highest timestamp durably persisted to ClickHouse.
    #[serde(with = "crate::time::serde_opt_micros")]
    pub persisted: Option<SystemTime>,
    /// Highest timestamp confirmed in the local WAL.
    #[serde(with = "crate::time::serde_opt_micros")]
    pub wal_high: Option<SystemTime>,
}

impl Watermark {
    pub const fn new(persisted: Option<SystemTime>, wal_high: Option<SystemTime>) -> Self {
        Self {
            persisted,
            wal_high,
        }
    }

    /// Returns the timestamp that separates cold ClickHouse history from hot in-memory data.
    pub fn t_persisted(&self) -> Option<SystemTime> {
        self.persisted
    }

    /// True when the hot set is empty or all hot data has been made durable.
    pub fn is_caught_up(&self) -> bool {
        match (self.persisted, self.wal_high) {
            (Some(p), Some(w)) => w <= p,
            (None, Some(_)) => false,
            (Some(_), None) => true,
            (None, None) => true,
        }
    }

    /// Bump the WAL watermark; does not advance persistence.
    pub fn bump_wal(&mut self, ts: SystemTime) {
        match self.wal_high {
            Some(current) if current >= ts => {}
            _ => self.wal_high = Some(ts),
        }
    }

    /// Bump the persisted watermark, ensuring it never exceeds the WAL watermark.
    pub fn bump_persisted(&mut self, ts: SystemTime) {
        if let Some(wal_high) = self.wal_high {
            debug_assert!(
                ts <= wal_high,
                "persisted watermark cannot advance past WAL watermark"
            );
        }
        match self.persisted {
            Some(current) if current >= ts => {}
            _ => self.persisted = Some(ts),
        }
    }
}

/// Unique identifier for a WAL batch that is flowing through the persistence pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistenceBatch {
    pub id: String,
    #[serde(with = "crate::time::serde_micros")]
    pub start: SystemTime,
    #[serde(with = "crate::time::serde_micros")]
    pub end: SystemTime,
}

impl PersistenceBatch {
    pub fn new(id: impl Into<String>, start: SystemTime, end: SystemTime) -> Self {
        Self {
            id: id.into(),
            start,
            end,
        }
    }
}

/// State machine for managing the lifecycle of a persistence batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistenceState {
    Capture,
    FlushReady {
        batch: PersistenceBatch,
    },
    Persisting {
        batch: PersistenceBatch,
    },
    Committed {
        batch: PersistenceBatch,
    },
    Published {
        batch: PersistenceBatch,
    },
    Checkpointed {
        batch: PersistenceBatch,
        snapshot_id: String,
    },
}

impl PersistenceState {
    pub fn batch(&self) -> Option<&PersistenceBatch> {
        match self {
            PersistenceState::Capture => None,
            PersistenceState::FlushReady { batch }
            | PersistenceState::Persisting { batch }
            | PersistenceState::Committed { batch }
            | PersistenceState::Published { batch }
            | PersistenceState::Checkpointed { batch, .. } => Some(batch),
        }
    }
}

/// Legal transitions between persistence states. Each transition is validated by the tracker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistenceTransition {
    Enqueue {
        batch: PersistenceBatch,
    },
    BeginPersist {
        batch_id: String,
    },
    ConfirmPersist {
        batch_id: String,
    },
    Publish {
        batch_id: String,
    },
    Checkpoint {
        batch_id: String,
        snapshot_id: String,
    },
    Reset,
}

/// Tracks per-shard persistence state and watermark advancement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardPersistenceTracker {
    pub shard_id: String,
    pub state: PersistenceState,
    pub watermark: Watermark,
}

impl ShardPersistenceTracker {
    pub fn new(shard_id: impl Into<String>) -> Self {
        Self {
            shard_id: shard_id.into(),
            state: PersistenceState::Capture,
            watermark: Watermark::new(None, None),
        }
    }

    /// Apply a transition, returning the updated state and any watermark adjustments.
    pub fn apply(&mut self, transition: PersistenceTransition) -> anyhow::Result<()> {
        use PersistenceState::*;
        use PersistenceTransition::*;

        match (&self.state, transition) {
            (Capture, Enqueue { batch }) => {
                self.watermark.bump_wal(batch.end);
                self.state = FlushReady { batch };
            }
            (FlushReady { batch }, BeginPersist { batch_id }) if batch.id == batch_id => {
                self.state = Persisting {
                    batch: batch.clone(),
                };
            }
            (Persisting { batch }, ConfirmPersist { batch_id }) if batch.id == batch_id => {
                self.watermark.bump_persisted(batch.end);
                self.state = Committed {
                    batch: batch.clone(),
                };
            }
            (Committed { batch }, Publish { batch_id }) if batch.id == batch_id => {
                self.state = Published {
                    batch: batch.clone(),
                };
            }
            (
                Published { batch },
                Checkpoint {
                    batch_id,
                    snapshot_id,
                },
            ) if batch.id == batch_id => {
                self.state = Checkpointed {
                    batch: batch.clone(),
                    snapshot_id,
                };
            }
            (_, Reset) => {
                self.state = Capture;
            }
            (state, transition) => {
                anyhow::bail!(
                    "illegal persistence transition: {:?} -> {:?}",
                    state,
                    transition
                );
            }
        }

        Ok(())
    }
}
