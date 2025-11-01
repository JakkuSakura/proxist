use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use async_trait::async_trait;
use proxist_core::metadata::{
    ClusterMetadata, MetadataStore, ShardAssignment, ShardHealth, TenantId,
};
use proxist_core::{PersistenceState, Watermark};
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
use tracing::info;

const MIGRATIONS: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS shard_assignments (
        shard_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        node_id TEXT NOT NULL,
        symbol_start TEXT NOT NULL,
        symbol_end TEXT NOT NULL
    );
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS symbols (
        tenant_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        symbol_id INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, symbol)
    );
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS shard_health (
        shard_id TEXT PRIMARY KEY,
        is_leader INTEGER NOT NULL,
        wal_backlog_bytes INTEGER NOT NULL,
        clickhouse_lag_ms INTEGER NOT NULL,
        watermark_json TEXT NOT NULL,
        persistence_state_json TEXT NOT NULL,
        updated_at INTEGER NOT NULL
    );
    "#,
];

#[derive(Clone)]
pub struct SqliteMetadataStore {
    pool: SqlitePool,
}

impl SqliteMetadataStore {
    pub async fn connect(path: &str) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;
        let store = Self { pool };
        store.run_migrations().await?;
        info!(%path, "initialized sqlite metadata store");
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn run_migrations(&self) -> Result<()> {
        for stmt in MIGRATIONS {
            sqlx::query(stmt).execute(&self.pool).await?;
        }
        Ok(())
    }

    fn encode_timestamp(ts: Option<SystemTime>) -> Option<i64> {
        ts.and_then(|time| {
            time.duration_since(UNIX_EPOCH)
                .map(|dur| dur.as_micros() as i64)
                .ok()
        })
    }
}

#[async_trait]
impl MetadataStore for SqliteMetadataStore {
    async fn get_cluster_metadata(&self) -> Result<ClusterMetadata> {
        let mut metadata = ClusterMetadata::default();

        let assignment_rows = sqlx::query(
            r#"
            SELECT shard_id, tenant_id, node_id, symbol_start, symbol_end
            FROM shard_assignments
            ORDER BY shard_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        metadata.assignments = assignment_rows
            .into_iter()
            .map(|row| ShardAssignment {
                shard_id: row.get("shard_id"),
                tenant_id: row.get("tenant_id"),
                node_id: row.get("node_id"),
                symbol_range: (row.get("symbol_start"), row.get("symbol_end")),
            })
            .collect();

        let symbol_rows = sqlx::query(
            r#"
            SELECT tenant_id, symbol, symbol_id
            FROM symbols
            ORDER BY tenant_id, symbol_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut dictionaries: BTreeMap<TenantId, Vec<String>> = BTreeMap::new();

        for row in symbol_rows {
            let tenant: String = row.get("tenant_id");
            let symbol: String = row.get("symbol");
            dictionaries.entry(tenant).or_default().push(symbol);
        }

        metadata.symbol_dictionaries = dictionaries;

        Ok(metadata)
    }

    async fn put_shard_assignment(&self, assignment: ShardAssignment) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO shard_assignments (shard_id, tenant_id, node_id, symbol_start, symbol_end)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(shard_id) DO UPDATE SET
                tenant_id=excluded.tenant_id,
                node_id=excluded.node_id,
                symbol_start=excluded.symbol_start,
                symbol_end=excluded.symbol_end
            "#,
        )
        .bind(&assignment.shard_id)
        .bind(&assignment.tenant_id)
        .bind(&assignment.node_id)
        .bind(&assignment.symbol_range.0)
        .bind(&assignment.symbol_range.1)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn record_shard_health(&self, health: ShardHealth) -> Result<()> {
        let watermark_json = serde_json::to_string(&health.watermark)?;
        let persistence_state_json = serde_json::to_string(&health.persistence_state)?;
        let updated_at = Self::encode_timestamp(Some(SystemTime::now())).unwrap_or_default();

        sqlx::query(
            r#"
            INSERT INTO shard_health (
                shard_id, is_leader, wal_backlog_bytes, clickhouse_lag_ms,
                watermark_json, persistence_state_json, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(shard_id) DO UPDATE SET
                is_leader=excluded.is_leader,
                wal_backlog_bytes=excluded.wal_backlog_bytes,
                clickhouse_lag_ms=excluded.clickhouse_lag_ms,
                watermark_json=excluded.watermark_json,
                persistence_state_json=excluded.persistence_state_json,
                updated_at=excluded.updated_at
            "#,
        )
        .bind(&health.shard_id)
        .bind(health.is_leader as i64)
        .bind(health.wal_backlog_bytes as i64)
        .bind(health.clickhouse_lag_ms as i64)
        .bind(watermark_json)
        .bind(persistence_state_json)
        .bind(updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn alloc_symbol(&self, tenant_id: &TenantId, symbol: &str) -> Result<u32> {
        let mut tx = self.pool.begin().await?;

        if let Some(existing) = sqlx::query(
            r#"
            SELECT symbol_id FROM symbols
            WHERE tenant_id = ?1 AND symbol = ?2
            "#,
        )
        .bind(tenant_id)
        .bind(symbol)
        .fetch_optional(&mut *tx)
        .await?
        {
            let id: i64 = existing.get("symbol_id");
            tx.commit().await?;
            return Ok(id as u32);
        }

        let max_row = sqlx::query(
            r#"
            SELECT MAX(symbol_id) as max_id FROM symbols WHERE tenant_id = ?1
            "#,
        )
        .bind(tenant_id)
        .fetch_one(&mut *tx)
        .await?;

        let max_id: Option<i64> = max_row.try_get("max_id")?;
        let next_id: i64 = max_id.unwrap_or(-1) + 1;

        sqlx::query(
            r#"
            INSERT INTO symbols (tenant_id, symbol, symbol_id)
            VALUES (?1, ?2, ?3)
            "#,
        )
        .bind(tenant_id)
        .bind(symbol)
        .bind(next_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(next_id as u32)
    }

    async fn upsert_symbols(&self, tenant_id: &TenantId, symbols: &[String]) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let existing_rows = sqlx::query(
            r#"
            SELECT symbol, symbol_id FROM symbols
            WHERE tenant_id = ?1
            ORDER BY symbol_id
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await?;

        let mut known: HashSet<String> = HashSet::new();
        let mut max_id: i64 = -1;
        for row in existing_rows {
            let symbol: String = row.get("symbol");
            let id: i64 = row.get("symbol_id");
            known.insert(symbol);
            if id > max_id {
                max_id = id;
            }
        }

        let mut next_id = max_id + 1;
        for symbol in symbols {
            if known.contains(symbol) {
                continue;
            }
            sqlx::query(
                r#"
                INSERT INTO symbols (tenant_id, symbol, symbol_id)
                VALUES (?1, ?2, ?3)
                "#,
            )
            .bind(tenant_id)
            .bind(symbol)
            .bind(next_id)
            .execute(&mut *tx)
            .await?;
            known.insert(symbol.clone());
            next_id += 1;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn list_shard_health(&self) -> Result<Vec<ShardHealth>> {
        let rows = sqlx::query(
            r#"
            SELECT shard_id, is_leader, wal_backlog_bytes, clickhouse_lag_ms,
                   watermark_json, persistence_state_json
            FROM shard_health
            ORDER BY shard_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut health = Vec::with_capacity(rows.len());
        for row in rows {
            let watermark_json: String = row.get("watermark_json");
            let persistence_state_json: String = row.get("persistence_state_json");
            let watermark: Watermark = serde_json::from_str(&watermark_json)?;
            let persistence_state: PersistenceState =
                serde_json::from_str(&persistence_state_json)?;

            health.push(ShardHealth {
                shard_id: row.get("shard_id"),
                is_leader: row.get::<i64, _>("is_leader") != 0,
                wal_backlog_bytes: row.get::<i64, _>("wal_backlog_bytes") as u64,
                clickhouse_lag_ms: row.get::<i64, _>("clickhouse_lag_ms") as u64,
                watermark,
                persistence_state,
            });
        }

        Ok(health)
    }
}

impl SqliteMetadataStore {
    pub async fn read_shard_health(&self, shard_id: &str) -> Result<Option<ShardHealth>> {
        let record = sqlx::query(
            r#"
            SELECT shard_id, is_leader, wal_backlog_bytes, clickhouse_lag_ms,
                   watermark_json, persistence_state_json
            FROM shard_health
            WHERE shard_id = ?1
            "#,
        )
        .bind(shard_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = record else {
            return Ok(None);
        };

        let watermark_json: String = row.get("watermark_json");
        let persistence_state_json: String = row.get("persistence_state_json");
        let watermark: Watermark = serde_json::from_str(&watermark_json)?;
        let persistence_state: PersistenceState = serde_json::from_str(&persistence_state_json)?;

        Ok(Some(ShardHealth {
            shard_id: row.get("shard_id"),
            is_leader: row.get::<i64, _>("is_leader") != 0,
            wal_backlog_bytes: row.get::<i64, _>("wal_backlog_bytes") as u64,
            clickhouse_lag_ms: row.get::<i64, _>("clickhouse_lag_ms") as u64,
            watermark,
            persistence_state,
        }))
    }
}
