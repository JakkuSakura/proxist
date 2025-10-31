use anyhow::Context;
use async_trait::async_trait;
use proxist_ch::ClickhouseHttpClient;
use proxist_metadata_sqlite::SqliteMetadataStore;
use serde_json::Value as JsonValue;
use sqlx::{Column, Row};
pub mod sql;

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub sqlite_path: Option<String>,
}

#[async_trait]
pub trait SqlExecutor: Send + Sync {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult>;
}

#[derive(Debug, Clone)]
pub enum SqlResult {
    Text(String),
    Rows(Vec<serde_json::Map<String, JsonValue>>),
}

pub struct InMemoryScheduler {
    sqlite: Option<SqliteExecutor>,
    clickhouse: Option<ClickhouseHttpClient>,
}

impl InMemoryScheduler {
    pub async fn new(cfg: ExecutorConfig, ch: Option<ClickhouseHttpClient>) -> anyhow::Result<Self> {
        let sqlite = match cfg.sqlite_path {
            Some(path) => Some(SqliteExecutor::connect(&path).await.context("connect sqlite")?),
            None => None,
        };
        Ok(Self { sqlite, clickhouse: ch })
    }
}

#[async_trait]
impl SqlExecutor for InMemoryScheduler {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult> {
        let normalized = sql.trim().to_ascii_lowercase();
        if normalized.starts_with("select") || normalized.starts_with("with") {
            if let Some(ch) = &self.clickhouse {
                let body = ch.execute_raw(sql).await?;
                return Ok(SqlResult::Text(body));
            }
        }
        if let Some(sqlite) = &self.sqlite {
            return sqlite.execute(sql).await;
        }
        anyhow::bail!("no executor available for query")
    }
}

pub struct SqliteExecutor {
    store: SqliteMetadataStore,
}

impl SqliteExecutor {
    pub async fn connect(path: &str) -> anyhow::Result<Self> {
        let store = SqliteMetadataStore::connect(path).await?;
        Ok(Self { store })
    }
}

#[async_trait]
impl SqlExecutor for SqliteExecutor {
    async fn execute(&self, sql: &str) -> anyhow::Result<SqlResult> {
        // Best-effort: run as execute or try fetch-all into JSON-like rows.
        // Note: sqlx generic row extraction requires DB-specific APIs; metadata store embeds pool.
        let pool = self.store.pool();
        // Try to fetch rows
        let rows = sqlx::query(sql).fetch_all(pool).await;
        match rows {
            Ok(rows) => {
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut map = serde_json::Map::new();
                    for col in row.columns() {
                        let name = col.name().to_string();
                        let v: Result<String, _> = row.try_get(name.as_str());
                        match v {
                            Ok(s) => {
                                map.insert(name, JsonValue::String(s));
                            }
                            Err(_) => {
                                // Fallback: represent as null when not stringly typed
                                map.insert(name, JsonValue::Null);
                            }
                        }
                    }
                    out.push(map);
                }
                Ok(SqlResult::Rows(out))
            }
            Err(_) => {
                // Execute as statement
                sqlx::query(sql).execute(pool).await?;
                Ok(SqlResult::Text("OK".into()))
            }
        }
    }
}

pub struct DuckDbExecutor;

#[async_trait]
impl SqlExecutor for DuckDbExecutor {
    async fn execute(&self, _sql: &str) -> anyhow::Result<SqlResult> {
        anyhow::bail!("duckdb executor not enabled")
    }
}

pub struct PostgresExecutor;

#[async_trait]
impl SqlExecutor for PostgresExecutor {
    async fn execute(&self, _sql: &str) -> anyhow::Result<SqlResult> {
        anyhow::bail!("postgres executor not enabled")
    }
}
