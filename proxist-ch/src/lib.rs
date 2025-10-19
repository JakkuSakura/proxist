//! ClickHouse persistence adapter.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use proxist_wal::WalSegment;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseConfig {
    pub endpoint: String,
    pub database: String,
    pub table: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub insert_batch_rows: usize,
    pub timeout_secs: u64,
}

impl Default for ClickhouseConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8123".into(),
            database: "proxist".into(),
            table: "ticks".into(),
            username: None,
            password: None,
            insert_batch_rows: 100_000,
            timeout_secs: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseTarget {
    pub endpoint: String,
    pub database: String,
    pub table: String,
}

#[async_trait]
pub trait ClickhouseSink: Send + Sync {
    async fn flush_segment(&self, segment: &WalSegment) -> anyhow::Result<()>;
    fn target(&self) -> ClickhouseTarget;
}

#[derive(Clone)]
pub struct ClickhouseHttpSink {
    client: Client,
    config: ClickhouseConfig,
}

impl std::fmt::Debug for ClickhouseHttpSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickhouseHttpSink")
            .field("endpoint", &self.config.endpoint)
            .field("database", &self.config.database)
            .field("table", &self.config.table)
            .finish()
    }
}

impl ClickhouseHttpSink {
    pub fn new(config: ClickhouseConfig) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .context("build ClickHouse HTTP client")?;

        Ok(Self { client, config })
    }

    fn url(&self) -> String {
        format!(
            "{}/?database={}",
            self.config.endpoint.trim_end_matches('/'),
            self.config.database
        )
    }

    fn chunk_size(&self) -> usize {
        std::cmp::max(1, self.config.insert_batch_rows)
    }
}

#[async_trait]
impl ClickhouseSink for ClickhouseHttpSink {
    async fn flush_segment(&self, segment: &WalSegment) -> anyhow::Result<()> {
        if segment.records.is_empty() {
            return Ok(());
        }

        for chunk in segment.records.chunks(self.chunk_size()) {
            let mut body = String::new();
            body.push_str(&format!(
                "INSERT INTO {} FORMAT JSONEachRow\n",
                self.config.table
            ));

            for record in chunk {
                let payload_base64 = BASE64_STANDARD.encode(&record.payload);
                let micros = system_time_to_micros(record.timestamp);
                let row = json!({
                    "tenant": record.tenant,
                    "shard_id": record.shard_id,
                    "symbol": record.symbol,
                    "ts_micros": micros,
                    "payload_base64": payload_base64,
                    "seq": record.seq,
                });
                let line = serde_json::to_string(&row).context("serialize ClickHouse row")?;
                body.push_str(&line);
                body.push('\n');
            }

            let mut request = self.client.post(self.url()).body(body);
            if let Some(username) = &self.config.username {
                request = request.basic_auth(username, self.config.password.as_ref());
            }

            let response = request.send().await.context("send ClickHouse request")?;
            let status = response.status();
            if !status.is_success() {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<unavailable>".into());
                return Err(anyhow!(
                    "clickhouse insert failed: status={} body={}",
                    status,
                    text
                ));
            }
        }

        debug!(
            rows = segment.records.len(),
            endpoint = %self.config.endpoint,
            table = %self.config.table,
            "flushed segment to ClickHouse"
        );

        Ok(())
    }

    fn target(&self) -> ClickhouseTarget {
        ClickhouseTarget {
            endpoint: self.config.endpoint.clone(),
            database: self.config.database.clone(),
            table: self.config.table.clone(),
        }
    }
}

fn system_time_to_micros(ts: SystemTime) -> i64 {
    ts.duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_micros() as i64)
        .unwrap_or_else(|err| {
            let dur = err.duration();
            -(dur.as_micros() as i64)
        })
}
