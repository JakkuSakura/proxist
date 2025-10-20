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
    pub max_retries: usize,
    pub retry_backoff_ms: u64,
    pub query_timeout_secs: Option<u64>,
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
            max_retries: 3,
            retry_backoff_ms: 200,
            query_timeout_secs: None,
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

        for (chunk_idx, chunk) in segment.records.chunks(self.chunk_size()).enumerate() {
            let mut body = String::new();
            let insert_id = format!(
                "{}-{}-{}",
                self.config.table.replace('.', "_"),
                segment.base_offset.0,
                chunk_idx
            );
            body.push_str(&format!(
                "INSERT INTO {} SETTINGS insert_id='{}', deduplicate=1 FORMAT JSONEachRow\n",
                self.config.table, insert_id
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

            let mut attempt = 0;
            loop {
                let response = request
                    .try_clone()
                    .ok_or_else(|| anyhow!("failed to clone ClickHouse request"))?
                    .send()
                    .await;

                match response {
                    Ok(resp) if resp.status().is_success() => break,
                    Ok(resp) => {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_else(|_| "<unavailable>".into());
                        attempt += 1;
                        if attempt > self.config.max_retries {
                            return Err(anyhow!(
                                "clickhouse insert failed after {} retries: status={} body={}",
                                self.config.max_retries,
                                status,
                                body
                            ));
                        }
                        tokio::time::sleep(Duration::from_millis(
                            self.config.retry_backoff_ms * attempt as u64,
                        ))
                        .await;
                        continue;
                    }
                    Err(err) => {
                        attempt += 1;
                        if attempt > self.config.max_retries {
                            return Err(anyhow!(
                                "clickhouse insert error after {} retries: {err}",
                                self.config.max_retries
                            ));
                        }
                        tokio::time::sleep(Duration::from_millis(
                            self.config.retry_backoff_ms * attempt as u64,
                        ))
                        .await;
                        continue;
                    }
                }
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

#[derive(Clone)]
pub struct ClickhouseHttpClient {
    client: Client,
    config: ClickhouseConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickhouseQueryRow {
    pub symbol: String,
    pub ts_micros: i64,
    pub payload_base64: String,
}

impl ClickhouseHttpClient {
    pub fn new(config: ClickhouseConfig) -> anyhow::Result<Self> {
        let timeout = config.query_timeout_secs.unwrap_or(config.timeout_secs);
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .context("build ClickHouse query client")?;
        Ok(Self { client, config })
    }

    fn url(&self) -> String {
        format!(
            "{}/?database={}",
            self.config.endpoint.trim_end_matches('/'),
            self.config.database
        )
    }

    fn escape(value: &str) -> String {
        value.replace('\'', "\\'")
    }

    async fn execute(&self, sql: String) -> anyhow::Result<Vec<ClickhouseQueryRow>> {
        let mut request = self.client.post(self.url()).body(sql);
        if let Some(username) = &self.config.username {
            request = request.basic_auth(username, self.config.password.as_ref());
        }
        let response = request.send().await.context("send ClickHouse query")?;
        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".into());
            return Err(anyhow!(
                "clickhouse query failed: status={} body={}",
                status,
                body
            ));
        }
        let body = response
            .text()
            .await
            .context("read ClickHouse query body")?;
        let mut rows = Vec::new();
        for line in body.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            rows.push(serde_json::from_str(trimmed).context("parse ClickHouse query row")?);
        }
        Ok(rows)
    }

    pub async fn fetch_last_by(
        &self,
        tenant: &str,
        symbols: &[String],
        ts_micros: i64,
    ) -> anyhow::Result<Vec<ClickhouseQueryRow>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        let symbol_list = symbols
            .iter()
            .map(|sym| format!("'{}'", Self::escape(sym)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT symbol, ts_micros, payload_base64 \
             FROM {table} \
             WHERE tenant = '{tenant}' \
               AND symbol IN ({symbols}) \
               AND ts_micros <= {ts} \
             ORDER BY symbol, ts_micros DESC \
             LIMIT 1 BY symbol \
             FORMAT JSONEachRow",
            table = self.config.table,
            tenant = Self::escape(tenant),
            symbols = symbol_list,
            ts = ts_micros
        );
        self.execute(sql).await
    }

    pub async fn fetch_range(
        &self,
        tenant: &str,
        symbols: &[String],
        start_micros: i64,
        end_micros: i64,
    ) -> anyhow::Result<Vec<ClickhouseQueryRow>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        let symbol_list = symbols
            .iter()
            .map(|sym| format!("'{}'", Self::escape(sym)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT symbol, ts_micros, payload_base64 \
             FROM {table} \
             WHERE tenant = '{tenant}' \
               AND symbol IN ({symbols}) \
               AND ts_micros BETWEEN {start} AND {end} \
             ORDER BY ts_micros ASC \
             FORMAT JSONEachRow",
            table = self.config.table,
            tenant = Self::escape(tenant),
            symbols = symbol_list,
            start = start_micros,
            end = end_micros
        );
        self.execute(sql).await
    }

    pub fn target(&self) -> ClickhouseTarget {
        ClickhouseTarget {
            endpoint: self.config.endpoint.clone(),
            database: self.config.database.clone(),
            table: self.config.table.clone(),
        }
    }
}
