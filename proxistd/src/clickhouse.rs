use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use proxist_core::ingest::IngestSegment;
use crate::scheduler::{ColumnType, TableRegistry};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

#[cfg(feature = "clickhouse-native")]
use clickhouse_rs::types::{Block, Complex, SqlType};
#[cfg(feature = "clickhouse-native")]
use clickhouse_rs::Pool;

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
    async fn flush_segment(&self, segment: &IngestSegment) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct ClickhouseHttpSink {
    client: Client,
    config: ClickhouseConfig,
    registry: std::sync::Arc<TableRegistry>,
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
    pub fn new(config: ClickhouseConfig, registry: std::sync::Arc<TableRegistry>) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .context("build ClickHouse HTTP client")?;

        Ok(Self { client, config, registry })
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
    async fn flush_segment(&self, segment: &IngestSegment) -> anyhow::Result<()> {
        if segment.rows.is_empty() {
            return Ok(());
        }

        for chunk in segment.rows.chunks(self.chunk_size()) {
            let mut body = String::new();
            body.push_str(&format!(
                "INSERT INTO {} FORMAT JSONEachRow\n",
                self.config.table
            ));

            for record in chunk {
                let expected = self.config.table.as_str();
                let qualified = format!("{}.{}", self.config.database, self.config.table);
                if record.table != expected && record.table != qualified {
                    anyhow::bail!(
                        "ingest row table {} does not match ClickHouse sink table {}",
                        record.table,
                        self.config.table
                    );
                }
                let cfg = self
                    .registry
                    .table_config(&record.table)
                    .ok_or_else(|| anyhow!("table schema not registered for {}", record.table))?;
                if record.values.len() != cfg.columns.len() {
                    anyhow::bail!(
                        "ingest row value count mismatch: expected {}, got {}",
                        cfg.columns.len(),
                        record.values.len()
                    );
                }
                let mut map = serde_json::Map::with_capacity(cfg.columns.len());
                for (idx, col) in cfg.columns.iter().enumerate() {
                    let ty = cfg
                        .column_types
                        .get(idx)
                        .copied()
                        .unwrap_or(ColumnType::Text);
                    let value = bytes_to_json(&record.values[idx], ty)?;
                    map.insert(col.clone(), value);
                }
                let row = serde_json::Value::Object(map);
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
                                "clickhouse insert error after {} retries: {}",
                                self.config.max_retries,
                                err
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
            rows = segment.rows.len(),
            endpoint = %self.config.endpoint,
            table = %self.config.table,
            "flushed segment to ClickHouse"
        );

        Ok(())
    }
}

fn bytes_to_json(value: &bytes::Bytes, ty: ColumnType) -> anyhow::Result<serde_json::Value> {
    if value.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    let text = unsafe { std::str::from_utf8_unchecked(value) };
    let json = match ty {
        ColumnType::Text => serde_json::Value::String(text.to_string()),
        ColumnType::Int64 => {
            let parsed = text
                .parse::<i64>()
                .map_err(|err| anyhow!("invalid int literal: {err}"))?;
            serde_json::Value::Number(parsed.into())
        }
        ColumnType::Float64 => {
            let parsed = text
                .parse::<f64>()
                .map_err(|err| anyhow!("invalid float literal: {err}"))?;
            serde_json::Number::from_f64(parsed)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        ColumnType::Bool => {
            let parsed = match text {
                "true" | "TRUE" | "True" => true,
                "false" | "FALSE" | "False" => false,
                _ => return Err(anyhow!("invalid bool literal")),
            };
            serde_json::Value::Bool(parsed)
        }
    };
    Ok(json)
}

#[derive(Clone)]
pub struct ClickhouseHttpClient {
    client: Client,
    config: ClickhouseConfig,
}

#[derive(Clone)]
pub struct ClickhouseNativeClient {
    #[cfg(feature = "clickhouse-native")]
    pool: Pool,
}

impl ClickhouseNativeClient {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        #[cfg(feature = "clickhouse-native")]
        {
            Ok(Self {
                pool: Pool::new(url),
            })
        }
        #[cfg(not(feature = "clickhouse-native"))]
        {
            let _ = url;
            anyhow::bail!("clickhouse-native feature not enabled");
        }
    }

    pub async fn query_rowset(
        &self,
        sql: &str,
    ) -> anyhow::Result<crate::scheduler::RowSet> {
        #[cfg(feature = "clickhouse-native")]
        {
            let mut handle = self.pool.get_handle().await?;
            let block = handle.query(sql).fetch_all().await?;
            block_to_rowset(&block)
        }
        #[cfg(not(feature = "clickhouse-native"))]
        {
            let _ = sql;
            anyhow::bail!("clickhouse-native feature not enabled");
        }
    }
}

#[cfg(feature = "clickhouse-native")]
fn block_to_rowset(block: &Block<Complex>) -> anyhow::Result<crate::scheduler::RowSet> {
    let columns = block
        .columns()
        .iter()
        .map(|col| col.name().to_ascii_lowercase())
        .collect::<Vec<_>>();
    let column_types = block
        .columns()
        .iter()
        .map(|col| map_sql_type_to_column_type(col.sql_type()))
        .collect::<Vec<_>>();
    let mut column_values = Vec::with_capacity(block.columns().len());
    for col in block.columns() {
        column_values.push(column_to_values(col)?);
    }

    let row_count = block.row_count();
    let mut rows = Vec::with_capacity(row_count);
    for row_idx in 0..row_count {
        let mut row = Vec::with_capacity(columns.len());
        for col in &column_values {
            row.push(col[row_idx].clone());
        }
        rows.push(row);
    }
    Ok(crate::scheduler::RowSet {
        columns,
        column_types,
        rows,
    })
}

#[cfg(feature = "clickhouse-native")]
fn column_to_values(
    column: &clickhouse_rs::types::Column<Complex>,
) -> anyhow::Result<Vec<crate::scheduler::ScalarValue>> {
    use crate::scheduler::ScalarValue;
    match column.sql_type() {
        SqlType::String | SqlType::FixedString(_) => {
            let iter = column.iter::<&[u8]>()?;
            Ok(iter
                .map(|v| ScalarValue::String(String::from_utf8_lossy(v).to_string()))
                .collect())
        }
        SqlType::Int64 => {
            let iter = column.iter::<i64>()?;
            Ok(iter.map(|v| ScalarValue::Int64(*v)).collect())
        }
        SqlType::Int32 => {
            let iter = column.iter::<i32>()?;
            Ok(iter.map(|v| ScalarValue::Int64(i64::from(*v))).collect())
        }
        SqlType::Int16 => {
            let iter = column.iter::<i16>()?;
            Ok(iter.map(|v| ScalarValue::Int64(i64::from(*v))).collect())
        }
        SqlType::Int8 => {
            let iter = column.iter::<i8>()?;
            Ok(iter.map(|v| ScalarValue::Int64(i64::from(*v))).collect())
        }
        SqlType::UInt64 => {
            let iter = column.iter::<u64>()?;
            Ok(iter
                .map(|v| {
                    i64::try_from(*v)
                        .map(ScalarValue::Int64)
                        .unwrap_or_else(|_| ScalarValue::String(v.to_string()))
                })
                .collect())
        }
        SqlType::UInt32 => {
            let iter = column.iter::<u32>()?;
            Ok(iter.map(|v| ScalarValue::Int64(i64::from(*v))).collect())
        }
        SqlType::UInt16 => {
            let iter = column.iter::<u16>()?;
            Ok(iter.map(|v| ScalarValue::Int64(i64::from(*v))).collect())
        }
        SqlType::UInt8 => {
            let iter = column.iter::<u8>()?;
            Ok(iter.map(|v| ScalarValue::Int64(i64::from(*v))).collect())
        }
        SqlType::Float64 => {
            let iter = column.iter::<f64>()?;
            Ok(iter.map(|v| ScalarValue::Float64(*v)).collect())
        }
        SqlType::Float32 => {
            let iter = column.iter::<f32>()?;
            Ok(iter.map(|v| ScalarValue::Float64(f64::from(*v))).collect())
        }
        SqlType::Bool => {
            let iter = column.iter::<bool>()?;
            Ok(iter.map(|v| ScalarValue::Bool(*v)).collect())
        }
        SqlType::Nullable(inner) => match *inner {
            SqlType::String | SqlType::FixedString(_) => {
                let iter = column.iter::<Option<&[u8]>>()?;
                Ok(iter
                    .map(|v| {
                        v.map(|s| ScalarValue::String(String::from_utf8_lossy(s).to_string()))
                            .unwrap_or(ScalarValue::Null)
                    })
                    .collect())
            }
            SqlType::Int64 => {
                let iter = column.iter::<Option<i64>>()?;
                Ok(iter
                    .map(|v| v.map(|num| ScalarValue::Int64(*num)).unwrap_or(ScalarValue::Null))
                    .collect())
            }
            SqlType::UInt64 => {
                let iter = column.iter::<Option<u64>>()?;
                Ok(iter
                    .map(|v| {
                        v.map(|num| {
                            i64::try_from(*num)
                                .map(ScalarValue::Int64)
                                .unwrap_or_else(|_| ScalarValue::String(num.to_string()))
                        })
                        .unwrap_or(ScalarValue::Null)
                    })
                    .collect())
            }
            SqlType::Float64 => {
                let iter = column.iter::<Option<f64>>()?;
                Ok(iter
                    .map(|v| v.map(|num| ScalarValue::Float64(*num)).unwrap_or(ScalarValue::Null))
                    .collect())
            }
            SqlType::Bool => {
                let iter = column.iter::<Option<bool>>()?;
                Ok(iter
                    .map(|v| v.map(|num| ScalarValue::Bool(*num)).unwrap_or(ScalarValue::Null))
                    .collect())
            }
            _ => anyhow::bail!("unsupported nullable column type {inner:?}"),
        },
        other => anyhow::bail!("unsupported column type {other:?}"),
    }
}

#[cfg(feature = "clickhouse-native")]
fn map_sql_type_to_column_type(sql_type: SqlType) -> crate::scheduler::ColumnType {
    use crate::scheduler::ColumnType;
    match sql_type {
        SqlType::Int8
        | SqlType::Int16
        | SqlType::Int32
        | SqlType::Int64
        | SqlType::UInt8
        | SqlType::UInt16
        | SqlType::UInt32
        | SqlType::UInt64 => ColumnType::Int64,
        SqlType::Float32 | SqlType::Float64 => ColumnType::Float64,
        SqlType::Bool => ColumnType::Bool,
        SqlType::Nullable(inner) => map_sql_type_to_column_type(inner.clone()),
        SqlType::String | SqlType::FixedString(_) => ColumnType::Text,
        _ => ColumnType::Text,
    }
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

    pub async fn execute_raw(&self, sql: &str) -> anyhow::Result<String> {
        let mut request = self.client.post(self.url()).body(sql.to_string());
        if let Some(username) = &self.config.username {
            request = request.basic_auth(username, self.config.password.as_ref());
        }
        let response = request.send().await.context("send raw ClickHouse query")?;
        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".into());
            anyhow::bail!("clickhouse query failed: status={} body={}", status, body);
        }
        response
            .text()
            .await
            .context("read raw ClickHouse query response")
    }

    pub fn target(&self) -> ClickhouseTarget {
        ClickhouseTarget {
            endpoint: self.config.endpoint.clone(),
            database: self.config.database.clone(),
            table: self.config.table.clone(),
        }
    }
}
