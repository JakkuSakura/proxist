use std::collections::HashMap;
use std::path::Path;
use std::thread;
use std::time::Instant;

use pxd::expr::Expr;
use pxd::pxl::{
    decode_delete_payload, decode_insert_payload, decode_schema_payload, decode_update_payload, Op,
};
use pxd::types::{infer_column_type, ColumnSpec, ColumnType, Schema, Value};
use tracing::{info, warn};

mod ch;
mod config;
mod translate;
mod wal;

pub use config::Config;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("invalid data: {0}")]
    InvalidData(String),
    #[error("pxd: {0}")]
    Pxd(#[from] pxd::error::Error),
    #[error("clickhouse: {0}")]
    ClickHouse(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
struct ColumnInfo {
    name: String,
    col_type: ColumnType,
    nullable: bool,
}

#[derive(Clone, Debug, Default)]
struct TableState {
    columns: HashMap<String, ColumnInfo>,
    created: bool,
}

impl TableState {
    fn from_schema(schema: &Schema) -> Self {
        let mut columns = HashMap::new();
        for column in schema.columns() {
            columns.insert(
                column.name.to_ascii_lowercase(),
                ColumnInfo {
                    name: column.name.clone(),
                    col_type: column.col_type,
                    nullable: column.nullable,
                },
            );
        }
        Self {
            columns,
            created: false,
        }
    }

    fn ensure_column(&mut self, spec: ColumnSpec) -> bool {
        let key = spec.name.to_ascii_lowercase();
        if let Some(existing) = self.columns.get(&key) {
            if existing.col_type != spec.col_type {
                warn!(
                    column = %spec.name,
                    ?spec.col_type,
                    ?existing.col_type,
                    "column type mismatch; keeping existing type"
                );
            }
            return false;
        }
        self.columns.insert(
            key,
            ColumnInfo {
                name: spec.name,
                col_type: spec.col_type,
                nullable: spec.nullable,
            },
        );
        true
    }

    fn columns_sorted(&self) -> Vec<ColumnInfo> {
        let mut cols: Vec<_> = self.columns.values().cloned().collect();
        cols.sort_by(|a, b| a.name.cmp(&b.name));
        cols
    }
}

#[derive(Debug)]
struct InsertBatch {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    last_lsn: u64,
}

pub fn run(config: Config) -> Result<()> {
    let mut wal = wal::WalTail::open(&config.wal_path)?;
    let mut table_states: HashMap<String, TableState> = HashMap::new();
    let mut checkpoint = load_checkpoint(&config.checkpoint_path)?;
    wal.seek_to_lsn(checkpoint)?;

    let client = ch::ChClient::new(&config)?;
    client.execute(&format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        translate::quote_ident(&config.database)
    ))?;

    info!(
        wal = %config.wal_path.display(),
        lsn = checkpoint,
        "px-chd started"
    );

    let mut batch: Option<InsertBatch> = None;
    let mut last_flush = Instant::now();

    loop {
        match wal.read_next()? {
            wal::WalRead::NeedMore => {
                if config.once {
                    if let Some(pending) = batch.take() {
                        let last_lsn = pending.last_lsn;
                        flush_batch(&client, &mut table_states, &config.database, pending)?;
                        checkpoint = last_lsn;
                        save_checkpoint(&config.checkpoint_path, checkpoint)?;
                    }
                    break;
                }

                if let Some(pending) = batch.take() {
                    if last_flush.elapsed() >= config.flush_interval {
                        let last_lsn = pending.last_lsn;
                        flush_batch(&client, &mut table_states, &config.database, pending)?;
                        checkpoint = last_lsn;
                        save_checkpoint(&config.checkpoint_path, checkpoint)?;
                        last_flush = Instant::now();
                    } else {
                        batch = Some(pending);
                    }
                }
                thread::sleep(config.poll_interval);
            }
            wal::WalRead::Record(record) => {
                if record.lsn <= checkpoint {
                    continue;
                }

                match record.op {
                    Op::Create => {
                        if let Some(pending) = batch.take() {
                            flush_batch(&client, &mut table_states, &config.database, pending)?;
                        }
                        let (table, schema) = decode_schema_payload(&record.payload)?;
                        apply_create(
                            &client,
                            &config.database,
                            &mut table_states,
                            &table,
                            &schema,
                        )?;
                        checkpoint = record.lsn;
                        save_checkpoint(&config.checkpoint_path, checkpoint)?;
                    }
                    Op::Insert => {
                        let (table, columns, rows) = decode_insert_payload(&record.payload)?;
                        let pending = match batch.take() {
                            Some(pending)
                                if pending.table == table && pending.columns == columns => pending,
                            Some(pending) => {
                                flush_batch(&client, &mut table_states, &config.database, pending)?;
                                InsertBatch {
                                    table: table.clone(),
                                    columns: columns.clone(),
                                    rows: Vec::new(),
                                    last_lsn: record.lsn,
                                }
                            }
                            None => InsertBatch {
                                table: table.clone(),
                                columns: columns.clone(),
                                rows: Vec::new(),
                                last_lsn: record.lsn,
                            },
                        };

                        let mut pending = pending;
                        pending.rows.extend(rows);
                        pending.last_lsn = record.lsn;
                        if pending.rows.len() >= config.batch_rows {
                            flush_batch(&client, &mut table_states, &config.database, pending)?;
                            checkpoint = record.lsn;
                            save_checkpoint(&config.checkpoint_path, checkpoint)?;
                            last_flush = Instant::now();
                        } else {
                            batch = Some(pending);
                        }
                    }
                    Op::Update => {
                        if let Some(pending) = batch.take() {
                            flush_batch(&client, &mut table_states, &config.database, pending)?;
                        }
                        let (table, assignments, filter) =
                            decode_update_payload(&record.payload)?;
                        apply_update(
                            &client,
                            &config.database,
                            &mut table_states,
                            &table,
                            &assignments,
                            filter.as_ref(),
                        )?;
                        checkpoint = record.lsn;
                        save_checkpoint(&config.checkpoint_path, checkpoint)?;
                    }
                    Op::Delete => {
                        if let Some(pending) = batch.take() {
                            flush_batch(&client, &mut table_states, &config.database, pending)?;
                        }
                        let (table, filter) = decode_delete_payload(&record.payload)?;
                        apply_delete(
                            &client,
                            &config.database,
                            &mut table_states,
                            &table,
                            filter.as_ref(),
                        )?;
                        checkpoint = record.lsn;
                        save_checkpoint(&config.checkpoint_path, checkpoint)?;
                    }
                    other => {
                        warn!(?other, "wal record ignored");
                    }
                }
            }
        }
    }

    Ok(())
}

fn apply_create(
    client: &ch::ChClient,
    database: &str,
    tables: &mut HashMap<String, TableState>,
    table: &str,
    schema: &Schema,
) -> Result<()> {
    let state = tables
        .entry(table.to_string())
        .or_insert_with(|| TableState::from_schema(schema));

    for column in schema.columns() {
        state.ensure_column(ColumnSpec {
            name: column.name.clone(),
            col_type: column.col_type,
            nullable: column.nullable,
        });
    }

    ensure_table(client, database, table, state)
}

fn apply_update(
    client: &ch::ChClient,
    database: &str,
    tables: &mut HashMap<String, TableState>,
    table: &str,
    assignments: &[(String, Value)],
    filter: Option<&Expr>,
) -> Result<()> {
    let state = tables
        .entry(table.to_string())
        .or_insert_with(TableState::default);

    for (name, value) in assignments {
        state.ensure_column(ColumnSpec {
            name: name.clone(),
            col_type: value.column_type(),
            nullable: true,
        });
    }

    ensure_table(client, database, table, state)?;
    ensure_columns(client, database, table, state)?;

    let mut set_parts = Vec::with_capacity(assignments.len());
    for (name, value) in assignments {
        let value_sql = translate::value_to_sql(value)?;
        set_parts.push(format!(
            "{} = {}",
            translate::quote_ident(name),
            value_sql
        ));
    }

    let predicate = match filter {
        Some(expr) => translate::expr_to_sql(expr)?,
        None => "1".to_string(),
    };

    let sql = format!(
        "ALTER TABLE {} UPDATE {} WHERE {}",
        translate::qualify_table(database, table),
        set_parts.join(", "),
        predicate
    );
    client.execute(&sql)
}

fn apply_delete(
    client: &ch::ChClient,
    database: &str,
    tables: &mut HashMap<String, TableState>,
    table: &str,
    filter: Option<&Expr>,
) -> Result<()> {
    let state = tables
        .entry(table.to_string())
        .or_insert_with(TableState::default);
    ensure_table(client, database, table, state)?;

    let predicate = match filter {
        Some(expr) => translate::expr_to_sql(expr)?,
        None => "1".to_string(),
    };

    let sql = format!(
        "ALTER TABLE {} DELETE WHERE {}",
        translate::qualify_table(database, table),
        predicate
    );
    client.execute(&sql)
}

fn flush_batch(
    client: &ch::ChClient,
    tables: &mut HashMap<String, TableState>,
    database: &str,
    batch: InsertBatch,
) -> Result<()> {
    let state = tables
        .entry(batch.table.clone())
        .or_insert_with(TableState::default);

    let mut new_columns = Vec::new();
    for (idx, name) in batch.columns.iter().enumerate() {
        let values: Vec<Value> = batch
            .rows
            .iter()
            .filter_map(|row| row.get(idx).cloned())
            .collect();
        let col_type = infer_column_type(&values);
        if state.ensure_column(ColumnSpec {
            name: name.clone(),
            col_type,
            nullable: true,
        }) {
            new_columns.push(name.clone());
        }
    }

    ensure_table(client, database, &batch.table, state)?;
    ensure_columns(client, database, &batch.table, state)?;

    let sql = translate::insert_values_sql(
        database,
        &batch.table,
        &batch.columns,
        &batch.rows,
    )?;
    client.execute(&sql)
}

fn ensure_table(
    client: &ch::ChClient,
    database: &str,
    table: &str,
    state: &mut TableState,
) -> Result<()> {
    if state.created {
        return Ok(());
    }

    if state.columns.is_empty() {
        return Err(Error::InvalidData(format!(
            "no columns for table {table}"
        )));
    }

    let columns = state.columns_sorted();
    let mut col_defs = Vec::with_capacity(columns.len());
    for col in columns {
        col_defs.push(format!(
            "{} {}",
            translate::quote_ident(&col.name),
            translate::column_type_sql(col.col_type, col.nullable)
        ));
    }

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = MergeTree ORDER BY tuple()",
        translate::qualify_table(database, table),
        col_defs.join(", ")
    );
    client.execute(&sql)?;
    state.created = true;
    Ok(())
}

fn ensure_columns(
    client: &ch::ChClient,
    database: &str,
    table: &str,
    state: &TableState,
) -> Result<()> {
    for col in state.columns.values() {
        let sql = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
            translate::qualify_table(database, table),
            translate::quote_ident(&col.name),
            translate::column_type_sql(col.col_type, col.nullable)
        );
        client.execute(&sql)?;
    }
    Ok(())
}

fn load_checkpoint(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let content = std::fs::read_to_string(path)?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }
    let value = trimmed
        .split(':')
        .last()
        .ok_or_else(|| Error::InvalidData("invalid checkpoint".to_string()))?;
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::InvalidData("invalid checkpoint".to_string()))
}

fn save_checkpoint(path: &Path, lsn: u64) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, format!("lsn:{lsn}\n"))?;
    Ok(())
}

impl From<ch::Error> for Error {
    fn from(err: ch::Error) -> Self {
        Error::ClickHouse(err.to_string())
    }
}

impl From<translate::Error> for Error {
    fn from(err: translate::Error) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<wal::Error> for Error {
    fn from(err: wal::Error) -> Self {
        Error::Protocol(err.to_string())
    }
}
