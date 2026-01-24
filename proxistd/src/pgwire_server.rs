use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, StreamExt};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use serde_json::{Map, Value as JsonValue};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::{execute_sql_batch, AppError, AppState, DialectMode, SqlBatchResult, SqlResult};

pub async fn serve(
    addr: SocketAddr,
    state: Arc<AppState>,
    dialect: DialectMode,
) -> anyhow::Result<()> {
    let handler = Arc::new(PgHandler { state, dialect });
    let factory = Arc::new(PgHandlerFactory { handler });
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "postgres wire listener started");
    loop {
        let (socket, _) = listener.accept().await?;
        let factory_ref = factory.clone();
        tokio::spawn(async move {
            if let Err(err) = process_socket(socket, None, factory_ref).await {
                error!(error = ?err, "pgwire socket error");
            }
        });
    }
}

struct PgHandler {
    state: Arc<AppState>,
    dialect: DialectMode,
}

#[async_trait]
impl NoopStartupHandler for PgHandler {}

#[async_trait]
impl SimpleQueryHandler for PgHandler {
    async fn do_query<'a, C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let outputs = execute_sql_batch(&self.state, self.dialect.clone(), query)
            .await
            .map_err(app_error_to_pg)?;
        let mut responses = Vec::new();
        for output in outputs {
            let response = match output {
                SqlBatchResult::Text(text) => text_response(text),
                SqlBatchResult::Scheduler(result) => match result {
                    SqlResult::Rows(rows) => rows_response(rows),
                    SqlResult::Text(text) => text_response(text),
                    SqlResult::Clickhouse(wire) => match wire.format {
                        crate::scheduler::ClickhouseWireFormat::JsonEachRow => {
                            match parse_jsoneachrow_to_maps(&wire.body) {
                                Ok(rows) => rows_response(rows),
                                Err(err) => return Err(app_error_to_pg(AppError(err))),
                            }
                        }
                        crate::scheduler::ClickhouseWireFormat::Other
                        | crate::scheduler::ClickhouseWireFormat::Unknown => {
                            text_response(wire.body)
                        }
                    },
                },
            };
            responses.push(response);
        }
        if responses.is_empty() {
            responses.push(Response::Execution(Tag::new("OK").with_rows(0)));
        }
        Ok(responses)
    }
}

struct PgHandlerFactory {
    handler: Arc<PgHandler>,
}

impl PgWireServerHandlers for PgHandlerFactory {
    type StartupHandler = PgHandler;
    type SimpleQueryHandler = PgHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

fn app_error_to_pg(err: AppError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        err.0.to_string(),
    )))
}

fn parse_jsoneachrow_to_maps(text: &str) -> anyhow::Result<Vec<Map<String, JsonValue>>> {
    let mut rows = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: JsonValue = serde_json::from_str(trimmed)?;
        match value {
            JsonValue::Object(map) => rows.push(map),
            other => {
                let mut map = Map::new();
                map.insert("value".to_string(), other);
                rows.push(map);
            }
        }
    }
    Ok(rows)
}

fn text_response(text: String) -> Response<'static> {
    let mut row = Map::new();
    row.insert("message".to_string(), JsonValue::String(text));
    rows_response(vec![row])
}

fn rows_response(rows: Vec<Map<String, JsonValue>>) -> Response<'static> {
    let columns = extract_columns(&rows);
    let schema = Arc::new(
        columns
            .iter()
            .map(|name| FieldInfo::new(name.clone(), None, None, Type::VARCHAR, FieldFormat::Text))
            .collect::<Vec<_>>(),
    );

    let data = rows.into_iter();
    let schema_ref = schema.clone();
    let stream = stream::iter(data).map(move |row| {
        let mut encoder = DataRowEncoder::new(schema_ref.clone());
        for col in &columns {
            let value = row.get(col).and_then(json_value_to_string);
            encoder.encode_field(&value)?;
        }
        encoder.finish()
    });
    Response::Query(QueryResponse::new(schema, stream))
}

fn extract_columns(rows: &[Map<String, JsonValue>]) -> Vec<String> {
    if let Some(first) = rows.first() {
        let mut cols = first.keys().cloned().collect::<Vec<_>>();
        cols.sort();
        cols
    } else {
        vec!["message".to_string()]
    }
}

fn json_value_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Null => None,
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(b.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            serde_json::to_string(value).ok()
        }
    }
}
