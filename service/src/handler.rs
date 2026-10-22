//! Request dispatch and the response envelope `{request_id, ok, result | error}`.
//! The same binary backs both functions (lance-control, lance-ingest), which only differ in
//! timeout/memory: the Go client sends control ops to lance-control, and writes and queries to
//! lance-ingest. Every op is accepted by either.

use std::time::Instant;

use serde::Serialize;
use serde_json::{Value, json};

use crate::app::App;
use crate::error::{Error, Result, code};
use crate::ops::{compact, control, query, write};
use crate::protocol::{Request, TableInfoReq};

pub async fn handle(app: &App, request_id: &str, payload: Value) -> Value {
    handle_with(app, request_id, payload, false).await
}

/// `handle` for the HTTP API (API key): ops that write are rejected with READ_ONLY.
pub async fn handle_read_only(app: &App, request_id: &str, payload: Value) -> Value {
    handle_with(app, request_id, payload, true).await
}

async fn handle_with(app: &App, request_id: &str, payload: Value, read_only: bool) -> Value {
    let started = Instant::now();
    let (op, table, result) = match serde_json::from_value::<Request>(payload) {
        Ok(req) if read_only && !req.is_read_only() => {
            let op = req.op();
            let message = format!("{op} writes; the HTTP API is read-only (writes need the direct Invoke)");
            (op, req.table().map(str::to_string), Err(Error::api(code::READ_ONLY, message)))
        }
        Ok(req) => {
            let (op, table) = (req.op(), req.table().map(str::to_string));
            // Boxed: the op futures (Lance writes) are deep enough to overflow the
            // compiler's layout recursion limit in the binaries otherwise.
            (op, table, Box::pin(dispatch(app, req)).await)
        }
        Err(e) => ("?", None, Err(Error::api(code::BAD_REQUEST, format!("invalid request: {e}")))),
    };
    let elapsed_ms = started.elapsed().as_millis() as u64;
    match result {
        Ok(result) => {
            tracing::info!(request_id, op, table, elapsed_ms, "ok");
            json!({ "request_id": request_id, "ok": true, "result": result })
        }
        Err(e) => {
            let api = e.to_api();
            match &e {
                Error::Internal(err) => tracing::error!(request_id, op, table, elapsed_ms, error = ?err, "internal error"),
                Error::Api(_) => tracing::warn!(request_id, op, table, elapsed_ms, code = api.code, message = %api.message, "rejected"),
            }
            json!({ "request_id": request_id, "ok": false, "error": api })
        }
    }
}

fn to_value<T: Serialize>(v: T) -> Result<Value> {
    Ok(serde_json::to_value(v).map_err(anyhow::Error::from)?)
}

async fn dispatch(app: &App, req: Request) -> Result<Value> {
    match req {
        Request::CreateTable(r) => to_value(control::create_table(app, r).await?),
        Request::DescribeTable(r) => to_value(control::table_info(app, TableInfoReq { table: r.table, detail: false }).await?),
        Request::TableInfo(r) => to_value(control::table_info(app, r).await?),
        Request::ListTables(_) => to_value(json!({ "tables": control::list_tables(app).await? })),
        Request::ListPartitions(r) => to_value(control::list_partitions(app, r).await?),
        Request::GetBatch(r) => to_value(control::get_batch(app, r).await?),
        Request::Ingest(r) => to_value(write::ingest(app, r).await?),
        Request::ReplacePartitions(r) => to_value(write::replace_partitions(app, r).await?),
        Request::Compact(r) => to_value(compact::compact(app, r).await?),
        Request::Query(r) => to_value(query::query(app, r, false).await?),
        Request::Explain(r) => to_value(query::query(app, r, true).await?),
    }
}
