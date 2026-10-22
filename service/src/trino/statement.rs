//! `POST /v1/statement`: runs the SQL and streams one complete `QueryResults` document
//! (trino-api-plan.md §2.2, §6). No `nextUri`: the response is final.
//!
//! The document is written in this order: id, infoUri, columns, data (row by row as DataFusion
//! produces them), then stats, warnings and error once the outcome is known.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use datafusion::sql::sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures::StreamExt;
use lambda_runtime::StreamResponse;
use serde_json::{Value, json};

use super::catalog::context;
use super::encode::{BatchWriter, TrinoColumn, columns};
use super::http::{BodyStream, HttpRequest, Sink, json_response, respond};
use super::protocol::{Stats, error_json, query_id, truncated_warning};
use super::session::{Action, classify};
use crate::app::App;
use crate::error::{Error, code};
use crate::ops::query::{df_error, plan};

pub fn run(app: Arc<App>, request_id: &str, req: &HttpRequest) -> StreamResponse<BodyStream> {
    let id = query_id(request_id);
    let info_uri = format!("{}/ui/query.html?{id}", req.base_url());
    let sql = req.body.trim().trim_end_matches(';').trim().to_string();
    let doc = |extra: Value| {
        let mut doc = json!({ "id": id, "infoUri": info_uri, "warnings": [] });
        doc.as_object_mut().unwrap().extend(extra.as_object().cloned().unwrap_or_default());
        doc
    };
    let finished = || Stats { state: "FINISHED", elapsed_ms: 0, rows: 0, bytes_read: 0 }.to_json();
    match classify(&sql, req, &app.cfg.trino) {
        Ok(Action::Done { update_type, headers }) => {
            tracing::info!(request_id, query_id = id, update_type, "trino session statement");
            // Like Trino: one boolean `result` column with `true` (JDBC fails on a result with no columns).
            let result = json!([{ "name": "result", "type": "boolean", "typeSignature": { "rawType": "boolean", "arguments": [] } }]);
            let body = json!({ "stats": finished(), "updateType": update_type, "columns": result, "data": [[true]] });
            json_response(200, &headers, &doc(body))
        }
        Err(e) => {
            log_failure(request_id, &id, &sql, &e);
            let mut stats = finished();
            stats["state"] = json!("FAILED");
            json_response(200, &[], &doc(json!({ "stats": stats, "error": error_json(&e) })))
        }
        Ok(Action::Query(query)) => {
            let (response, sink) = respond(200, &[], req.accepts_gzip());
            let request_id = request_id.to_string();
            tokio::spawn(async move { execute(app, request_id, id, info_uri, query, sink).await });
            response
        }
    }
}

/// Trino names an output column without an alias `_colN` (N = its position) unless it is a
/// plain column reference; DataFusion would name it after the expression (`Int64(1)`).
fn trino_names(sql: &str, cols: &mut [TrinoColumn]) {
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else { return };
    let Some(Statement::Query(query)) = statements.first() else { return };
    let mut body = query.body.as_ref();
    let select = loop {
        match body {
            SetExpr::Select(select) => break select,
            SetExpr::SetOperation { left, .. } => body = left,
            SetExpr::Query(q) => body = q.body.as_ref(),
            _ => return,
        }
    };
    // With a wildcard the items don't line up with the columns.
    if select.projection.len() != cols.len() {
        return;
    }
    for (i, (item, col)) in select.projection.iter().zip(cols.iter_mut()).enumerate() {
        if let SelectItem::UnnamedExpr(expr) = item
            && !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
        {
            col.name = format!("_col{i}");
        }
    }
}

/// Why the rows stopped before the end of the result.
enum Cut {
    Rows,
    Size,
    Time,
}

async fn execute(app: Arc<App>, request_id: String, id: String, info_uri: String, sql: String, mut sink: Sink) {
    let started = Instant::now();
    let (q, t) = (&app.cfg.query, &app.cfg.trino);
    let first_deadline = tokio::time::Instant::from_std(started + Duration::from_millis(q.max_timeout_ms));
    let stream_deadline = tokio::time::Instant::from_std(started + Duration::from_secs(t.max_stream_seconds));
    let max_bytes = t.max_result_mb * 1024 * 1024;
    app.query.warm.store(true, Ordering::Relaxed);
    // One invocation at a time per container: the I/O counters belong to this query.
    app.query.io.incremental_stats();

    // Planning, table opening and every blocking operator run before the first batch.
    let first = tokio::time::timeout_at(first_deadline, async {
        let ctx = context(&app, &sql).await?;
        let (df, _) = plan(&ctx, &sql).await?;
        let df = df.limit(0, Some(t.max_rows + 1)).map_err(df_error)?;
        let mut cols = columns(df.schema().as_arrow());
        trino_names(&sql, &mut cols);
        let mut stream = df.execute_stream().await.map_err(df_error)?;
        let first = stream.next().await.transpose().map_err(df_error)?;
        Ok::<_, Error>((cols, stream, first))
    })
    .await
    .unwrap_or_else(|_| Err(Error::api(code::QUERY_TIMEOUT, format!("no rows within {} ms", q.max_timeout_ms))));

    let mut head = json!({ "id": id, "infoUri": info_uri });
    let (cols, mut stream, first) = match first {
        Ok(v) => v,
        Err(e) => {
            log_failure(&request_id, &id, &sql, &e);
            head["stats"] = stats(&app, started, "FAILED", 0);
            head["error"] = error_json(&e);
            head["warnings"] = json!([]);
            sink.buf = serde_json::to_vec(&head).unwrap_or_default();
            sink.finish().await;
            return;
        }
    };

    head["columns"] = Value::Array(cols.iter().map(|c| c.to_json()).collect());
    // The object stays open: rows and the trailing fields follow.
    let head = serde_json::to_string(&head).unwrap_or_default();
    sink.buf.extend_from_slice(&head.as_bytes()[..head.len() - 1]);

    let (mut rows, mut cut, mut failure) = (0u64, None, None);
    let mut batch = first;
    'batches: while let Some(b) = batch {
        let writer = match BatchWriter::new(&b, &cols) {
            Ok(w) => w,
            Err(e) => {
                failure = Some(Error::from(e));
                break;
            }
        };
        for row in 0..b.num_rows() {
            if rows as usize == t.max_rows {
                cut = Some(Cut::Rows);
                break 'batches;
            }
            if sink.written + sink.buf.len() >= max_bytes {
                cut = Some(Cut::Size);
                break 'batches;
            }
            sink.buf.extend_from_slice(if rows == 0 { b",\"data\":[" } else { b"," });
            writer.write_row(row, &mut sink.buf);
            rows += 1;
        }
        if !sink.flush().await {
            return; // the client is gone
        }
        batch = match tokio::time::timeout_at(stream_deadline, stream.next()).await {
            Err(_) => {
                cut = Some(Cut::Time);
                break;
            }
            Ok(next) => match next.transpose() {
                Ok(next) => next,
                Err(e) => {
                    failure = Some(df_error(e));
                    break;
                }
            },
        };
    }
    drop(stream);

    if rows > 0 {
        sink.buf.push(b']');
    }
    let state = if failure.is_some() { "FAILED" } else { "FINISHED" };
    let mut tail = json!({ "stats": stats(&app, started, state, rows), "warnings": [] });
    if let Some(reason) = &cut {
        let reason = match reason {
            Cut::Rows => format!("trino.max_rows = {}", t.max_rows),
            Cut::Size => format!("trino.max_result_mb = {}", t.max_result_mb),
            Cut::Time => format!("trino.max_stream_seconds = {}", t.max_stream_seconds),
        };
        tail["warnings"] = json!([truncated_warning(rows, &reason)]);
        tracing::warn!(request_id, query_id = id, rows, reason, sql, "trino result truncated");
    }
    if let Some(e) = &failure {
        log_failure(&request_id, &id, &sql, e);
        tail["error"] = error_json(e);
    }
    // Splice the tail object's fields into the open document.
    let tail = serde_json::to_string(&tail).unwrap_or_default();
    sink.buf.push(b',');
    sink.buf.extend_from_slice(&tail.as_bytes()[1..]);
    let bytes = sink.written + sink.buf.len();
    sink.finish().await;
    tracing::info!(request_id, query_id = id, rows, bytes, elapsed_ms = started.elapsed().as_millis() as u64, "trino query");
}

fn stats(app: &App, started: Instant, state: &'static str, rows: u64) -> Value {
    let io = app.query.io.incremental_stats();
    Stats { state, elapsed_ms: started.elapsed().as_millis() as u64, rows, bytes_read: io.read_bytes }.to_json()
}

fn log_failure(request_id: &str, id: &str, sql: &str, e: &Error) {
    match e {
        Error::Internal(err) => tracing::error!(request_id, query_id = id, sql, error = ?err, "trino query failed"),
        Error::Api(api) => tracing::warn!(request_id, query_id = id, sql, code = api.code, message = %api.message, "trino query rejected"),
    }
}
