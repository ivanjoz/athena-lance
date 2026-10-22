//! Trino-compatible HTTP API, served by the ingest function through its Function URL
//! (trino-api-plan.md). Only read-only code is reachable from here: requests never become a
//! `protocol::Request`, so writes, compaction and control ops can't be called over HTTP.

pub mod auth;
pub mod catalog;
pub mod encode;
pub mod http;
pub mod protocol;
pub mod session;
pub mod statement;

use std::sync::Arc;

use lambda_runtime::StreamResponse;
use serde_json::{Value, json};

use crate::app::App;
use self::http::{BodyStream, HttpRequest, json_response};

/// A Function URL event (payload format 2.0), as opposed to a direct `Invoke` request.
pub fn is_http_event(payload: &Value) -> bool {
    payload.get("requestContext").and_then(|r| r.get("http")).is_some()
}

pub async fn handle(app: Arc<App>, request_id: &str, payload: Value) -> StreamResponse<BodyStream> {
    let req = match HttpRequest::parse(&payload) {
        Ok(req) => req,
        Err(e) => return json_response(400, &[], &json!({ "error": e })),
    };
    // Every Trino header, to learn what each client sends; credentials are never logged.
    let trino_headers: Vec<String> = req
        .headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-trino-") && *k != "x-trino-extra-credential")
        .map(|(k, v)| format!("{k}: {v}"))
        .collect();
    tracing::info!(
        request_id,
        method = req.method,
        path = req.path,
        source_ip = req.source_ip,
        user_agent = req.header("user-agent").unwrap_or_default(),
        accept_encoding = req.header("accept-encoding").unwrap_or_default(),
        trino_headers = ?trino_headers,
        sql = req.body,
        "trino request"
    );

    let cfg = &app.cfg.trino;
    if req.method == "GET" && req.path == "/v1/info" {
        let info = json!({
            "nodeVersion": { "version": cfg.server_version },
            "environment": "lance", "coordinator": true, "starting": false, "uptime": "1.00d",
        });
        return json_response(200, &[], &info);
    }
    if let Err(reason) = auth::check(cfg, &req) {
        tracing::warn!(request_id, source_ip = req.source_ip, reason, "trino authentication failed");
        let challenge = [("www-authenticate", "Basic realm=\"lance\"".to_string())];
        return json_response(401, &challenge, &json!({ "error": "unauthorized" }));
    }
    match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/v1/statement") => statement::run(app.clone(), request_id, &req),
        ("DELETE", p) if p.starts_with("/v1/statement/") => json_response(204, &[], &Value::Null),
        ("GET", p) if p.starts_with("/v1/statement/") => {
            json_response(410, &[], &json!({ "error": "results are returned in a single response" }))
        }
        _ => json_response(404, &[], &json!({ "error": "not found" })),
    }
}
