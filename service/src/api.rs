//! HTTP entry point of the ingest Function URL, and the read-only JSON API it serves next to the
//! Trino API.
//!
//! `POST /v1/lance` takes the same body as a direct Invoke (`{"op": ...}`) and answers with the
//! same envelope, for clients that can't invoke the functions with IAM: another AWS account (the
//! dev backend) or a developer machine. It authenticates with `Authorization: Bearer <[api]
//! read_key>` and only accepts read ops (`Request::is_read_only`): writes, compaction and table
//! creation still need the direct Invoke. Every other path goes to the Trino API.

use std::sync::Arc;

use lambda_runtime::StreamResponse;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::app::App;
use crate::config::ApiCfg;
use crate::error::code;
use crate::handler;
use crate::trino;
use crate::trino::auth::constant_time_eq;
use crate::trino::http::{BodyStream, HttpRequest, json_response, respond};

pub const PATH: &str = "/v1/lance";

/// A Function URL event: `/v1/lance` here, anything else to the Trino API.
pub async fn handle_http(app: Arc<App>, request_id: &str, payload: Value) -> StreamResponse<BodyStream> {
    if payload["rawPath"].as_str() == Some(PATH) {
        handle(app, request_id, payload).await
    } else {
        trino::handle(app, request_id, payload).await
    }
}

async fn handle(app: Arc<App>, request_id: &str, payload: Value) -> StreamResponse<BodyStream> {
    let req = match HttpRequest::parse(&payload) {
        Ok(req) => req,
        Err(e) => return error(request_id, 400, code::BAD_REQUEST, &e),
    };
    if req.method != "POST" {
        return error(request_id, 405, code::BAD_REQUEST, "use POST");
    }
    if let Err(reason) = check_key(&app.cfg.api, &req) {
        tracing::warn!(request_id, source_ip = req.source_ip, reason, "api authentication failed");
        return error(request_id, 401, code::UNAUTHORIZED, "missing or wrong API key");
    }
    let body: Value = match serde_json::from_str(&req.body) {
        Ok(v) => v,
        Err(e) => return error(request_id, 400, code::BAD_REQUEST, &format!("body is not JSON: {e}")),
    };
    tracing::info!(request_id, source_ip = req.source_ip, "api request");
    let envelope = handler::handle_read_only(&app, request_id, body).await;

    // A result can be a few MB of JSON: gzip it when the client accepts it (Go's client does).
    let (response, mut sink) = respond(200, &[], req.accepts_gzip());
    sink.buf = serde_json::to_vec(&envelope).unwrap_or_default();
    tokio::spawn(sink.finish());
    response
}

/// Both keys are hashed before comparing, so the constant-time comparison runs over equal
/// lengths and doesn't reveal the key's length.
fn check_key(cfg: &ApiCfg, req: &HttpRequest) -> Result<(), &'static str> {
    if cfg.read_key.is_empty() {
        return Err("no API key configured");
    }
    let given = req
        .header("authorization")
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .ok_or("missing Bearer authorization")?;
    let (given, expected) = (Sha256::digest(given.trim().as_bytes()), Sha256::digest(cfg.read_key.as_bytes()));
    if !constant_time_eq(&given, &expected) {
        return Err("wrong key");
    }
    Ok(())
}

/// An error in the envelope shape, so the client decodes it like any service error.
fn error(request_id: &str, status: u16, code: &str, message: &str) -> StreamResponse<BodyStream> {
    let body = json!({ "request_id": request_id, "ok": false, "error": { "code": code, "message": message } });
    json_response(status, &[], &body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn req(authorization: Option<&str>) -> HttpRequest {
        let mut headers = BTreeMap::new();
        if let Some(a) = authorization {
            headers.insert("authorization".into(), a.into());
        }
        HttpRequest { method: "POST".into(), path: PATH.into(), headers, body: String::new(), source_ip: String::new() }
    }

    #[test]
    fn accepts_only_the_configured_key() {
        let cfg = ApiCfg { read_key: "secret".into() };
        assert_eq!(check_key(&cfg, &req(Some("Bearer secret"))), Ok(()));
        assert_eq!(check_key(&cfg, &req(Some("bearer secret "))), Ok(()));
        assert!(check_key(&cfg, &req(Some("Bearer wrong"))).is_err());
        assert!(check_key(&cfg, &req(Some("secret"))).is_err());
        assert!(check_key(&cfg, &req(Some("Basic c2VjcmV0"))).is_err());
        assert!(check_key(&cfg, &req(None)).is_err());
        assert!(check_key(&ApiCfg::default(), &req(Some("Bearer "))).is_err());
    }
}
