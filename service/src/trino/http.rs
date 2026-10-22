//! Function URL plumbing: the request event (payload format 2.0) and the streamed response
//! (invoke mode RESPONSE_STREAM). The body is produced by a task and sent through a bounded
//! channel, so a slow client slows the query down instead of buffering the result in memory.

use std::collections::BTreeMap;
use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use futures::SinkExt;
use futures::channel::mpsc;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use lambda_runtime::{MetadataPrelude, StreamResponse};
use serde_json::Value;

/// The streamed body handed to the Lambda runtime.
pub type BodyStream = mpsc::Receiver<Result<Bytes, std::io::Error>>;

/// Chunks in flight between the producer and the runtime; each is about one record batch.
const CHANNEL_CHUNKS: usize = 4;

pub struct HttpRequest {
    pub method: String,
    pub path: String,
    /// Lower-case names; repeated headers arrive joined by commas.
    pub headers: BTreeMap<String, String>,
    pub body: String,
    pub source_ip: String,
}

impl HttpRequest {
    pub fn parse(payload: &Value) -> Result<HttpRequest, String> {
        let http = &payload["requestContext"]["http"];
        let text = |v: &Value| v.as_str().unwrap_or_default().to_string();
        let headers = payload["headers"]
            .as_object()
            .map(|h| h.iter().map(|(k, v)| (k.to_ascii_lowercase(), text(v))).collect())
            .unwrap_or_default();
        let raw_body = text(&payload["body"]);
        let body = if payload["isBase64Encoded"].as_bool().unwrap_or(false) {
            let bytes = BASE64.decode(raw_body.as_bytes()).map_err(|e| format!("body is not valid base64: {e}"))?;
            String::from_utf8(bytes).map_err(|_| "body is not UTF-8".to_string())?
        } else {
            raw_body
        };
        Ok(HttpRequest {
            method: text(&http["method"]).to_ascii_uppercase(),
            path: text(&payload["rawPath"]),
            headers,
            body,
            source_ip: text(&http["sourceIp"]),
        })
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    pub fn accepts_gzip(&self) -> bool {
        self.header("accept-encoding").is_some_and(|v| v.split(',').any(|e| e.trim().starts_with("gzip")))
    }

    /// `https://<host>` as the client reached us (for infoUri).
    pub fn base_url(&self) -> String {
        format!("https://{}", self.header("host").unwrap_or("localhost"))
    }
}

/// Writes the response body: JSON text goes into `buf`, `flush` sends it (gzipped when the
/// client asked for it) as one chunk.
pub struct Sink {
    tx: mpsc::Sender<Result<Bytes, std::io::Error>>,
    gzip: Option<GzEncoder<Vec<u8>>>,
    pub buf: Vec<u8>,
    /// Uncompressed bytes written so far (the size cap counts these).
    pub written: usize,
}

impl Sink {
    pub async fn flush(&mut self) -> bool {
        if self.buf.is_empty() {
            return true;
        }
        self.written += self.buf.len();
        let chunk = match &mut self.gzip {
            Some(gz) => {
                // A sync flush per chunk lets the client decode rows as they arrive.
                if gz.write_all(&self.buf).and_then(|_| gz.flush()).is_err() {
                    return false;
                }
                std::mem::take(gz.get_mut())
            }
            None => std::mem::take(&mut self.buf),
        };
        self.buf.clear();
        chunk.is_empty() || self.tx.send(Ok(Bytes::from(chunk))).await.is_ok()
    }

    pub async fn finish(mut self) {
        if !self.flush().await {
            return;
        }
        if let Some(gz) = self.gzip.take()
            && let Ok(tail) = gz.finish()
            && !tail.is_empty()
        {
            let _ = self.tx.send(Ok(Bytes::from(tail))).await;
        }
    }
}

/// A response whose body is written later through the returned `Sink` (from a spawned task:
/// the runtime only starts reading the body once the handler has returned).
pub fn respond(status: u16, headers: &[(&str, String)], gzip: bool) -> (StreamResponse<BodyStream>, Sink) {
    let (tx, rx) = mpsc::channel(CHANNEL_CHUNKS);
    let mut map = HeaderMap::new();
    map.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if gzip {
        map.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    for (name, value) in headers {
        if let (Ok(n), Ok(v)) = (HeaderName::try_from(*name), HeaderValue::try_from(value.as_str())) {
            map.insert(n, v);
        }
    }
    let prelude = MetadataPrelude {
        status_code: StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        headers: map,
        cookies: Vec::new(),
    };
    let sink = Sink {
        tx,
        gzip: gzip.then(|| GzEncoder::new(Vec::new(), Compression::fast())),
        buf: Vec::with_capacity(64 * 1024),
        written: 0,
    };
    (StreamResponse { metadata_prelude: prelude, stream: rx }, sink)
}

/// A complete small response (errors, /v1/info). `Value::Null` sends an empty body.
pub fn json_response(status: u16, headers: &[(&str, String)], body: &Value) -> StreamResponse<BodyStream> {
    let (response, mut sink) = respond(status, headers, false);
    if !body.is_null() {
        sink.buf = serde_json::to_vec(body).unwrap_or_default();
    }
    tokio::spawn(sink.finish());
    response
}
