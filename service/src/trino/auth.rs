//! Basic auth against the single read-only credential in `[trino]`: the user name and the
//! SHA-256 of a random key. The key is random, so a plain hash is enough (nothing to guess by
//! dictionary); the comparison is constant-time.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use sha2::{Digest, Sha256};

use super::http::HttpRequest;
use crate::config::TrinoCfg;

/// The authenticated user, or why the request was rejected (logged, never sent back).
pub fn check(cfg: &TrinoCfg, req: &HttpRequest) -> Result<String, &'static str> {
    if cfg.password_sha256.is_empty() {
        return Err("no credential configured");
    }
    let encoded = req
        .header("authorization")
        .and_then(|v| v.strip_prefix("Basic ").or_else(|| v.strip_prefix("basic ")))
        .ok_or("missing Basic authorization")?;
    let decoded = BASE64.decode(encoded.trim()).map_err(|_| "malformed authorization")?;
    let decoded = String::from_utf8(decoded).map_err(|_| "malformed authorization")?;
    let (user, password) = decoded.split_once(':').ok_or("malformed authorization")?;
    let hash = hex(&Sha256::digest(password.as_bytes()));
    if user != cfg.user || !constant_time_eq(hash.as_bytes(), cfg.password_sha256.to_ascii_lowercase().as_bytes()) {
        return Err("wrong user or key");
    }
    if req.header("x-trino-user").is_some_and(|u| u != user) {
        return Err("X-Trino-User differs from the authenticated user");
    }
    Ok(user.to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn cfg() -> TrinoCfg {
        TrinoCfg {
            user: "tableau".into(),
            // sha256("secret")
            password_sha256: "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b".into(),
            catalog: "lance".into(),
            schema: "default".into(),
            server_version: "476".into(),
            max_rows: 10,
            max_result_mb: 1,
            max_stream_seconds: 10,
        }
    }

    fn req(user_pass: Option<&str>, trino_user: Option<&str>) -> HttpRequest {
        let mut headers = BTreeMap::new();
        if let Some(up) = user_pass {
            headers.insert("authorization".into(), format!("Basic {}", BASE64.encode(up)));
        }
        if let Some(u) = trino_user {
            headers.insert("x-trino-user".into(), u.into());
        }
        HttpRequest { method: "POST".into(), path: "/v1/statement".into(), headers, body: String::new(), source_ip: String::new() }
    }

    #[test]
    fn accepts_only_the_configured_credential() {
        assert_eq!(check(&cfg(), &req(Some("tableau:secret"), Some("tableau"))), Ok("tableau".into()));
        assert!(check(&cfg(), &req(Some("tableau:wrong"), None)).is_err());
        assert!(check(&cfg(), &req(Some("other:secret"), None)).is_err());
        assert!(check(&cfg(), &req(Some("tableau:secret"), Some("other"))).is_err());
        assert!(check(&cfg(), &req(None, None)).is_err());
        let mut empty = cfg();
        empty.password_sha256.clear();
        assert!(check(&empty, &req(Some("tableau:secret"), None)).is_err());
    }
}
