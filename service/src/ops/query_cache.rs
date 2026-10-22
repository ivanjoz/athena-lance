//! Query result cache in the data bucket: one object per query, at
//! `<query.cache_prefix>/query/<sha256>.json`.
//!
//! The key is the SHA-256 of what decides the result besides the data: the SQL and the row and
//! byte caps. An entry answers a request while it is younger than the request's cache_seconds and,
//! when the request names min_versions, only if it was computed at those versions or later.
//! Freshness is by age only: a write does not invalidate entries, so a reader that must see its own
//! write passes min_versions (or cache_seconds = -1).
//!
//! The cache never fails a query: a read or write error is logged and the query runs as if there
//! were no entry. Old objects are removed by the bucket's lifecycle rule (deploy/setup.sh).

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::path::Path;
use object_store::{ObjectStoreExt, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::query::QueryResult;
use crate::app::App;
use crate::config::QueryCfg;

/// Bumped when the entry format or what the key covers changes: old entries are just missed.
const KEY_VERSION: &str = "v1";

#[derive(Deserialize)]
struct Entry {
    created_ms: u64,
    sql: String,
    result: QueryResult,
}

#[derive(Serialize)]
struct EntryRef<'a> {
    created_ms: u64,
    sql: &'a str,
    result: &'a QueryResult,
}

/// Where a query's result is cached and how old an entry the request accepts.
pub(crate) struct Lookup {
    path: Path,
    max_age: Duration,
}

impl Lookup {
    /// None when the request (or the config) turns the cache off.
    pub(crate) fn new(cfg: &QueryCfg, sql: &str, max_rows: usize, cache_seconds: i64) -> Option<Lookup> {
        let seconds = max_age_seconds(cfg, cache_seconds)?;
        Some(Lookup { path: path(cfg, sql, max_rows), max_age: Duration::from_secs(seconds) })
    }

    /// The cached result, if there is one young enough and at the versions the request needs.
    pub(crate) async fn get(&self, app: &App, sql: &str, min_versions: &BTreeMap<String, u64>) -> Option<QueryResult> {
        let bytes = match app.store.get(&self.path).await {
            Ok(r) => match r.bytes().await {
                Ok(b) => b,
                Err(e) => return miss(&self.path, e),
            },
            Err(object_store::Error::NotFound { .. }) => return None,
            Err(e) => return miss(&self.path, e),
        };
        let entry: Entry = match serde_json::from_slice(&bytes) {
            Ok(e) => e,
            Err(e) => return miss(&self.path, e),
        };
        let age_ms = now_ms().saturating_sub(entry.created_ms);
        // The SQL is stored to rule out a hash collision, however unlikely.
        if entry.sql != sql || age_ms >= self.max_age.as_millis() as u64 {
            return None;
        }
        let versions = &entry.result.stats.dataset_versions;
        // A table the SQL does not name is not in dataset_versions, and its minimum does not apply.
        if !min_versions.iter().all(|(t, min)| versions.get(t).is_none_or(|v| v >= min)) {
            return None;
        }
        let mut result = entry.result;
        result.stats.cache_hit = true;
        result.stats.cache_age_ms = age_ms;
        // Nothing was planned or executed for this request.
        result.stats.plan_ms = 0.0;
        result.stats.exec_ms = 0.0;
        Some(result)
    }

    pub(crate) async fn put(&self, app: &App, sql: &str, result: &QueryResult) {
        let entry = EntryRef { created_ms: now_ms(), sql, result };
        let body = match serde_json::to_vec(&entry) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, path = %self.path, "query cache: serializing the result failed");
                return;
            }
        };
        if let Err(e) = app.store.put_opts(&self.path, PutPayload::from(body), PutOptions::default()).await {
            tracing::warn!(error = %e, path = %self.path, "query cache: writing the result failed");
        }
    }
}

/// 0 = the configured default, negative = off, positive = capped at the configured max.
fn max_age_seconds(cfg: &QueryCfg, cache_seconds: i64) -> Option<u64> {
    let seconds = match cache_seconds {
        n if n < 0 => return None,
        0 => cfg.cache_default_seconds,
        n => (n as u64).min(cfg.cache_max_seconds),
    };
    (seconds > 0).then_some(seconds)
}

fn path(cfg: &QueryCfg, sql: &str, max_rows: usize) -> Path {
    let key = format!("{KEY_VERSION}\n{max_rows}\n{}\n{sql}", cfg.max_response_bytes);
    let hash: String = Sha256::digest(key.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
    Path::from(format!("{}/query/{hash}.json", cfg.cache_prefix))
}

fn miss(path: &Path, e: impl std::fmt::Display) -> Option<QueryResult> {
    tracing::warn!(error = %e, path = %path, "query cache: reading the entry failed; running the query");
    None
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> QueryCfg {
        QueryCfg {
            default_max_rows: 10_000,
            max_rows_limit: 100_000,
            default_timeout_ms: 30_000,
            max_timeout_ms: 30_000,
            max_response_bytes: 5_000_000,
            freshness_seconds: 5,
            memory_fraction: 0.6,
            cache_prefix: "cache".into(),
            cache_default_seconds: 300,
            cache_max_seconds: 86_400,
        }
    }

    #[test]
    fn max_age_from_the_request() {
        let c = cfg();
        assert_eq!(max_age_seconds(&c, 0), Some(300));
        assert_eq!(max_age_seconds(&c, -1), None);
        assert_eq!(max_age_seconds(&c, -50), None);
        assert_eq!(max_age_seconds(&c, 60), Some(60));
        assert_eq!(max_age_seconds(&c, 1_000_000), Some(86_400));
        // A default of 0 turns the cache off unless the request asks for it.
        let off = QueryCfg { cache_default_seconds: 0, ..cfg() };
        assert_eq!(max_age_seconds(&off, 0), None);
        assert_eq!(max_age_seconds(&off, 10), Some(10));
    }

    #[test]
    fn key_covers_sql_and_caps() {
        let c = cfg();
        let p = path(&c, "SELECT 1", 100);
        assert!(p.as_ref().starts_with("cache/query/") && p.as_ref().ends_with(".json"), "{p}");
        assert_eq!(p.as_ref().len(), "cache/query/".len() + 64 + ".json".len());
        assert_eq!(p, path(&c, "SELECT 1", 100));
        assert_ne!(p, path(&c, "SELECT 2", 100));
        assert_ne!(p, path(&c, "SELECT 1", 101));
        assert_ne!(p, path(&QueryCfg { max_response_bytes: 1, ..cfg() }, "SELECT 1", 100));
    }
}
