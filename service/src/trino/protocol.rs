//! Pieces of Trino's `QueryResults` document (trino-api-plan.md §2.2, §8): query id, stats,
//! errors and warnings. The document itself is written incrementally by `statement`.

use std::sync::atomic::{AtomicU32, Ordering};

use serde_json::{Value, json};

use crate::error::{Error, code};

static QUERY_SEQ: AtomicU32 = AtomicU32::new(0);

/// Trino's query id format, `YYYYMMDD_HHMMSS_NNNNN_xxxxx`, with the suffix taken from the
/// Lambda request id so a query can be found in the logs.
pub fn query_id(request_id: &str) -> String {
    let seq = QUERY_SEQ.fetch_add(1, Ordering::Relaxed) % 100_000;
    let suffix: String = request_id.chars().filter(char::is_ascii_alphanumeric).take(5).collect::<String>().to_ascii_lowercase();
    format!("{}_{seq:05}_{suffix:0<5}", chrono::Utc::now().format("%Y%m%d_%H%M%S"))
}

pub struct Stats {
    pub state: &'static str,
    pub elapsed_ms: u64,
    pub rows: u64,
    pub bytes_read: u64,
}

impl Stats {
    pub fn to_json(&self) -> Value {
        json!({
            "state": self.state, "queued": false, "scheduled": true, "nodes": 1,
            "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1,
            "cpuTimeMillis": 0, "wallTimeMillis": self.elapsed_ms, "queuedTimeMillis": 0,
            "elapsedTimeMillis": self.elapsed_ms, "processedRows": self.rows,
            "processedBytes": self.bytes_read, "physicalInputBytes": self.bytes_read,
            "peakMemoryBytes": 0, "spilledBytes": 0,
        })
    }
}

/// `(errorName, errorType, errorCode)` from io.trino.spi.StandardErrorCode.
fn trino_code(service_code: &str) -> (&'static str, &'static str, i64) {
    match service_code {
        code::TABLE_NOT_FOUND => ("TABLE_NOT_FOUND", "USER_ERROR", 46),
        code::SQL_ERROR | code::BAD_REQUEST => ("GENERIC_USER_ERROR", "USER_ERROR", 0),
        NOT_SUPPORTED => ("NOT_SUPPORTED", "USER_ERROR", 13),
        code::QUERY_TIMEOUT => ("EXCEEDED_TIME_LIMIT", "INSUFFICIENT_RESOURCES", 131075),
        code::RESOURCES_EXHAUSTED => ("EXCEEDED_LOCAL_MEMORY_LIMIT", "INSUFFICIENT_RESOURCES", 131079),
        _ => ("GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR", 65536),
    }
}

pub const NOT_SUPPORTED: &str = "NOT_SUPPORTED";

pub fn error_json(e: &Error) -> Value {
    let api = e.to_api();
    let (name, kind, number) = trino_code(api.code);
    json!({
        "message": api.message,
        "errorCode": number,
        "errorName": name,
        "errorType": kind,
        "failureInfo": { "type": "io.trino.spi.TrinoException", "message": api.message, "suppressed": [], "stack": [] },
    })
}

pub fn truncated_warning(rows: u64, reason: &str) -> Value {
    json!({
        "warningCode": { "code": 1, "name": "RESULT_TRUNCATED" },
        "message": format!("result cut at {rows} rows ({reason}); add filters or a LIMIT to get all rows"),
    })
}
