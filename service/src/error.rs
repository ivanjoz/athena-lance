//! Service errors. `Api` errors are the caller's fault or a definite outcome (bad schema,
//! missing table, busy table) and carry a stable code; `Internal` errors are unexpected.

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug)]
pub enum Error {
    Api(ApiError),
    Internal(anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn api(code: &'static str, message: impl Into<String>) -> Self {
        Error::Api(ApiError { code, message: message.into(), details: None })
    }

    pub fn with_details(code: &'static str, message: impl Into<String>, details: Value) -> Self {
        Error::Api(ApiError { code, message: message.into(), details: Some(details) })
    }

    pub fn to_api(&self) -> ApiError {
        match self {
            Error::Api(e) => e.clone(),
            Error::Internal(e) => ApiError { code: "INTERNAL", message: format!("{e:#}"), details: None },
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let e = self.to_api();
        write!(f, "{}: {}", e.code, e.message)
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Internal(e)
    }
}

impl From<lance::Error> for Error {
    fn from(e: lance::Error) -> Self {
        Error::Internal(e.into())
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Error::Internal(e.into())
    }
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Error::Internal(e.into())
    }
}

/// Error codes returned to clients.
pub mod code {
    pub const BAD_REQUEST: &str = "BAD_REQUEST";
    pub const INVALID_SPEC: &str = "INVALID_SPEC";
    pub const TABLE_NOT_FOUND: &str = "TABLE_NOT_FOUND";
    pub const TABLE_EXISTS_SPEC_MISMATCH: &str = "TABLE_EXISTS_SPEC_MISMATCH";
    pub const SCHEMA_MISMATCH: &str = "SCHEMA_MISMATCH";
    pub const PARTITION_MISMATCH: &str = "PARTITION_MISMATCH";
    pub const TABLE_BUSY: &str = "TABLE_BUSY";
    pub const BATCH_NOT_FOUND: &str = "BATCH_NOT_FOUND";
    pub const LOCK_LOST: &str = "LOCK_LOST";
    pub const SQL_ERROR: &str = "SQL_ERROR";
    pub const QUERY_TIMEOUT: &str = "QUERY_TIMEOUT";
    pub const RESOURCES_EXHAUSTED: &str = "RESOURCES_EXHAUSTED";
    pub const VERSION_NOT_AVAILABLE: &str = "VERSION_NOT_AVAILABLE";
}
