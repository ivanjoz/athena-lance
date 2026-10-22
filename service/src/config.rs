//! config.toml: the single source of environment values (ADR-001).

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub aws: Aws,
    pub storage: Storage,
    pub lambda: LambdaNames,
    pub ingest: Ingest,
    pub ledger: Ledger,
    pub service: ServiceCfg,
    pub compaction: Compaction,
    pub query: QueryCfg,
    pub trino: TrinoCfg,
    #[serde(default)]
    pub api: ApiCfg,
}

/// The read-only JSON API on the ingest Function URL (`POST /v1/lance`), for clients that
/// can't invoke the functions directly (another AWS account, a developer machine).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ApiCfg {
    /// `Authorization: Bearer <read_key>`. Empty (or no [api] section) rejects every request.
    #[serde(default)]
    pub read_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Aws {
    pub profile: String,
    pub region: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Storage {
    pub bucket: String,
    pub tables_prefix: String,
    pub staging_prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LambdaNames {
    pub control_function: String,
    pub ingest_function: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ingest {
    pub max_rows_per_request: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ledger {
    pub dynamodb_table: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceCfg {
    pub locks_prefix: String,
    pub lock_lease_seconds: u64,
    pub lock_wait_seconds: u64,
    pub version_retention_hours: u64,
    pub batch_ttl_days: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Compaction {
    pub max_file_mb: u64,
    pub max_files: usize,
    pub max_tasks: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryCfg {
    pub default_max_rows: usize,
    pub max_rows_limit: usize,
    pub default_timeout_ms: u64,
    pub max_timeout_ms: u64,
    pub max_response_bytes: usize,
    pub freshness_seconds: u64,
    pub memory_fraction: f64,
    /// Query result cache: s3://<bucket>/<cache_prefix>/query/<sha256>.json.
    pub cache_prefix: String,
    /// Max age of a cached result when the request sets cache_seconds = 0.
    pub cache_default_seconds: u64,
    /// Cap on the max age a request may ask for.
    pub cache_max_seconds: u64,
}

/// The Trino-compatible HTTP API on the ingest Function URL (trino-api-plan.md).
#[derive(Debug, Clone, Deserialize)]
pub struct TrinoCfg {
    /// The single read-only credential: Basic auth user and its key. An empty key rejects every
    /// request.
    pub user: String,
    pub password: String,
    pub catalog: String,
    pub schema: String,
    pub server_version: String,
    pub max_rows: usize,
    /// Uncompressed JSON per response; beyond it the result is cut with a warning.
    pub max_result_mb: usize,
    /// Whole response (execution + sending). The first row must still be ready within
    /// query.max_timeout_ms.
    pub max_stream_seconds: u64,
}

impl Config {
    /// `$LANCE_CONFIG`, or the first config.toml found walking up from the cwd. In Lambda
    /// the file is bundled in the zip and LANCE_CONFIG points at it.
    pub fn load() -> Result<Self> {
        let path = match std::env::var_os("LANCE_CONFIG") {
            Some(p) => PathBuf::from(p),
            None => {
                let cwd = std::env::current_dir()?;
                cwd.ancestors()
                    .map(|d| d.join("config.toml"))
                    .find(|p| p.is_file())
                    .ok_or_else(|| {
                        anyhow!(
                            "config.toml not found above {} (copy config.example.toml to config.toml, or set LANCE_CONFIG)",
                            cwd.display()
                        )
                    })?
            }
        };
        let text = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }

    /// Exports the AWS profile and region for the default credential chain (Lance and the
    /// AWS SDK both read them). In Lambda the profile is ignored: the execution role is used.
    /// Must run before any other thread starts.
    pub fn export_aws_env(&self) {
        let in_lambda = std::env::var_os("AWS_LAMBDA_FUNCTION_NAME").is_some();
        // SAFETY: called at the top of main, while the process is single-threaded.
        unsafe {
            if !in_lambda && !self.aws.profile.is_empty() {
                std::env::set_var("AWS_PROFILE", &self.aws.profile);
            }
            if std::env::var_os("AWS_REGION").is_none() {
                std::env::set_var("AWS_REGION", &self.aws.region);
            }
        }
    }

    pub fn table_uri(&self, table: &str) -> String {
        format!("s3://{}/{}/{}.lance", self.storage.bucket, self.storage.tables_prefix, table)
    }

    pub fn tables_uri(&self) -> String {
        format!("s3://{}/{}", self.storage.bucket, self.storage.tables_prefix)
    }
}
