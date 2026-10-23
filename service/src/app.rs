//! Process-wide state, built once per Lambda container (init phase) and reused by every
//! warm invocation: config, ledger client, bucket store, the Lance session (caches) and the
//! query engine (dataset cache, DataFusion runtime).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::session::Session;
use lance_io::object_store::ObjectStoreParams;
use object_store::{ObjectStore, ObjectStoreExt};
use object_store::path::Path;

use crate::config::Config;
use crate::error::{Error, Result, code};
use crate::ledger::Ledger;
use crate::lock::{LockSettings, TableLock};
use crate::ops::query::QueryEngine;
use crate::spec::{SPEC_CONFIG_KEY, TableSpec, validate_table_name};

pub struct App {
    pub cfg: Config,
    pub ledger: Ledger,
    /// Object store rooted at the data bucket (paths are bucket-relative).
    pub store: Arc<dyn ObjectStore>,
    pub session: Arc<Session>,
    pub query: QueryEngine,
}

impl App {
    pub async fn new(cfg: Config) -> anyhow::Result<App> {
        let ledger = Ledger::new(&cfg.ledger.dynamodb_table, cfg.service.batch_ttl_days).await;
        let (store, _) = lance_io::object_store::ObjectStore::from_uri(&format!("s3://{}", cfg.storage.bucket))
            .await
            .context("opening the data bucket")?;
        let query = QueryEngine::new(&cfg.query)?;
        Ok(App { cfg, ledger, store: store.inner.clone(), session: Arc::new(Session::default()), query })
    }

    pub async fn open(&self, table: &str) -> Result<Dataset> {
        self.open_with(table, None).await
    }

    /// Opens a table with custom store params (the query engine adds its I/O counter).
    pub async fn open_with(&self, table: &str, store_params: Option<ObjectStoreParams>) -> Result<Dataset> {
        validate_table_name(table)?;
        let uri = self.cfg.table_uri(table);
        let mut builder = DatasetBuilder::from_uri(&uri).with_session(self.session.clone());
        if let Some(params) = store_params {
            builder = builder.with_store_params(params);
        }
        match builder.load().await {
            Ok(ds) => Ok(ds),
            Err(lance::Error::DatasetNotFound { .. }) => {
                Err(Error::api(code::TABLE_NOT_FOUND, format!("table {table:?} does not exist")))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn lock_path(&self, table: &str) -> Path {
        Path::from(format!("{}/{table}.json", self.cfg.service.locks_prefix))
    }

    pub async fn lock(&self, table: &str, owner: &str, op: &str) -> Result<TableLock> {
        self.lock_waiting(table, owner, op, self.cfg.service.lock_wait_seconds).await
    }

    /// The table lock for create_table and drop_table (control function), which waits less
    /// (`control_lock_wait_seconds`) so it gives up before that function's timeout.
    pub async fn lock_control(&self, table: &str, op: &str) -> Result<TableLock> {
        self.lock_waiting(table, op, op, self.cfg.service.control_lock_wait_seconds).await
    }

    async fn lock_waiting(&self, table: &str, owner: &str, op: &str, wait_seconds: u64) -> Result<TableLock> {
        let settings = LockSettings {
            lease: Duration::from_secs(self.cfg.service.lock_lease_seconds),
            wait: Duration::from_secs(wait_seconds),
        };
        TableLock::acquire(self.store.clone(), self.lock_path(table), owner, op, &settings).await
    }

    /// Reads a staged file, only from this bucket's staging prefix.
    pub async fn read_staged(&self, uri: &str) -> Result<bytes::Bytes> {
        let prefix = format!("s3://{}/{}/", self.cfg.storage.bucket, self.cfg.storage.staging_prefix);
        let Some(key) = uri.strip_prefix(&format!("s3://{}/", self.cfg.storage.bucket)).filter(|_| uri.starts_with(&prefix))
        else {
            return Err(Error::api(code::BAD_REQUEST, format!("source {uri:?} must be under {prefix}")));
        };
        match self.store.get(&Path::from(key)).await {
            Ok(r) => Ok(r.bytes().await?),
            Err(object_store::Error::NotFound { .. }) => {
                Err(Error::api(code::BAD_REQUEST, format!("source {uri:?} does not exist")))
            }
            Err(e) => Err(e.into()),
        }
    }
}

pub fn load_spec(ds: &Dataset) -> Result<TableSpec> {
    let raw = ds
        .config()
        .get(SPEC_CONFIG_KEY)
        .ok_or_else(|| Error::Internal(anyhow::anyhow!("table at {} has no {SPEC_CONFIG_KEY}", ds.uri())))?;
    Ok(serde_json::from_str(raw).map_err(anyhow::Error::from)?)
}

/// The table schema as Arrow (what incoming files must match).
pub fn arrow_schema(ds: &Dataset) -> arrow_schema::Schema {
    arrow_schema::Schema::from(ds.schema())
}
