//! One writer per table: a lease lock object per table in the data bucket, built on S3
//! Express conditional writes (If-None-Match create, If-Match overwrite; verified 2026-10-21).
//!
//! The holder renews the lease while it works. A crashed holder's lease expires and the next
//! writer takes over with an If-Match overwrite, so only one of several contenders wins.
//! Lance's own conditional commit still protects the data if a lease is ever lost; the lock
//! only avoids wasted work and keeps per-table operations in order.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::error::{Error, Result, code};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockBody {
    pub owner: String,
    pub op: String,
    /// Unix milliseconds; 0 = released.
    pub expires_at: i64,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub struct TableLock {
    store: Arc<dyn ObjectStore>,
    path: Path,
    body: LockBody,
    etag: Arc<Mutex<Option<String>>>,
    lost: Arc<AtomicBool>,
    renew: JoinHandle<()>,
}

pub struct LockSettings {
    pub lease: Duration,
    pub wait: Duration,
}

fn payload(body: &LockBody) -> PutPayload {
    PutPayload::from(serde_json::to_vec(body).expect("lock body serializes"))
}

/// Reads the current lock object, if any.
pub async fn read(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<Option<(LockBody, Option<String>)>> {
    match store.get(path).await {
        Ok(r) => {
            let etag = r.meta.e_tag.clone();
            let bytes = r.bytes().await?;
            let body = serde_json::from_slice(&bytes).unwrap_or(LockBody { owner: "?".into(), op: "?".into(), expires_at: 0 });
            Ok(Some((body, etag)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl TableLock {
    pub async fn acquire(
        store: Arc<dyn ObjectStore>,
        path: Path,
        owner: &str,
        op: &str,
        settings: &LockSettings,
    ) -> Result<TableLock> {
        let lease_ms = settings.lease.as_millis() as i64;
        let deadline = tokio::time::Instant::now() + settings.wait;
        let mut delay = Duration::from_millis(200);
        loop {
            let body = LockBody { owner: owner.into(), op: op.into(), expires_at: now_ms() + lease_ms };
            let created = store
                .put_opts(&path, payload(&body), PutOptions { mode: PutMode::Create, ..Default::default() })
                .await;
            let mut holder = None;
            match created {
                Ok(res) => return Ok(Self::start(store, path, body, res.e_tag, settings.lease)),
                Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => {
                    if let Some((current, etag)) = read(&store, &path).await? {
                        if current.expires_at < now_ms() {
                            // Expired or released: take over, but only if nobody changed it meanwhile.
                            let mode = PutMode::Update(UpdateVersion { e_tag: etag, version: None });
                            match store.put_opts(&path, payload(&body), PutOptions { mode, ..Default::default() }).await {
                                Ok(res) => return Ok(Self::start(store, path, body, res.e_tag, settings.lease)),
                                Err(object_store::Error::Precondition { .. }) => {}
                                Err(e) => return Err(e.into()),
                            }
                        } else {
                            holder = Some(current);
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
            if tokio::time::Instant::now() + delay > deadline {
                let details = serde_json::to_value(&holder).unwrap_or_default();
                return Err(Error::with_details(
                    code::TABLE_BUSY,
                    format!("another operation holds the table lock (waited {}s)", settings.wait.as_secs()),
                    details,
                ));
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(2));
        }
    }

    fn start(store: Arc<dyn ObjectStore>, path: Path, body: LockBody, etag: Option<String>, lease: Duration) -> TableLock {
        let etag = Arc::new(Mutex::new(etag));
        let lost = Arc::new(AtomicBool::new(false));
        let renew = {
            let (store, path, mut body, etag, lost) = (store.clone(), path.clone(), body.clone(), etag.clone(), lost.clone());
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(lease / 3).await;
                    body.expires_at = now_ms() + lease.as_millis() as i64;
                    let mut guard = etag.lock().await;
                    let mode = PutMode::Update(UpdateVersion { e_tag: guard.clone(), version: None });
                    match store.put_opts(&path, payload(&body), PutOptions { mode, ..Default::default() }).await {
                        Ok(res) => *guard = res.e_tag,
                        Err(e) => {
                            tracing::error!(error = %e, "table lock renewal failed; lock lost");
                            lost.store(true, Ordering::SeqCst);
                            return;
                        }
                    }
                }
            })
        };
        TableLock { store, path, body, etag, lost, renew }
    }

    /// Fails if the lease was lost. Call right before committing.
    pub fn check(&self) -> Result<()> {
        if self.lost.load(Ordering::SeqCst) {
            return Err(Error::api(code::LOCK_LOST, "the table lock was lost during the operation"));
        }
        Ok(())
    }

    /// Marks the lock released (expires_at = 0) if we still hold it.
    pub async fn release(self) {
        self.renew.abort();
        let guard = self.etag.lock().await;
        let body = LockBody { expires_at: 0, ..self.body.clone() };
        let mode = PutMode::Update(UpdateVersion { e_tag: guard.clone(), version: None });
        if let Err(e) = self.store.put_opts(&self.path, payload(&body), PutOptions { mode, ..Default::default() }).await {
            tracing::warn!(error = %e, "releasing table lock failed; it will expire");
        }
    }
}
