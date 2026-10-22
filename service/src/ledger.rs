//! DynamoDB ledger: batch status (for idempotency and confirmation) and the partition
//! registry (hash, ID, rows, commit time, version per partition). ADR-007.
//!
//!   pk = <table>, sk = "b#<batch_id>"  -> batch record
//!   pk = <table>, sk = "p#<partition>" -> partition record

use std::collections::HashMap;

use anyhow::{Context, anyhow};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest};
use serde::Serialize;

use crate::error::{ApiError, Result};
use crate::protocol::PartitionWrite;

pub const PENDING: &str = "pending";
pub const COMMITTED: &str = "committed";
pub const FAILED: &str = "failed";

#[derive(Debug, Clone, Serialize, Default)]
pub struct BatchRecord {
    pub batch_id: String,
    pub op: String,
    pub status: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub version: u64,
    /// Version of the data commit itself; `version` may be later (index updates).
    #[serde(skip_serializing_if = "is_zero")]
    pub data_version: u64,
    pub started_at: i64,
    #[serde(skip_serializing_if = "is_zero_i")]
    pub committed_at: i64,
    #[serde(skip_serializing_if = "is_zero")]
    pub rows: u64,
    #[serde(skip_serializing_if = "is_zero")]
    pub partitions: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorRecord {
    pub code: String,
    pub message: String,
}

impl From<&ApiError> for ApiErrorRecord {
    fn from(e: &ApiError) -> Self {
        ApiErrorRecord { code: e.code.to_string(), message: e.message.clone() }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionRecord {
    pub key: String,
    pub values: serde_json::Value,
    #[serde(skip_serializing_if = "is_zero_i")]
    pub id: i64,
    pub hash: String,
    #[serde(skip_serializing_if = "is_zero_i")]
    pub records_updated: i64,
    pub rows: u64,
    pub committed_at: i64,
    pub version: u64,
    pub batch_id: String,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}
fn is_zero_i(v: &i64) -> bool {
    *v == 0
}

#[derive(Clone)]
pub struct Ledger {
    client: Client,
    table: String,
    batch_ttl_secs: i64,
}

fn s(v: impl Into<String>) -> AttributeValue {
    AttributeValue::S(v.into())
}
fn n(v: impl ToString) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn get_s(item: &HashMap<String, AttributeValue>, k: &str) -> String {
    item.get(k).and_then(|v| v.as_s().ok()).cloned().unwrap_or_default()
}
fn get_n<T: std::str::FromStr + Default>(item: &HashMap<String, AttributeValue>, k: &str) -> T {
    item.get(k).and_then(|v| v.as_n().ok()).and_then(|v| v.parse().ok()).unwrap_or_default()
}

impl Ledger {
    pub async fn new(dynamodb_table: &str, batch_ttl_days: u64) -> Self {
        let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ledger { client: Client::new(&sdk), table: dynamodb_table.into(), batch_ttl_secs: batch_ttl_days as i64 * 86400 }
    }

    pub async fn get_batch(&self, table: &str, batch_id: &str) -> Result<Option<BatchRecord>> {
        let out = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("pk", s(table))
            .key("sk", s(format!("b#{batch_id}")))
            .consistent_read(true)
            .send()
            .await
            .context("ledger get_batch")?;
        Ok(out.item.map(|i| batch_from_item(&i)))
    }

    pub async fn put_batch(&self, table: &str, b: &BatchRecord) -> Result<()> {
        let mut item = HashMap::from([
            ("pk".to_string(), s(table)),
            ("sk".to_string(), s(format!("b#{}", b.batch_id))),
            ("batch_id".to_string(), s(&b.batch_id)),
            ("op".to_string(), s(&b.op)),
            ("status".to_string(), s(&b.status)),
            ("started_at".to_string(), n(b.started_at)),
            ("ttl".to_string(), n(b.started_at + self.batch_ttl_secs)),
        ]);
        for (k, v) in [("version", b.version), ("data_version", b.data_version), ("rows", b.rows), ("partitions", b.partitions)] {
            if v != 0 {
                item.insert(k.into(), n(v));
            }
        }
        if b.committed_at != 0 {
            item.insert("committed_at".into(), n(b.committed_at));
        }
        if let Some(e) = &b.error {
            item.insert("error_code".into(), s(&e.code));
            item.insert("error_message".into(), s(&e.message));
        }
        self.client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(item))
            .send()
            .await
            .context("ledger put_batch")?;
        Ok(())
    }

    /// Most recent batches first (batch ids start with a UTC timestamp, so sk order is time order).
    pub async fn recent_batches(&self, table: &str, limit: i32) -> Result<Vec<BatchRecord>> {
        let out = self
            .client
            .query()
            .table_name(&self.table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :b)")
            .expression_attribute_values(":pk", s(table))
            .expression_attribute_values(":b", s("b#"))
            .scan_index_forward(false)
            .limit(limit)
            .send()
            .await
            .context("ledger recent_batches")?;
        Ok(out.items().iter().map(batch_from_item).collect())
    }

    /// Upserts (rows > 0) or removes (rows = 0) the partitions written by one commit.
    pub async fn write_partitions(
        &self,
        table: &str,
        parts: &[PartitionWrite],
        version: u64,
        batch_id: &str,
        committed_at: i64,
    ) -> Result<()> {
        let mut requests: Vec<WriteRequest> = Vec::with_capacity(parts.len());
        for p in parts {
            let key = HashMap::from([("pk".to_string(), s(table)), ("sk".to_string(), s(format!("p#{}", p.key)))]);
            let req = if p.rows == 0 {
                WriteRequest::builder().delete_request(DeleteRequest::builder().set_key(Some(key)).build().map_err(anyhow::Error::from)?).build()
            } else {
                let mut item = key;
                item.extend([
                    ("key".to_string(), s(&p.key)),
                    ("values".to_string(), s(serde_json::to_string(&p.values).map_err(anyhow::Error::from)?)),
                    ("hash".to_string(), s(&p.hash)),
                    ("rows".to_string(), n(p.rows)),
                    ("committed_at".to_string(), n(committed_at)),
                    ("version".to_string(), n(version)),
                    ("batch_id".to_string(), s(batch_id)),
                ]);
                if p.id != 0 {
                    item.insert("id".into(), n(p.id));
                }
                if p.records_updated != 0 {
                    item.insert("records_updated".into(), n(p.records_updated));
                }
                WriteRequest::builder().put_request(PutRequest::builder().set_item(Some(item)).build().map_err(anyhow::Error::from)?).build()
            };
            requests.push(req);
        }
        for chunk in requests.chunks(25) {
            let mut pending = chunk.to_vec();
            for attempt in 0..8 {
                let out = self
                    .client
                    .batch_write_item()
                    .request_items(&self.table, pending.clone())
                    .send()
                    .await
                    .context("ledger write_partitions")?;
                pending = out.unprocessed_items.and_then(|mut m| m.remove(&self.table)).unwrap_or_default();
                if pending.is_empty() {
                    break;
                }
                if attempt == 7 {
                    return Err(anyhow!("ledger: {} partition writes still unprocessed", pending.len()).into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(50 << attempt)).await;
            }
        }
        Ok(())
    }

    pub async fn list_partitions(&self, table: &str) -> Result<Vec<PartitionRecord>> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let page = self
                .client
                .query()
                .table_name(&self.table)
                .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                .expression_attribute_values(":pk", s(table))
                .expression_attribute_values(":p", s("p#"))
                .set_exclusive_start_key(start)
                .send()
                .await
                .context("ledger list_partitions")?;
            for i in page.items() {
                out.push(PartitionRecord {
                    key: get_s(i, "key"),
                    values: serde_json::from_str(&get_s(i, "values")).unwrap_or(serde_json::Value::Null),
                    id: get_n(i, "id"),
                    hash: get_s(i, "hash"),
                    records_updated: get_n(i, "records_updated"),
                    rows: get_n(i, "rows"),
                    committed_at: get_n(i, "committed_at"),
                    version: get_n(i, "version"),
                    batch_id: get_s(i, "batch_id"),
                });
            }
            start = page.last_evaluated_key;
            if start.is_none() {
                return Ok(out);
            }
        }
    }
}

fn batch_from_item(i: &HashMap<String, AttributeValue>) -> BatchRecord {
    let code = get_s(i, "error_code");
    BatchRecord {
        batch_id: get_s(i, "batch_id"),
        op: get_s(i, "op"),
        status: get_s(i, "status"),
        version: get_n(i, "version"),
        data_version: get_n(i, "data_version"),
        started_at: get_n(i, "started_at"),
        committed_at: get_n(i, "committed_at"),
        rows: get_n(i, "rows"),
        partitions: get_n(i, "partitions"),
        error: (!code.is_empty()).then(|| ApiErrorRecord { code, message: get_s(i, "error_message") }),
    }
}
