//! Wire types: must match lancego/protocol.go exactly.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::spec::IndexSpec;

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    // lance-control
    CreateTable(CreateTable),
    DescribeTable(TableRef),
    TableInfo(TableInfoReq),
    ListTables(Empty),
    ListPartitions(TableRef),
    GetBatch(BatchRef),
    // lance-ingest (serialized per table by the table lock)
    Ingest(Ingest),
    ReplacePartitions(ReplacePartitions),
    Compact(Compact),
}

impl Request {
    pub fn op(&self) -> &'static str {
        match self {
            Request::CreateTable(_) => "create_table",
            Request::DescribeTable(_) => "describe_table",
            Request::TableInfo(_) => "table_info",
            Request::ListTables(_) => "list_tables",
            Request::ListPartitions(_) => "list_partitions",
            Request::GetBatch(_) => "get_batch",
            Request::Ingest(_) => "ingest",
            Request::ReplacePartitions(_) => "replace_partitions",
            Request::Compact(_) => "compact",
        }
    }

    pub fn table(&self) -> Option<&str> {
        match self {
            Request::CreateTable(r) => Some(&r.table),
            Request::DescribeTable(r) | Request::ListPartitions(r) => Some(&r.table),
            Request::TableInfo(r) => Some(&r.table),
            Request::GetBatch(r) => Some(&r.table),
            Request::Ingest(r) => Some(&r.table),
            Request::ReplacePartitions(r) => Some(&r.table),
            Request::Compact(r) => Some(&r.table),
            Request::ListTables(_) => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Empty {}

#[derive(Debug, Deserialize)]
pub struct TableRef {
    pub table: String,
}

#[derive(Debug, Deserialize)]
pub struct TableInfoReq {
    pub table: String,
    /// Include the per-file list and the pruning stats (scans the partition and sort columns).
    #[serde(default)]
    pub detail: bool,
}

#[derive(Debug, Deserialize)]
pub struct BatchRef {
    pub table: String,
    pub batch_id: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateTable {
    pub table: String,
    pub schema_ipc_b64: String,
    #[serde(default)]
    pub partition_keys: Vec<String>,
    #[serde(default)]
    pub sort_key: Vec<String>,
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    #[serde(default)]
    pub storage_version: String,
    #[serde(default)]
    pub field_compression: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Source {
    pub format: String,
    pub uris: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Ingest {
    pub table: String,
    pub batch_id: String,
    pub source: Source,
}

#[derive(Debug, Deserialize)]
pub struct ReplacePartitions {
    pub table: String,
    pub batch_id: String,
    #[serde(default)]
    pub source: Option<Source>,
    pub partitions: Vec<PartitionWrite>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionWrite {
    pub key: String,
    pub values: Vec<Value>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub id: i64,
    pub hash: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub records_updated: i64,
    pub rows: u64,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

#[derive(Debug, Deserialize)]
pub struct Compact {
    pub table: String,
    pub operation_id: String,
    #[serde(default)]
    pub max_file_mb: u64,
    #[serde(default)]
    pub max_files: usize,
    #[serde(default)]
    pub max_tasks: usize,
}
