//! Control ops: create_table, list_tables, list_partitions, get_batch. Changing or deleting an
//! existing table is in `schema` (update_schema, drop_table).

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchIterator};
use arrow::ipc::reader::StreamReader;
use arrow_schema::{Field, Schema};
use base64::Engine;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use object_store::path::Path;
use serde::Serialize;

use crate::app::{App, arrow_schema, load_spec};
use crate::error::{Error, Result, code};
use crate::ledger::{BatchRecord, PartitionRecord};
use crate::ops::info::{self, TableInfo};
use crate::protocol::{BatchRef, CreateTable, TableInfoReq, TableRef};
use crate::spec::{SPEC_CONFIG_KEY, TableSpec, schema_diff, validate, validate_table_name};

/// Lance field metadata key selecting the compression codec of a column.
const COMPRESSION_META: &str = "lance-encoding:compression";

#[derive(Debug, Serialize)]
pub struct CreateTableResult {
    pub created: bool,
    pub table: TableInfo,
}

pub async fn create_table(app: &App, req: CreateTable) -> Result<CreateTableResult> {
    let (schema, spec) = requested(&req)?;
    // A table that already exists as requested is answered without the lock: clients call
    // create_table if_not_exists before writing, and waiting there behind a long write made the
    // control function time out (docs/escrituras-concurrentes-y-costo-de-indices.md). Reading a
    // version is safe while another writer commits.
    if req.if_not_exists {
        match app.open(&req.table).await {
            Ok(ds) if load_spec(&ds)? == spec && schema_diff(&arrow_schema(&ds), &schema).is_empty() => {
                let info = info::table_info(app, &ds, &req.table, false).await?;
                return Ok(CreateTableResult { created: false, table: info });
            }
            Ok(_) => {} // the locked path reports the mismatch
            Err(Error::Api(e)) if e.code == code::TABLE_NOT_FOUND => {}
            Err(e) => return Err(e),
        }
    }
    let lock = app.lock_control(&req.table, "create_table").await?;
    let result = create_locked(app, &req.table, schema, spec, req.if_not_exists).await;
    lock.release().await;
    result
}

/// The schema (with its compression metadata) and the validated spec a create_table body asks for.
pub fn requested(req: &CreateTable) -> Result<(Arc<Schema>, TableSpec)> {
    validate_table_name(&req.table)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&req.schema_ipc_b64)
        .map_err(|e| Error::api(code::BAD_REQUEST, format!("schema_ipc_b64: {e}")))?;
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| Error::api(code::BAD_REQUEST, format!("schema_ipc_b64 is not an Arrow IPC stream: {e}")))?;
    let schema = reader.schema();
    let spec = validate(
        &schema,
        TableSpec {
            spec_version: 0,
            partition_keys: req.partition_keys.clone(),
            sort_key: req.sort_key.clone(),
            indexes: req.indexes.clone(),
            storage_version: req.storage_version.clone(),
            field_compression: req.field_compression.clone(),
        },
    )?;
    Ok((Arc::new(with_compression(&schema, &spec)), spec))
}

/// Creates the table; an existing one is never touched (update_schema changes it, drop_table
/// deletes it). Call with the table lock held.
pub async fn create_locked(
    app: &App,
    table: &str,
    schema: Arc<Schema>,
    spec: TableSpec,
    if_not_exists: bool,
) -> Result<CreateTableResult> {
    match app.open(table).await {
        Ok(ds) => {
            let current = load_spec(&ds)?;
            let diff = schema_diff(&arrow_schema(&ds), &schema);
            if current == spec && diff.is_empty() && if_not_exists {
                let info = info::table_info(app, &ds, table, false).await?;
                return Ok(CreateTableResult { created: false, table: info });
            }
            let details = serde_json::json!({ "schema_diff": diff, "current_spec": current, "requested_spec": spec });
            let msg = if if_not_exists {
                format!("table {table:?} exists with a different schema or spec (update_schema changes it)")
            } else {
                format!("table {table:?} already exists (update_schema changes it, drop_table deletes it)")
            };
            return Err(Error::with_details(code::TABLE_EXISTS_SPEC_MISMATCH, msg, details));
        }
        Err(Error::Api(e)) if e.code == code::TABLE_NOT_FOUND => {}
        Err(e) => return Err(e),
    }

    let params = WriteParams {
        mode: WriteMode::Create,
        data_storage_version: Some(spec.file_version()),
        session: Some(app.session.clone()),
        ..Default::default()
    };
    let empty = RecordBatchIterator::new(Vec::<std::result::Result<RecordBatch, _>>::new(), schema.clone());
    let mut ds = Dataset::write(empty, &app.cfg.table_uri(table), Some(params)).await?;
    store_spec(&mut ds, &spec).await?;
    let info = info::table_info(app, &ds, table, false).await?;
    Ok(CreateTableResult { created: true, table: info })
}

/// Saves the spec in the dataset config (a commit of its own).
pub async fn store_spec(ds: &mut Dataset, spec: &TableSpec) -> Result<()> {
    let spec_json = serde_json::to_string(spec).map_err(anyhow::Error::from)?;
    ds.update_config([(SPEC_CONFIG_KEY, spec_json.as_str())]).await?;
    Ok(())
}

/// Copies the spec's per-column codecs into the field metadata Lance reads.
fn with_compression(schema: &Schema, spec: &TableSpec) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match spec.field_compression.get(f.name()) {
            Some(codec) => {
                let mut meta: HashMap<String, String> = f.metadata().clone();
                meta.insert(COMPRESSION_META.into(), codec.clone());
                f.as_ref().clone().with_metadata(meta)
            }
            None => f.as_ref().clone(),
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

#[derive(Debug, Serialize)]
pub struct TableEntry {
    pub table: String,
}

pub async fn list_tables(app: &App) -> Result<Vec<TableEntry>> {
    let prefix = Path::from(app.cfg.storage.tables_prefix.as_str());
    let listing = app.store.list_with_delimiter(Some(&prefix)).await?;
    let mut out: Vec<TableEntry> = listing
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename()?.strip_suffix(".lance").map(|t| TableEntry { table: t.to_string() }))
        .collect();
    out.sort_by(|a, b| a.table.cmp(&b.table));
    Ok(out)
}

#[derive(Debug, Serialize)]
pub struct PartitionsResult {
    pub table: String,
    pub partitions: Vec<PartitionRecord>,
}

pub async fn list_partitions(app: &App, req: TableRef) -> Result<PartitionsResult> {
    validate_table_name(&req.table)?;
    let mut partitions = app.ledger.list_partitions(&req.table).await?;
    partitions.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(PartitionsResult { table: req.table, partitions })
}

pub async fn get_batch(app: &App, req: BatchRef) -> Result<BatchRecord> {
    validate_table_name(&req.table)?;
    app.ledger
        .get_batch(&req.table, &req.batch_id)
        .await?
        .ok_or_else(|| Error::api(code::BATCH_NOT_FOUND, format!("batch {:?} not found for table {:?}", req.batch_id, req.table)))
}

pub async fn table_info(app: &App, req: TableInfoReq) -> Result<TableInfo> {
    let ds = app.open(&req.table).await?;
    info::table_info(app, &ds, &req.table, req.detail).await
}

/// Lists files under a prefix (bucket-relative), as (path, size).
pub async fn list_files(app: &App, prefix: &str) -> Result<Vec<(Path, u64)>> {
    let prefix = Path::from(prefix);
    let items: Vec<_> = app.store.list(Some(&prefix)).try_collect().await?;
    Ok(items.into_iter().map(|m| (m.location, m.size)).collect())
}
