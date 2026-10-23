//! Changes to an existing table: `update_schema` / `create_or_update` (ADR-015) and
//! `drop_table`, the only op that deletes a table.
//!
//! update_schema compares the requested columns with the table's, by name, and never rewrites
//! data files:
//! * new column: added as all nulls (metadata only: the existing files read it as null). It is
//!   nullable in the table even if requested not null, because the existing rows have no value.
//! * column no longer requested: dropped (metadata only; compaction removes its data later).
//! * same name, other type: rejected with SCHEMA_MISMATCH, nothing is changed.
//! * not null in the table, nullable requested: made nullable (metadata only). The opposite is
//!   kept as is: a not-null column can always be written into a nullable one.
//!
//! Partition keys, sort key and storage version can't change (they decide where the existing rows
//! are). Indexes follow the request: the ones no longer requested are dropped, new ones built.
//! Each step is its own Lance commit; if one fails, calling update_schema again finishes the rest.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use futures::{StreamExt, TryStreamExt};
use lance::dataset::{ColumnAlteration, NewColumnTransform};
use lance::index::DatasetIndexExt;
use object_store::path::Path;
use serde::Serialize;
use serde_json::json;

use crate::app::{App, arrow_schema, load_spec};
use crate::error::{Error, Result, code};
use crate::lock::TableLock;
use crate::ops::control;
use crate::ops::info::{self, TableInfo};
use crate::ops::write::ensure_indexes;
use crate::protocol::{CreateTable, TableRef};
use crate::spec::{TableSpec, validate_table_name};

#[derive(Debug, Default, PartialEq, Serialize)]
pub struct SchemaChanges {
    pub added: Vec<String>,
    pub dropped: Vec<String>,
    pub made_nullable: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct UpdateSchemaResult {
    /// create_or_update created the table.
    pub created: bool,
    /// Something was committed (columns, indexes or spec).
    pub changed: bool,
    #[serde(flatten)]
    pub changes: SchemaChanges,
    pub indexes_dropped: Vec<String>,
    pub table: TableInfo,
}

pub async fn update_schema(app: &App, req: CreateTable, create_if_missing: bool) -> Result<UpdateSchemaResult> {
    let (schema, spec) = control::requested(&req)?;
    // No storage version requested: the table's (the create default applies to a new table).
    let keep_storage = req.storage_version.is_empty();
    let lock = app.lock(&req.table, "update_schema", "update_schema").await?;
    let result = update_locked(app, &req.table, schema, spec, keep_storage, create_if_missing, &lock).await;
    lock.release().await;
    result
}

async fn update_locked(
    app: &App,
    table: &str,
    schema: Arc<Schema>,
    mut spec: TableSpec,
    keep_storage: bool,
    create_if_missing: bool,
    lock: &TableLock,
) -> Result<UpdateSchemaResult> {
    let mut ds = match app.open(table).await {
        Ok(ds) => ds,
        Err(Error::Api(e)) if e.code == code::TABLE_NOT_FOUND && create_if_missing => {
            let created = control::create_locked(app, table, schema, spec, false).await?;
            return Ok(UpdateSchemaResult {
                created: true,
                changed: true,
                changes: SchemaChanges::default(),
                indexes_dropped: vec![],
                table: created.table,
            });
        }
        Err(e) => return Err(e),
    };
    let current = load_spec(&ds)?;
    if keep_storage {
        spec.storage_version = current.storage_version.clone();
    }
    let changes = plan(&arrow_schema(&ds), &schema, &current, &spec)?;

    // Every index is the spec's; names starting with "__" are Lance's own (e.g. fragment reuse).
    let wanted: HashSet<String> = spec.indexes.iter().map(|ix| ix.resolved_name()).collect();
    let indexes_dropped: Vec<String> = ds
        .load_indices()
        .await?
        .iter()
        .map(|ix| ix.name.clone())
        .filter(|name| !name.starts_with("__") && !wanted.contains(name))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let changed = changes != SchemaChanges::default() || !indexes_dropped.is_empty() || current != spec;
    if changed {
        lock.check()?;
        for name in &indexes_dropped {
            ds.drop_index(name).await?;
        }
        // Add before dropping: Lance refuses to drop every column of a table.
        if !changes.added.is_empty() {
            let fields: Vec<Field> = changes
                .added
                .iter()
                .map(|name| schema.field_with_name(name).expect("planned from this schema").clone().with_nullable(true))
                .collect();
            ds.add_columns(NewColumnTransform::AllNulls(Arc::new(Schema::new(fields))), None, None).await?;
        }
        if !changes.dropped.is_empty() {
            let names: Vec<&str> = changes.dropped.iter().map(String::as_str).collect();
            ds.drop_columns(&names).await?;
        }
        if !changes.made_nullable.is_empty() {
            let alterations: Vec<ColumnAlteration> =
                changes.made_nullable.iter().map(|name| ColumnAlteration::new(name.clone()).set_nullable(true)).collect();
            ds.alter_columns(&alterations).await?;
        }
        if current != spec {
            control::store_spec(&mut ds, &spec).await?;
        }
        ensure_indexes(&mut ds, &spec, app.cfg.compaction.max_index_segments).await?;
        tracing::warn!(
            table,
            version = ds.version().version,
            added = ?changes.added,
            dropped = ?changes.dropped,
            made_nullable = ?changes.made_nullable,
            indexes_dropped = ?indexes_dropped,
            "schema updated"
        );
    }
    let info = info::table_info(app, &ds, table, false).await?;
    Ok(UpdateSchemaResult { created: false, changed, changes, indexes_dropped, table: info })
}

/// The column changes that bring `table` to `requested`, or why it can't be done in place.
pub fn plan(table: &Schema, requested: &Schema, current: &TableSpec, spec: &TableSpec) -> Result<SchemaChanges> {
    let mut layout = Vec::new();
    if current.partition_keys != spec.partition_keys {
        layout.push(format!("partition_keys: {:?} in the table, {:?} requested", current.partition_keys, spec.partition_keys));
    }
    if current.sort_key != spec.sort_key {
        layout.push(format!("sort_key: {:?} in the table, {:?} requested", current.sort_key, spec.sort_key));
    }
    if current.storage_version != spec.storage_version {
        layout.push(format!("storage_version: {} in the table, {} requested", current.storage_version, spec.storage_version));
    }
    if !layout.is_empty() {
        return Err(Error::with_details(
            code::TABLE_EXISTS_SPEC_MISMATCH,
            "partition keys, sort key and storage version can't change (drop_table, then create it again)",
            json!(layout),
        ));
    }

    let mut changes = SchemaChanges::default();
    let mut incompatible = Vec::new();
    for f in table.fields() {
        let Ok(g) = requested.field_with_name(f.name()) else {
            changes.dropped.push(f.name().clone());
            continue;
        };
        if g.data_type() != f.data_type() {
            incompatible.push(format!("column {}: type {} in the table, {} requested", f.name(), f.data_type(), g.data_type()));
            continue;
        }
        if g.is_nullable() && !f.is_nullable() {
            changes.made_nullable.push(f.name().clone());
        }
        let (was, now) = (current.field_compression.get(f.name()), spec.field_compression.get(f.name()));
        if was != now {
            incompatible.push(format!("column {}: compression {was:?} in the table, {now:?} requested", f.name()));
        }
    }
    for g in requested.fields() {
        if table.field_with_name(g.name()).is_err() {
            changes.added.push(g.name().clone());
        }
    }
    if !incompatible.is_empty() {
        return Err(Error::with_details(
            code::SCHEMA_MISMATCH,
            "existing columns can't change type or compression (drop_table, then create it again)",
            json!(incompatible),
        ));
    }
    Ok(changes)
}

#[derive(Debug, Serialize)]
pub struct DropTableResult {
    pub table: String,
    pub files_deleted: usize,
}

/// Deletes every file of the table (all versions) and its partition registry. The batch records
/// stay in the ledger until their TTL.
pub async fn drop_table(app: &App, req: TableRef) -> Result<DropTableResult> {
    validate_table_name(&req.table)?;
    let lock = app.lock_control(&req.table, "drop_table").await?;
    let result = drop_locked(app, &req.table, &lock).await;
    lock.release().await;
    result
}

async fn drop_locked(app: &App, table: &str, lock: &TableLock) -> Result<DropTableResult> {
    let root = Path::from(format!("{}/{table}.lance", app.cfg.storage.tables_prefix));
    let files: Vec<Path> = app.store.list(Some(&root)).map_ok(|m| m.location).try_collect().await?;
    if files.is_empty() {
        return Err(Error::api(code::TABLE_NOT_FOUND, format!("table {table:?} does not exist")));
    }
    lock.check()?;
    // Manifests first: once they are gone no reader sees the table, even if deleting the data
    // fails halfway (drop_table again deletes what is left).
    let files_deleted = files.len();
    let (manifests, data): (Vec<Path>, Vec<Path>) = files.into_iter().partition(|p| p.as_ref().contains("/_versions/"));
    delete_all(app, manifests).await?;
    delete_all(app, data).await?;
    app.ledger.delete_partitions(table).await?;
    app.query.forget(table);
    tracing::warn!(table, files_deleted, "table dropped");
    Ok(DropTableResult { table: table.to_string(), files_deleted })
}

async fn delete_all(app: &App, paths: Vec<Path>) -> Result<()> {
    let locations = futures::stream::iter(paths.into_iter().map(Ok)).boxed();
    app.store.delete_stream(locations).try_collect::<Vec<_>>().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use std::collections::BTreeMap;

    fn spec() -> TableSpec {
        TableSpec {
            spec_version: 1,
            partition_keys: vec!["filial_id".into()],
            sort_key: vec![],
            indexes: vec![],
            storage_version: "2.2".into(),
            field_compression: BTreeMap::new(),
        }
    }

    fn table() -> Schema {
        Schema::new(vec![
            Field::new("filial_id", DataType::Int16, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("note", DataType::Utf8, true),
        ])
    }

    #[test]
    fn adds_drops_and_relaxes_by_name() {
        let requested = Schema::new(vec![
            Field::new("extra", DataType::Int32, false),
            Field::new("amount", DataType::Float64, true),
            Field::new("filial_id", DataType::Int16, false),
        ]);
        let c = plan(&table(), &requested, &spec(), &spec()).unwrap();
        assert_eq!(c.added, vec!["extra"]);
        assert_eq!(c.dropped, vec!["note"]);
        assert_eq!(c.made_nullable, vec!["amount"]);

        // Same columns in another order, or not null requested for a nullable column: no change.
        let requested = Schema::new(vec![
            Field::new("note", DataType::Utf8, false),
            Field::new("filial_id", DataType::Int16, false),
            Field::new("amount", DataType::Float64, false),
        ]);
        assert_eq!(plan(&table(), &requested, &spec(), &spec()).unwrap(), SchemaChanges::default());
    }

    #[test]
    fn type_and_layout_changes_are_rejected() {
        let requested = Schema::new(vec![
            Field::new("filial_id", DataType::Int16, false),
            Field::new("amount", DataType::Utf8, false),
        ]);
        let err = plan(&table(), &requested, &spec(), &spec()).unwrap_err().to_api();
        assert_eq!(err.code, code::SCHEMA_MISMATCH);
        assert!(err.details.unwrap().to_string().contains("column amount: type"));

        let moved = TableSpec { partition_keys: vec![], ..spec() };
        let err = plan(&table(), &table(), &spec(), &moved).unwrap_err().to_api();
        assert_eq!(err.code, code::TABLE_EXISTS_SPEC_MISMATCH);

        let compressed = TableSpec { field_compression: BTreeMap::from([("note".into(), "zstd".into())]), ..spec() };
        let err = plan(&table(), &table(), &spec(), &compressed).unwrap_err().to_api();
        assert_eq!(err.code, code::SCHEMA_MISMATCH);
    }
}
