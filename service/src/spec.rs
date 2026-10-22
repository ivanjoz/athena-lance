//! Table spec (layout decided by the caller) and its validation (plan §3.3).
//! The spec is stored in the Lance dataset config under `svc.spec`; the schema lives in the
//! Lance manifest. No table is defined in code.

use std::collections::{BTreeMap, HashSet};
use std::sync::LazyLock;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, code};

pub const SPEC_CONFIG_KEY: &str = "svc.spec";
const MAX_COLUMNS: usize = 1000;
const MAX_INDEXES: usize = 32;

pub static NAME_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9_]{0,62}$").unwrap());
const RESERVED: &[&str] = &["select", "from", "where", "group", "order", "by", "limit", "table", "and", "or", "not", "null"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSpec {
    pub column: String,
    #[serde(rename = "type")]
    pub index_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl IndexSpec {
    /// Explicit name: given, or `<column>_<type>` lowercase.
    pub fn resolved_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| format!("{}_{}", self.column, self.index_type.to_lowercase()))
    }

    pub fn lance_type(&self) -> Option<IndexType> {
        Some(match self.index_type.as_str() {
            "BTREE" => IndexType::BTree,
            "BITMAP" => IndexType::Bitmap,
            "ZONEMAP" => IndexType::ZoneMap,
            "BLOOMFILTER" => IndexType::BloomFilter,
            "LABEL_LIST" => IndexType::LabelList,
            "NGRAM" => IndexType::NGram,
            "INVERTED" => IndexType::Inverted,
            _ => return None,
        })
    }

    /// Lance scalar-index plugin name for ScalarIndexParams.
    pub fn plugin_name(&self) -> String {
        self.index_type.to_lowercase()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSpec {
    pub spec_version: u32,
    pub partition_keys: Vec<String>,
    pub sort_key: Vec<String>,
    pub indexes: Vec<IndexSpec>,
    pub storage_version: String,
    pub field_compression: BTreeMap<String, String>,
}

impl TableSpec {
    pub fn file_version(&self) -> LanceFileVersion {
        match self.storage_version.as_str() {
            "2.3" => LanceFileVersion::V2_3,
            _ => LanceFileVersion::V2_2,
        }
    }
}

fn invalid(msg: impl Into<String>) -> Error {
    Error::api(code::INVALID_SPEC, msg)
}

pub fn validate_table_name(name: &str) -> Result<()> {
    if !NAME_RE.is_match(name) || RESERVED.contains(&name) {
        return Err(Error::api(code::BAD_REQUEST, format!("invalid table name {name:?} (must match {})", NAME_RE.as_str())));
    }
    Ok(())
}

/// Normalizes and validates a spec against its schema.
pub fn validate(schema: &Schema, mut spec: TableSpec) -> Result<TableSpec> {
    if spec.storage_version.is_empty() {
        spec.storage_version = "2.2".into();
    }
    if !matches!(spec.storage_version.as_str(), "2.2" | "2.3") {
        return Err(invalid(format!("storage_version {:?} not supported (2.2 or 2.3)", spec.storage_version)));
    }
    if schema.fields().is_empty() || schema.fields().len() > MAX_COLUMNS {
        return Err(invalid(format!("a table needs 1..={MAX_COLUMNS} columns")));
    }
    for f in schema.fields() {
        validate_field(f, f.name())?;
    }

    let top = |name: &str| schema.field_with_name(name).ok();
    let mut seen = HashSet::new();
    for k in &spec.partition_keys {
        let f = top(k).ok_or_else(|| invalid(format!("partition key {k:?} is not a column")))?;
        if f.is_nullable() {
            return Err(invalid(format!("partition key {k:?} must not be nullable")));
        }
        if !is_partition_type(f.data_type()) {
            return Err(invalid(format!("partition key {k:?} has unsupported type {}", f.data_type())));
        }
        if !seen.insert(k) {
            return Err(invalid(format!("partition key {k:?} repeated")));
        }
    }
    for k in &spec.sort_key {
        let f = top(k).ok_or_else(|| invalid(format!("sort key {k:?} is not a column")))?;
        if !is_scalar(f.data_type()) {
            return Err(invalid(format!("sort key {k:?} must be a scalar column")));
        }
    }

    if spec.indexes.len() > MAX_INDEXES {
        return Err(invalid(format!("at most {MAX_INDEXES} indexes")));
    }
    let mut names = HashSet::new();
    let mut filter_class = HashSet::new();
    for ix in &mut spec.indexes {
        ix.index_type = ix.index_type.to_uppercase();
        let f = top(&ix.column).ok_or_else(|| invalid(format!("index column {:?} is not a column", ix.column)))?;
        if ix.lance_type().is_none() {
            return Err(invalid(format!("unknown index type {:?}", ix.index_type)));
        }
        let dt = f.data_type();
        let ok = match ix.index_type.as_str() {
            "BTREE" | "BITMAP" | "ZONEMAP" | "BLOOMFILTER" => is_scalar(dt),
            "LABEL_LIST" => matches!(dt, DataType::List(_) | DataType::LargeList(_)),
            "NGRAM" | "INVERTED" => matches!(dt, DataType::Utf8 | DataType::LargeUtf8),
            _ => false,
        };
        if !ok {
            return Err(invalid(format!("index {} is not valid on column {:?} of type {dt}", ix.index_type, ix.column)));
        }
        let name = ix.resolved_name();
        if !NAME_RE.is_match(&name) || !names.insert(name.clone()) {
            return Err(invalid(format!("index name {name:?} is invalid or repeated")));
        }
        ix.name = Some(name);
        // BTREE and BITMAP answer the same filters: two on one column only cost build time.
        if matches!(ix.index_type.as_str(), "BTREE" | "BITMAP") && !filter_class.insert(ix.column.clone()) {
            return Err(invalid(format!("column {:?} already has a BTREE/BITMAP index", ix.column)));
        }
    }
    for (col, codec) in &spec.field_compression {
        if top(col).is_none() {
            return Err(invalid(format!("field_compression column {col:?} is not a column")));
        }
        if !matches!(codec.as_str(), "zstd" | "lz4" | "none") {
            return Err(invalid(format!("compression {codec:?} not supported (zstd, lz4, none)")));
        }
    }
    spec.spec_version = 1;
    Ok(spec)
}

fn validate_field(f: &Field, path: &str) -> Result<()> {
    if !NAME_RE.is_match(f.name()) {
        return Err(invalid(format!("column name {path:?} must match {}", NAME_RE.as_str())));
    }
    if f.metadata().contains_key("ARROW:extension:name") {
        return Err(invalid(format!("column {path:?}: extension types are not supported")));
    }
    match f.data_type() {
        DataType::List(child) | DataType::LargeList(child) => validate_child(child, path),
        DataType::Struct(children) => {
            for c in children {
                validate_field(c, &format!("{path}.{}", c.name()))?;
            }
            Ok(())
        }
        dt if is_scalar(dt) || matches!(dt, DataType::Binary | DataType::LargeBinary) => Ok(()),
        dt => Err(invalid(format!("column {path:?}: type {dt} is not supported"))),
    }
}

/// List elements are named "item" by Arrow, so only their type is checked.
fn validate_child(f: &Field, path: &str) -> Result<()> {
    match f.data_type() {
        DataType::Struct(children) => {
            for c in children {
                validate_field(c, &format!("{path}[].{}", c.name()))?;
            }
            Ok(())
        }
        DataType::List(child) | DataType::LargeList(child) => validate_child(child, path),
        dt if is_scalar(dt) || matches!(dt, DataType::Binary | DataType::LargeBinary) => Ok(()),
        dt => Err(invalid(format!("column {path:?}: element type {dt} is not supported"))),
    }
}

pub fn is_scalar(dt: &DataType) -> bool {
    use DataType::*;
    match dt {
        Boolean | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 | Utf8
        | LargeUtf8 | Date32 | Decimal128(_, _) => true,
        Timestamp(unit, _) => matches!(unit, TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond),
        _ => false,
    }
}

pub fn is_partition_type(dt: &DataType) -> bool {
    use DataType::*;
    matches!(dt, Boolean | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Utf8 | LargeUtf8 | Timestamp(_, _))
}

/// Differences between an incoming file schema and the table schema, for SCHEMA_MISMATCH.
pub fn schema_diff(table: &Schema, file: &Schema) -> Vec<String> {
    let mut diff = Vec::new();
    for f in table.fields() {
        match file.field_with_name(f.name()) {
            Err(_) => diff.push(format!("missing column {}", f.name())),
            Ok(g) if g.data_type() != f.data_type() => {
                diff.push(format!("column {}: type {} in file, {} in table", f.name(), g.data_type(), f.data_type()))
            }
            Ok(g) if g.is_nullable() && !f.is_nullable() => {
                diff.push(format!("column {}: nullable in file, not null in table", f.name()))
            }
            Ok(_) => {}
        }
    }
    for g in file.fields() {
        if table.field_with_name(g.name()).is_err() {
            diff.push(format!("unknown column {}", g.name()));
        }
    }
    let order_differs = diff.is_empty()
        && table.fields().iter().zip(file.fields().iter()).any(|(a, b)| a.name() != b.name());
    if order_differs {
        diff.push("columns are in a different order".into());
    }
    diff
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("filial_id", DataType::Int16, false),
            Field::new("fecha", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, true),
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))), true),
            Field::new("note", DataType::Utf8, true),
        ])
    }

    fn spec() -> TableSpec {
        TableSpec {
            spec_version: 0,
            partition_keys: vec!["filial_id".into(), "fecha".into()],
            sort_key: vec!["fecha".into()],
            indexes: vec![
                IndexSpec { column: "amount".into(), index_type: "btree".into(), name: None },
                IndexSpec { column: "tags".into(), index_type: "LABEL_LIST".into(), name: None },
                IndexSpec { column: "note".into(), index_type: "INVERTED".into(), name: None },
            ],
            storage_version: String::new(),
            field_compression: BTreeMap::from([("note".into(), "zstd".into())]),
        }
    }

    #[test]
    fn valid_spec_is_normalized() {
        let s = validate(&schema(), spec()).unwrap();
        assert_eq!(s.storage_version, "2.2");
        assert_eq!(s.indexes[0].index_type, "BTREE");
        assert_eq!(s.indexes[0].name.as_deref(), Some("amount_btree"));
    }

    #[test]
    fn invalid_specs_are_rejected() {
        let cases: Vec<Box<dyn Fn(&mut TableSpec)>> = vec![
            Box::new(|s| s.partition_keys = vec!["amount".into()]), // nullable
            Box::new(|s| s.partition_keys = vec!["nope".into()]),
            Box::new(|s| s.sort_key = vec!["tags".into()]),          // not scalar
            Box::new(|s| s.indexes[1].column = "note".into()),       // label_list on string
            Box::new(|s| s.indexes[2].column = "amount".into()),     // inverted on float
            Box::new(|s| s.indexes.push(IndexSpec { column: "amount".into(), index_type: "BITMAP".into(), name: None })),
            Box::new(|s| s.indexes[0].index_type = "HASH".into()),
            Box::new(|s| s.storage_version = "2.0".into()),
            Box::new(|s| { s.field_compression.insert("note".into(), "gzip".into()); }),
        ];
        for (i, mutate) in cases.iter().enumerate() {
            let mut s = spec();
            mutate(&mut s);
            assert!(validate(&schema(), s).is_err(), "case {i} should fail");
        }
    }

    #[test]
    fn bad_column_names_and_types() {
        let s = Schema::new(vec![Field::new("Amount", DataType::Float64, true)]);
        assert!(validate(&s, TableSpec { indexes: vec![], partition_keys: vec![], sort_key: vec![], field_compression: BTreeMap::new(), ..spec() }).is_err());
        let s = Schema::new(vec![Field::new("d", DataType::Duration(TimeUnit::Second), true)]);
        assert!(validate(&s, TableSpec { indexes: vec![], partition_keys: vec![], sort_key: vec![], field_compression: BTreeMap::new(), ..spec() }).is_err());
    }

    #[test]
    fn diff_reports_differences() {
        let file = Schema::new(vec![
            Field::new("filial_id", DataType::Int32, false),
            Field::new("fecha", DataType::Utf8, false),
            Field::new("extra", DataType::Utf8, true),
        ]);
        let d = schema_diff(&schema(), &file);
        assert!(d.iter().any(|x| x.contains("filial_id: type")));
        assert!(d.iter().any(|x| x.contains("missing column amount")));
        assert!(d.iter().any(|x| x.contains("unknown column extra")));
    }
}
