//! `table_info` (and `describe_table`): everything worth knowing about one table.
//!
//!   - version, rows, deleted rows, schema and spec
//!   - files: count and size distribution of the current version's data files
//!   - storage: files under data/ that the current version no longer references (removed by
//!     cleanup once their versions pass the retention), and versions eligible for cleanup
//!   - indexes: coverage (fragments not yet indexed) and spec indexes not built yet
//!   - compaction: what a `compact` with the default limits would do now
//!   - registry: partitions in the DynamoDB ledger
//!   - operations: the table lock and recent / pending / failed batches
//!   - detail=true: per-fragment list with min/max of the partition and sort columns (how
//!     well filters on them can skip fragments) and how many fragments each partition spans

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::compute::{SortOptions, sort_to_indices};
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::deps::datafusion::scalar::ScalarValue;
use lance::index::DatasetIndexExt;
use serde::Serialize;
use serde_json::Value;

use crate::app::{App, arrow_schema, load_spec};
use crate::error::Result;
use crate::ledger::{self, BatchRecord};
use crate::lock::{self, LockBody};
use crate::ops::compact::{CompactLimits, fragment_bytes, plan_table};
use crate::ops::control::list_files;
use crate::ops::write::{PVal, column_pvals};
use crate::spec::TableSpec;

const RECENT_BATCHES: i32 = 20;
const MB: u64 = 1 << 20;
const DETAIL_CONCURRENCY: usize = 8;

#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub table: String,
    pub uri: String,
    pub version: u64,
    /// Unix seconds of the current version.
    pub version_time: i64,
    pub storage_version: String,
    pub rows: u64,
    pub deleted_rows: u64,
    pub columns: Vec<Column>,
    pub spec: TableSpec,
    pub files: Files,
    pub storage: Storage,
    pub versions: Versions,
    pub indexes: Indexes,
    pub compaction: Compaction,
    pub registry: Registry,
    pub operations: Operations,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Detail>,
}

#[derive(Debug, Serialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct Files {
    pub fragments: usize,
    pub data_files: usize,
    pub deletion_files: usize,
    pub total_bytes: u64,
    pub min_bytes: u64,
    pub max_bytes: u64,
    pub avg_bytes: u64,
    /// Fragments whose size the manifest does not record (never compacted).
    pub unknown_size: usize,
    /// Fragment count and bytes by size range.
    pub size_buckets: Vec<SizeBucket>,
}

#[derive(Debug, Serialize)]
pub struct SizeBucket {
    pub range: &'static str,
    pub fragments: usize,
    pub bytes: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct Storage {
    /// Every object under the table's data/ directory.
    pub data_objects: usize,
    pub data_bytes: u64,
    /// Data files the current version does not use (older versions, failed writes).
    pub unreferenced_files: usize,
    pub unreferenced_bytes: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct Versions {
    pub count: usize,
    pub oldest: u64,
    pub oldest_time: i64,
    pub retention_hours: u64,
    /// Versions older than the retention: the next cleanup (run by `compact`) removes them
    /// and the files only they reference.
    pub cleanup_eligible: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct Indexes {
    pub built: Vec<IndexInfo>,
    /// Spec indexes not built yet (they are built after the first write with rows).
    pub missing: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct IndexInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub index_type: String,
    pub columns: Vec<String>,
    /// Index segments (deltas); optimize merges them.
    pub segments: usize,
    pub fragments_indexed: usize,
    /// Fragments written after the index was last updated: scanned without the index.
    pub fragments_unindexed: usize,
}

#[derive(Debug, Serialize)]
pub struct Compaction {
    pub limits: CompactLimits,
    /// Groups a `compact` with these limits would merge now (up to max_tasks).
    pub groups: usize,
    pub fragments: usize,
    pub bytes: u64,
    /// More groups than max_tasks are eligible.
    pub more: bool,
    pub recommended: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct Registry {
    pub partitions: usize,
    pub rows: u64,
    pub oldest_committed_at: i64,
    pub newest_committed_at: i64,
}

#[derive(Debug, Serialize)]
pub struct Operations {
    pub lock: Option<LockState>,
    pub pending: usize,
    pub failed: usize,
    pub recent: Vec<BatchRecord>,
}

#[derive(Debug, Serialize)]
pub struct LockState {
    #[serde(flatten)]
    pub body: LockBody,
    /// Held now (not released and not expired).
    pub held: bool,
}

#[derive(Debug, Serialize)]
pub struct Detail {
    pub fragments: Vec<FragmentInfo>,
    /// Per partition/sort column: in how many fragments a single value can be, on average.
    pub pruning: BTreeMap<String, ColumnPruning>,
    pub partitions: Option<PartitionSpread>,
}

#[derive(Debug, Serialize)]
pub struct FragmentInfo {
    pub id: u64,
    pub rows: u64,
    pub deleted_rows: u64,
    pub bytes: Option<u64>,
    pub files: Vec<String>,
    pub deletion_file: bool,
    /// min/max per partition and sort column.
    pub stats: BTreeMap<String, MinMax>,
    pub partitions: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct MinMax {
    pub min: Value,
    pub max: Value,
}

#[derive(Debug, Serialize)]
pub struct ColumnPruning {
    /// Fragments whose [min, max] ranges overlap another fragment's range.
    pub overlapping_fragments: usize,
    pub fragments: usize,
}

#[derive(Debug, Serialize)]
pub struct PartitionSpread {
    pub partitions: usize,
    /// Partitions whose rows sit in more than one fragment.
    pub in_multiple_fragments: usize,
    pub max_fragments_per_partition: usize,
}

pub async fn table_info(app: &App, ds: &Dataset, table: &str, detail: bool) -> Result<TableInfo> {
    let spec = load_spec(ds)?;
    let schema = arrow_schema(ds);
    let fragments: Vec<_> = ds.get_fragments().iter().map(|f| f.metadata().clone()).collect();
    let version = ds.version();

    let lock_path = app.lock_path(table);
    let data_prefix = format!("{}/{table}.lance/data", app.cfg.storage.tables_prefix);
    let (rows, versions, indices, lock, recent, registry, objects) = futures::try_join!(
        async { Ok(ds.count_rows(None).await? as u64) },
        async { Ok(ds.versions().await?) },
        async { Ok(ds.load_indices().await?) },
        lock::read(&app.store, &lock_path),
        app.ledger.recent_batches(table, RECENT_BATCHES),
        app.ledger.list_partitions(table),
        list_files(app, &data_prefix),
    )?;

    // Files.
    let sizes: Vec<Option<u64>> = fragments.iter().map(fragment_bytes).collect();
    let known: Vec<u64> = sizes.iter().flatten().copied().collect();
    let total: u64 = known.iter().sum();
    let buckets: [(&str, u64); 5] = [("<1MB", MB), ("1-8MB", 8 * MB), ("8-32MB", 32 * MB), ("32-128MB", 128 * MB), (">=128MB", u64::MAX)];
    let mut size_buckets: Vec<SizeBucket> = buckets.iter().map(|(r, _)| SizeBucket { range: r, fragments: 0, bytes: 0 }).collect();
    for s in &known {
        let i = buckets.iter().position(|(_, hi)| s < hi).unwrap_or(buckets.len() - 1);
        size_buckets[i].fragments += 1;
        size_buckets[i].bytes += s;
    }
    let files = Files {
        fragments: fragments.len(),
        data_files: fragments.iter().map(|f| f.files.len()).sum(),
        deletion_files: fragments.iter().filter(|f| f.deletion_file.is_some()).count(),
        total_bytes: total,
        min_bytes: known.iter().copied().min().unwrap_or(0),
        max_bytes: known.iter().copied().max().unwrap_or(0),
        avg_bytes: if known.is_empty() { 0 } else { total / known.len() as u64 },
        unknown_size: sizes.len() - known.len(),
        size_buckets,
    };
    let deleted_rows: u64 = fragments
        .iter()
        .filter_map(|f| f.deletion_file.as_ref().and_then(|d| d.num_deleted_rows))
        .map(|n| n as u64)
        .sum();

    // Storage: data files not referenced by the current version.
    let referenced: HashSet<&str> = fragments.iter().flat_map(|f| f.files.iter().map(|d| d.path.as_str())).collect();
    let mut storage = Storage::default();
    for (path, size) in &objects {
        storage.data_objects += 1;
        storage.data_bytes += size;
        let name = path.filename().unwrap_or_default();
        if !referenced.contains(name) {
            storage.unreferenced_files += 1;
            storage.unreferenced_bytes += size;
        }
    }

    // Versions.
    let retention_hours = app.cfg.service.version_retention_hours;
    let cutoff = chrono::Utc::now() - chrono::Duration::hours(retention_hours as i64);
    let latest = version.version;
    let versions = Versions {
        count: versions.len(),
        oldest: versions.iter().map(|v| v.version).min().unwrap_or(latest),
        oldest_time: versions.iter().map(|v| v.timestamp.timestamp()).min().unwrap_or(0),
        retention_hours,
        cleanup_eligible: versions.iter().filter(|v| v.version != latest && v.timestamp < cutoff).count(),
    };

    // Indexes.
    let fragment_ids: HashSet<u32> = fragments.iter().map(|f| f.id as u32).collect();
    let mut by_name: BTreeMap<String, (String, Vec<String>, usize, HashSet<u32>)> = BTreeMap::new();
    for ix in indices.iter() {
        let entry = by_name.entry(ix.name.clone()).or_insert_with(|| {
            let index_type = ix
                .index_details
                .as_ref()
                .map(|d| d.type_url.rsplit(['.', '/']).next().unwrap_or_default().trim_end_matches("IndexDetails").to_string())
                .unwrap_or_default();
            let columns = ix.fields.iter().filter_map(|id| ds.schema().field_by_id(*id).map(|f| f.name.clone())).collect();
            (index_type, columns, 0, HashSet::new())
        });
        entry.2 += 1;
        if let Some(bitmap) = &ix.fragment_bitmap {
            entry.3.extend(bitmap.iter());
        }
    }
    let built: Vec<IndexInfo> = by_name
        .into_iter()
        .map(|(name, (index_type, columns, segments, covered))| {
            let indexed = covered.intersection(&fragment_ids).count();
            IndexInfo {
                name,
                index_type,
                columns,
                segments,
                fragments_indexed: indexed,
                fragments_unindexed: fragment_ids.len() - indexed,
            }
        })
        .collect();
    let built_names: HashSet<&str> = built.iter().map(|i| i.name.as_str()).collect();
    let missing = spec.indexes.iter().map(|i| i.resolved_name()).filter(|n| !built_names.contains(n.as_str())).collect();

    // Compaction preview with the default limits.
    let limits = CompactLimits::resolve(app, 0, 0, 0)?;
    let (groups, more) = plan_table(&spec, &sizes, &limits);
    let compaction = Compaction {
        limits,
        groups: groups.len(),
        fragments: groups.iter().map(|g| g.len()).sum(),
        bytes: groups.iter().flat_map(|g| sizes[g.clone()].iter().flatten()).sum(),
        more,
        recommended: !groups.is_empty(),
    };

    // Registry.
    let registry = Registry {
        partitions: registry.len(),
        rows: registry.iter().map(|p| p.rows).sum(),
        oldest_committed_at: registry.iter().map(|p| p.committed_at).min().unwrap_or(0),
        newest_committed_at: registry.iter().map(|p| p.committed_at).max().unwrap_or(0),
    };

    // Operations.
    let now = lock::now_ms();
    let operations = Operations {
        lock: lock.map(|(body, _)| LockState { held: body.expires_at > now, body }),
        pending: recent.iter().filter(|b| b.status == ledger::PENDING).count(),
        failed: recent.iter().filter(|b| b.status == ledger::FAILED).count(),
        recent,
    };

    let detail = if detail { Some(detail_info(ds, &spec, &fragments).await?) } else { None };

    Ok(TableInfo {
        table: table.into(),
        uri: app.cfg.table_uri(table),
        version: latest,
        version_time: version.timestamp.timestamp(),
        storage_version: ds.manifest().data_storage_format.version.to_string(),
        rows,
        deleted_rows,
        columns: schema
            .fields()
            .iter()
            .map(|f| Column { name: f.name().clone(), data_type: f.data_type().to_string(), nullable: f.is_nullable() })
            .collect(),
        spec,
        files,
        storage,
        versions,
        indexes: Indexes { built, missing },
        compaction,
        registry,
        operations,
        detail,
    })
}

/// Scans the partition and sort columns of every fragment.
async fn detail_info(ds: &Dataset, spec: &TableSpec, fragments: &[lance_table::format::Fragment]) -> Result<Detail> {
    let mut columns: Vec<String> = spec.partition_keys.clone();
    for k in &spec.sort_key {
        if !columns.contains(k) {
            columns.push(k.clone());
        }
    }

    let scanned: Vec<(Option<BTreeMap<String, (ScalarValue, ScalarValue)>>, HashSet<Vec<PVal>>)> =
        futures::stream::iter(fragments.iter().cloned())
            .map(|frag| {
                let columns = &columns;
                async move { scan_fragment(ds, spec, frag, columns).await }
            })
            .buffered(DETAIL_CONCURRENCY)
            .try_collect()
            .await?;

    let mut infos = Vec::with_capacity(fragments.len());
    let mut spread: HashMap<Vec<PVal>, usize> = HashMap::new();
    for (frag, (stats, parts)) in fragments.iter().zip(&scanned) {
        for p in parts {
            *spread.entry(p.clone()).or_default() += 1;
        }
        let deleted = frag.deletion_file.as_ref().and_then(|d| d.num_deleted_rows).unwrap_or(0) as u64;
        infos.push(FragmentInfo {
            id: frag.id,
            rows: frag.physical_rows.unwrap_or(0) as u64 - deleted,
            deleted_rows: deleted,
            bytes: fragment_bytes(frag),
            files: frag.files.iter().map(|d| d.path.clone()).collect(),
            deletion_file: frag.deletion_file.is_some(),
            stats: stats
                .iter()
                .flatten()
                .map(|(c, (lo, hi))| (c.clone(), MinMax { min: scalar_json(lo), max: scalar_json(hi) }))
                .collect(),
            partitions: parts.len(),
        });
    }

    let mut pruning = BTreeMap::new();
    for c in &columns {
        let ranges: Vec<&(ScalarValue, ScalarValue)> = scanned.iter().filter_map(|(s, _)| s.as_ref()?.get(c)).collect();
        let overlapping = (0..ranges.len())
            .filter(|&i| {
                (0..ranges.len()).any(|j| {
                    j != i
                        && ranges[i].0.partial_cmp(&ranges[j].1).is_some_and(|o| o.is_le())
                        && ranges[j].0.partial_cmp(&ranges[i].1).is_some_and(|o| o.is_le())
                })
            })
            .count();
        pruning.insert(c.clone(), ColumnPruning { overlapping_fragments: overlapping, fragments: ranges.len() });
    }

    let partitions = (!spec.partition_keys.is_empty()).then(|| PartitionSpread {
        partitions: spread.len(),
        in_multiple_fragments: spread.values().filter(|n| **n > 1).count(),
        max_fragments_per_partition: spread.values().copied().max().unwrap_or(0),
    });
    Ok(Detail { fragments: infos, pruning, partitions })
}

type FragmentScan = (Option<BTreeMap<String, (ScalarValue, ScalarValue)>>, HashSet<Vec<PVal>>);

async fn scan_fragment(
    ds: &Dataset,
    spec: &TableSpec,
    frag: lance_table::format::Fragment,
    columns: &[String],
) -> Result<FragmentScan> {
    if columns.is_empty() {
        return Ok((None, HashSet::new()));
    }
    let mut scan = ds.scan();
    scan.with_fragments(vec![frag]);
    scan.project(columns)?;
    let batches: Vec<RecordBatch> = scan.try_into_stream().await?.try_collect().await?;

    let mut stats: BTreeMap<String, (ScalarValue, ScalarValue)> = BTreeMap::new();
    let mut parts = HashSet::new();
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        for c in columns {
            let Some((lo, hi)) = min_max(b.column_by_name(c).expect("projected"))? else { continue };
            stats
                .entry(c.clone())
                .and_modify(|(a, z)| {
                    if lo.partial_cmp(a).is_some_and(|o| o.is_lt()) {
                        *a = lo.clone();
                    }
                    if hi.partial_cmp(z).is_some_and(|o| o.is_gt()) {
                        *z = hi.clone();
                    }
                })
                .or_insert((lo, hi));
        }
        if !spec.partition_keys.is_empty() {
            let cols = spec
                .partition_keys
                .iter()
                .map(|k| column_pvals(b.column_by_name(k).expect("projected")))
                .collect::<Result<Vec<_>>>()?;
            for row in 0..b.num_rows() {
                parts.insert(cols.iter().map(|c| c[row].clone()).collect());
            }
        }
    }
    Ok((Some(stats), parts))
}

/// Smallest and largest non-null value of a column (one partial sort each way).
fn min_max(a: &Arc<dyn Array>) -> Result<Option<(ScalarValue, ScalarValue)>> {
    if a.null_count() == a.len() {
        return Ok(None);
    }
    let first = |descending| -> Result<ScalarValue> {
        let idx = sort_to_indices(a, Some(SortOptions { descending, nulls_first: false }), Some(1))?;
        Ok(ScalarValue::try_from_array(a, idx.value(0) as usize).map_err(anyhow::Error::from)?)
    };
    Ok(Some((first(false)?, first(true)?)))
}

fn scalar_json(v: &ScalarValue) -> Value {
    match v {
        ScalarValue::Int8(Some(x)) => (*x).into(),
        ScalarValue::Int16(Some(x)) => (*x).into(),
        ScalarValue::Int32(Some(x)) => (*x).into(),
        ScalarValue::Int64(Some(x)) => (*x).into(),
        ScalarValue::UInt8(Some(x)) => (*x).into(),
        ScalarValue::UInt16(Some(x)) => (*x).into(),
        ScalarValue::UInt32(Some(x)) => (*x).into(),
        ScalarValue::UInt64(Some(x)) => (*x).into(),
        ScalarValue::Boolean(Some(x)) => (*x).into(),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone().into(),
        other => other.to_string().into(),
    }
}
