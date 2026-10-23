//! Query ops: read-only SQL over the tables through DataFusion (plan §4.6, §5.1).
//!
//! No catalog is declared: each request resolves the table names its SQL references, opens
//! `tables/<name>.lance` (or takes it from the process-wide cache) and registers it in a fresh
//! SessionContext, so a table created at runtime is queryable at once. A cached table is
//! re-checked for a newer version after `query.freshness_seconds`, or right away when the
//! request names a minimum version for it (the one `Save` confirmed).
//!
//! Results are cached in the bucket (`query_cache`): a request whose result is cached and young
//! enough is answered without planning or running anything.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::json::writer::{LineDelimited, WriterBuilder};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SQLOptions, SessionConfig, SessionContext};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::DataFrame;
use datafusion::sql::TableReference;
use futures::StreamExt;
use lance::Dataset;
use lance::datafusion::LanceTableProvider;
use lance_io::object_store::{ObjectStoreParams, WrappingObjectStore};
use lance_io::utils::tracking_store::IOTracker;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::query_cache::Lookup;
use crate::app::App;
use crate::config::QueryCfg;
use crate::error::{Error, Result, code};
use crate::protocol::Query;
use crate::spec::validate_table_name;

/// Memory assumed for the DataFusion pool outside Lambda (the CLI).
const LOCAL_MEMORY_MB: u64 = 2048;

pub struct QueryEngine {
    pub(crate) runtime: Arc<RuntimeEnv>,
    pub(crate) io: IOTracker,
    /// The same allocation on every open: Lance keys its object store cache by this pointer.
    io_wrapper: Arc<dyn WrappingObjectStore>,
    datasets: Mutex<HashMap<String, Cached>>,
    pub(crate) warm: AtomicBool,
}

#[derive(Clone)]
struct Cached {
    ds: Arc<Dataset>,
    checked: Instant,
}

impl QueryEngine {
    pub fn new(cfg: &QueryCfg) -> anyhow::Result<Self> {
        let memory_mb = std::env::var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(LOCAL_MEMORY_MB);
        let pool_bytes = (memory_mb as f64 * cfg.memory_fraction) as usize * 1024 * 1024;
        let runtime = RuntimeEnvBuilder::new().with_memory_limit(pool_bytes, 1.0).build_arc()?;
        let io = IOTracker::default();
        Ok(QueryEngine {
            runtime,
            io_wrapper: Arc::new(io.clone()),
            io,
            datasets: Mutex::new(HashMap::new()),
            warm: AtomicBool::new(false),
        })
    }

    /// The table at its latest version, from the cache while it is fresh enough.
    pub(crate) async fn dataset(&self, app: &App, table: &str, min_version: Option<u64>) -> Result<Arc<Dataset>> {
        let cached = self.datasets.lock().unwrap().get(table).cloned();
        let max_age = Duration::from_secs(app.cfg.query.freshness_seconds);
        let ds = match cached {
            Some(c) if c.checked.elapsed() < max_age && min_version.is_none_or(|v| c.ds.version().version >= v) => {
                return Ok(c.ds);
            }
            // The ETag and not only the version number: a table dropped and created again can
            // reach the cached version number with other data.
            Some(c) => match c.ds.latest_manifest().await {
                Ok((_, latest)) => {
                    let current = c.ds.manifest_location();
                    if latest.version == current.version && latest.e_tag.is_some() && latest.e_tag == current.e_tag {
                        c.ds
                    } else {
                        let mut ds = (*c.ds).clone();
                        ds.checkout_latest().await?;
                        Arc::new(ds)
                    }
                }
                // Dropped: opening it again reports TABLE_NOT_FOUND.
                Err(_) => {
                    self.forget(table);
                    Arc::new(self.open(app, table).await?)
                }
            },
            None => Arc::new(self.open(app, table).await?),
        };
        let version = ds.version().version;
        self.datasets.lock().unwrap().insert(table.to_string(), Cached { ds: ds.clone(), checked: Instant::now() });
        if let Some(min) = min_version
            && version < min
        {
            return Err(Error::api(
                code::VERSION_NOT_AVAILABLE,
                format!("table {table:?} is at version {version}, the query asked for at least {min}"),
            ));
        }
        Ok(ds)
    }

    async fn open(&self, app: &App, table: &str) -> Result<Dataset> {
        let params = ObjectStoreParams { object_store_wrapper: Some(self.io_wrapper.clone()), ..Default::default() };
        app.open_with(table, Some(params)).await
    }

    /// Drops the cached dataset of a table (drop_table, in this container).
    pub(crate) fn forget(&self, table: &str) {
        self.datasets.lock().unwrap().remove(table);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    /// Array of objects keyed by column name (nulls included).
    pub rows: Value,
    pub row_count: usize,
    /// More rows existed than max_rows, or they did not fit in query.max_response_bytes.
    pub truncated: bool,
    pub stats: QueryStats,
    /// explain only: EXPLAIN ANALYZE output (the query runs).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub elapsed_ms: f64,
    /// Table resolution (opening / refreshing datasets) + SQL planning. 0 on a cache hit.
    pub plan_ms: f64,
    pub exec_ms: f64,
    pub bytes_read: u64,
    pub get_requests: u64,
    pub cold_start: bool,
    /// On a cache hit, the versions the cached result was computed at.
    pub dataset_versions: BTreeMap<String, u64>,
    /// The result came from the cache, computed cache_age_ms ago.
    #[serde(default)]
    pub cache_hit: bool,
    #[serde(default)]
    pub cache_age_ms: u64,
}

pub async fn query(app: &App, req: Query, explain: bool) -> Result<QueryResult> {
    let q = &app.cfg.query;
    if !req.format.is_empty() && req.format != "json" {
        return Err(Error::api(code::BAD_REQUEST, format!("format {:?} is not supported (use \"json\")", req.format)));
    }
    let max_rows = match req.max_rows {
        0 => q.default_max_rows,
        n => n.min(q.max_rows_limit),
    };
    let timeout_ms = match req.timeout_ms {
        0 => q.default_timeout_ms,
        n => n.min(q.max_timeout_ms),
    };

    let started = Instant::now();
    let cold_start = !app.query.warm.swap(true, Ordering::Relaxed);
    // EXPLAIN ANALYZE is for looking at a run, so it always runs.
    let cache = if explain { None } else { Lookup::new(q, &req.sql, max_rows, req.cache_seconds) };
    if let Some(cache) = &cache
        && let Some(mut result) = cache.get(app, &req.sql, &req.min_versions).await
    {
        // bytes_read / get_requests count table reads, and there were none.
        result.stats.bytes_read = 0;
        result.stats.get_requests = 0;
        result.stats.cold_start = cold_start;
        result.stats.elapsed_ms = ms(started);
        return Ok(result);
    }

    // One invocation at a time per Lambda container: the counters belong to this query.
    app.query.io.incremental_stats();
    let mut result = tokio::time::timeout(Duration::from_millis(timeout_ms), run(app, &req, explain, max_rows, started))
        .await
        .map_err(|_| Error::api(code::QUERY_TIMEOUT, format!("query exceeded {timeout_ms} ms")))??;
    let io = app.query.io.incremental_stats();
    result.stats.bytes_read = io.read_bytes;
    result.stats.get_requests = io.read_iops;
    result.stats.cold_start = cold_start;
    if let Some(cache) = &cache {
        cache.put(app, &req.sql, &result).await;
    }
    result.stats.elapsed_ms = ms(started);
    Ok(result)
}

async fn run(app: &App, req: &Query, explain: bool, max_rows: usize, started: Instant) -> Result<QueryResult> {
    let (df, columns, dataset_versions) = prepare(app, &req.sql, &req.min_versions).await?;
    let plan_ms = ms(started);

    let (rows, row_count, truncated, plan) = if explain {
        (Value::Array(vec![]), 0, false, Some(explain_analyze(df).await?))
    } else {
        let (rows, n, truncated) = collect_json(df, max_rows, app.cfg.query.max_response_bytes).await?;
        (rows, n, truncated, None)
    };
    let stats = QueryStats { plan_ms, exec_ms: ms(started) - plan_ms, dataset_versions, ..Default::default() };
    Ok(QueryResult { columns, rows, row_count, truncated, stats, plan })
}

/// Resolves and registers the tables the SQL names, and plans it read-only. Shared by the JSON
/// query op and the Trino HTTP API, which differ only in how they render the rows.
pub(crate) async fn prepare(
    app: &App,
    sql: &str,
    min_versions: &BTreeMap<String, u64>,
) -> Result<(DataFrame, Vec<Column>, BTreeMap<String, u64>)> {
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), app.query.runtime.clone());
    let mut dataset_versions = BTreeMap::new();
    for name in referenced_tables(&ctx, sql)? {
        let ds = app.query.dataset(app, &name, min_versions.get(&name).copied()).await?;
        dataset_versions.insert(name.clone(), ds.version().version);
        register(&ctx, &name, ds)?;
    }
    let (df, columns) = plan(&ctx, sql).await?;
    Ok((df, columns, dataset_versions))
}

/// The distinct tables the SQL references (CTE names excluded). Only tables of the service's
/// namespace can be named: no catalogs, schemas or paths.
fn referenced_tables(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(sql, &dialect).map_err(df_error)?;
    let mut names = Vec::new();
    for reference in state.resolve_table_references(&statement).map_err(df_error)? {
        let name = match &reference {
            TableReference::Bare { table } => table.to_string(),
            TableReference::Partial { schema, table } if schema.as_ref() == "public" => table.to_string(),
            _ => String::new(),
        };
        if validate_table_name(&name).is_err() {
            return Err(Error::api(code::SQL_ERROR, format!("{reference} is not a table of this service")));
        }
        if !names.contains(&name) {
            names.push(name);
        }
    }
    Ok(names)
}

pub(crate) fn register(ctx: &SessionContext, name: &str, ds: Arc<Dataset>) -> Result<()> {
    // Unordered: rows of a scan may interleave across fragments; SQL has no order without ORDER BY.
    ctx.register_table(name, Arc::new(LanceTableProvider::new_with_ordering(ds, false, false, false)))
        .map_err(df_error)?;
    Ok(())
}

/// Plans the SQL as a read-only query: DDL, DML and statements like SET are rejected.
pub(crate) async fn plan(ctx: &SessionContext, sql: &str) -> Result<(DataFrame, Vec<Column>)> {
    let read_only = SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false);
    let df = ctx.sql_with_options(sql, read_only).await.map_err(df_error)?;
    let columns = df
        .schema()
        .fields()
        .iter()
        .map(|f| Column { name: f.name().clone(), data_type: f.data_type().to_string(), nullable: f.is_nullable() })
        .collect();
    Ok((df, columns))
}

/// Runs the query and renders up to max_rows rows (and max_bytes of JSON) as a JSON array.
async fn collect_json(df: DataFrame, max_rows: usize, max_bytes: usize) -> Result<(Value, usize, bool)> {
    // One row past the cap tells whether the result was truncated; the limit is pushed into the scan.
    let mut stream = df.limit(0, Some(max_rows + 1)).map_err(df_error)?.execute_stream().await.map_err(df_error)?;
    let mut out = Vec::with_capacity(4096);
    out.push(b'[');
    let (mut count, mut truncated) = (0, false);
    'batches: while let Some(batch) = stream.next().await {
        let lines = to_ndjson(&batch.map_err(df_error)?)?;
        // JSON strings escape newlines, so every line is exactly one row.
        for line in lines.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            if count == max_rows || out.len() + line.len() + 2 > max_bytes {
                truncated = true;
                break 'batches;
            }
            if count > 0 {
                out.push(b',');
            }
            out.extend_from_slice(line);
            count += 1;
        }
    }
    out.push(b']');
    let rows = serde_json::from_slice(&out).map_err(anyhow::Error::from)?;
    Ok((rows, count, truncated))
}

fn to_ndjson(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = WriterBuilder::new().with_explicit_nulls(true).build::<_, LineDelimited>(Vec::new());
    writer.write(batch)?;
    writer.finish()?;
    Ok(writer.into_inner())
}

/// EXPLAIN ANALYZE as text: one section per plan type (logical / physical with metrics).
async fn explain_analyze(df: DataFrame) -> Result<String> {
    let batches = df.explain(false, true).map_err(df_error)?.collect().await.map_err(df_error)?;
    let mut text = String::new();
    for batch in &batches {
        let plan_type = arrow::compute::cast(batch.column(0), &arrow_schema::DataType::Utf8)?;
        let plan = arrow::compute::cast(batch.column(1), &arrow_schema::DataType::Utf8)?;
        let (plan_type, plan) = (arrow::array::as_string_array(&plan_type), arrow::array::as_string_array(&plan));
        for i in 0..batch.num_rows() {
            text.push_str(&format!("{}:\n{}\n", plan_type.value(i), plan.value(i)));
        }
    }
    Ok(text)
}

/// Errors in the SQL itself are the caller's (SQL_ERROR); an exhausted memory pool is
/// reported as such; anything else is internal.
pub(crate) fn df_error(e: DataFusionError) -> Error {
    match e.find_root() {
        DataFusionError::SQL(..)
        | DataFusionError::Plan(..)
        | DataFusionError::SchemaError(..)
        | DataFusionError::NotImplemented(..) => Error::api(code::SQL_ERROR, e.to_string()),
        DataFusionError::ResourcesExhausted(..) => Error::api(
            code::RESOURCES_EXHAUSTED,
            format!("{e} (reduce the data the query sorts, groups or joins, or raise the function memory)"),
        ),
        _ =>Error::Internal(anyhow::anyhow!("{e}")),
    }
}

pub(crate) fn ms(since: Instant) -> f64 {
    (since.elapsed().as_secs_f64() * 1e6).round() / 1e3
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, RecordBatchIterator, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use lance::index::DatasetIndexExt;
    use lance_index::IndexType;
    use lance_index::scalar::ScalarIndexParams;

    const ROWS: i64 = 1000;

    /// A local Lance table "ventas" (id with a BTREE index) registered in a fresh context.
    async fn context() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, true),
            Field::new("sold_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        ]));
        let regions = ["PE", "CL", "MX", "US"];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..ROWS)),
                Arc::new(StringArray::from_iter_values((0..ROWS).map(|i| regions[i as usize % 4]))),
                Arc::new(Float64Array::from_iter((0..ROWS).map(|i| (i % 10 != 0).then_some(i as f64 * 1.5)))),
                Arc::new(TimestampMicrosecondArray::from_iter_values((0..ROWS).map(|i| 1_790_812_800_000_000 + i)).with_timezone("UTC")),
            ],
        )
        .unwrap();
        let dir = std::env::temp_dir().join(format!("lance-query-test-{}", uuid::Uuid::new_v4()));
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let mut ds = Dataset::write(reader, dir.to_str().unwrap(), None).await.unwrap();
        ds.create_index(&["id"], IndexType::BTree, Some("id_btree".into()), &ScalarIndexParams::new("btree".into()), false)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        register(&ctx, "ventas", Arc::new(ds)).unwrap();
        ctx
    }

    async fn fetch(ctx: &SessionContext, sql: &str, max_rows: usize, max_bytes: usize) -> (Value, usize, bool) {
        let (df, _) = plan(ctx, sql).await.unwrap();
        collect_json(df, max_rows, max_bytes).await.unwrap()
    }

    fn code_of(r: Result<impl std::fmt::Debug>) -> &'static str {
        match r {
            Err(Error::Api(e)) => e.code,
            other => panic!("expected an API error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn aggregates_and_json_rows() {
        let ctx = context().await;
        let sql = "SELECT region, count(*) AS n, sum(amount) AS total FROM ventas GROUP BY region ORDER BY region";
        let (df, columns) = plan(&ctx, sql).await.unwrap();
        assert_eq!(columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["region", "n", "total"]);
        let (rows, n, truncated) = collect_json(df, 100, 1 << 20).await.unwrap();
        assert_eq!((n, truncated), (4, false));
        assert_eq!(rows[0]["region"], "CL");
        assert_eq!(rows[0]["n"], 250);

        // Nulls are explicit and timestamps keep their zone.
        let (rows, _, _) = fetch(&ctx, "SELECT id, amount, sold_at FROM ventas WHERE id = 10", 100, 1 << 20).await;
        assert_eq!(rows[0]["amount"], Value::Null);
        assert_eq!(rows[0]["sold_at"], "2026-10-01T00:00:00.000010Z");
    }

    #[tokio::test]
    async fn truncates_by_rows_and_bytes() {
        let ctx = context().await;
        let (rows, n, truncated) = fetch(&ctx, "SELECT * FROM ventas", 10, 1 << 20).await;
        assert_eq!((rows.as_array().unwrap().len(), n, truncated), (10, 10, true));
        // Exactly max_rows rows is not truncated.
        let (_, n, truncated) = fetch(&ctx, "SELECT * FROM ventas WHERE id < 10", 10, 1 << 20).await;
        assert_eq!((n, truncated), (10, false));
        // ~90 bytes per row: 1000 bytes hold about 10.
        let (_, n, truncated) = fetch(&ctx, "SELECT * FROM ventas", 1000, 1000).await;
        assert!(truncated && n > 0 && n < 15, "n = {n}");
    }

    #[tokio::test]
    async fn read_only_and_namespace() {
        let ctx = context().await;
        for sql in [
            "CREATE TABLE t AS SELECT 1",
            "INSERT INTO ventas SELECT * FROM ventas",
            "DROP TABLE ventas",
            "SET datafusion.execution.batch_size = 1",
            "SELECT * FROM nope",
            "SELEC 1",
        ] {
            assert_eq!(code_of(plan(&ctx, sql).await), code::SQL_ERROR, "{sql}");
        }
        for sql in ["SELECT * FROM other.ventas", "SELECT * FROM c.public.ventas", "SELECT * FROM 's3://x/y.parquet'"] {
            assert_eq!(code_of(referenced_tables(&ctx, sql)), code::SQL_ERROR, "{sql}");
        }
        let names = referenced_tables(
            &ctx,
            "WITH v AS (SELECT * FROM ventas) SELECT * FROM v JOIN public.ventas w ON v.id = w.id JOIN pagos p ON p.id = v.id",
        )
        .unwrap();
        let mut names = names;
        names.sort();
        assert_eq!(names, ["pagos", "ventas"]);
    }

    #[tokio::test]
    async fn explain_shows_index_use() {
        let ctx = context().await;
        let (df, _) = plan(&ctx, "SELECT id, amount FROM ventas WHERE id = 42").await.unwrap();
        let text = explain_analyze(df).await.unwrap();
        assert!(text.contains("Plan with Metrics"), "{text}");
        assert!(text.contains("ScalarIndexQuery: query=[id = 42]@id_btree"), "{text}");
    }
}
