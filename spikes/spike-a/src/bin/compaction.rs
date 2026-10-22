//! End-to-end compaction check on the S3 Express bucket:
//! many small appends + a BTREE index, then a bounded compaction that races a
//! concurrent append. Verifies no rows are lost, the limits hold, and the index still works.
//!
//! Settings: [aws], [storage] and [spikes.compaction] in config.toml.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use serde::Deserialize;
use spike_a::compaction::{CompactLimits, compact, fragment_bytes, plan_groups};

#[derive(Deserialize)]
struct Config {
    aws: Aws,
    storage: Storage,
    spikes: Spikes,
}
#[derive(Deserialize)]
struct Aws {
    profile: String,
    region: String,
}
#[derive(Deserialize)]
struct Storage {
    bucket: String,
}
#[derive(Deserialize)]
struct Spikes {
    compaction: SpikeCompaction,
}
#[derive(Deserialize)]
struct SpikeCompaction {
    prefix: String,
    appends: i64,
    rows_per_append: i64,
    max_file_mb: u64,
    max_files: usize,
    max_tasks: usize,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Rows with ids [start, start + rows). The payload is pseudo-random so it doesn't
/// compress away and fragments have a realistic size.
fn batch(start: i64, rows: i64) -> RecordBatchIterator<std::vec::IntoIter<Result<RecordBatch, arrow_schema::ArrowError>>> {
    let ids: Vec<i64> = (start..start + rows).collect();
    let amount: Vec<i64> = ids.iter().map(|i| i % 1000).collect();
    let payload: Vec<String> = ids
        .iter()
        .map(|i| {
            let x = (*i as u64).wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            format!("{x:016x}{:016x}", x.rotate_left(17) ^ 0x9e3779b97f4a7c15)
        })
        .collect();
    let rb = RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(Int64Array::from(amount)), Arc::new(StringArray::from(payload))],
    )
    .expect("valid batch");
    RecordBatchIterator::new(vec![Ok(rb)].into_iter(), schema())
}

fn append_params() -> WriteParams {
    WriteParams { mode: WriteMode::Append, data_storage_version: Some(LanceFileVersion::V2_2), ..Default::default() }
}

async fn totals(ds: &Dataset) -> Result<(usize, i64)> {
    let mut scan = ds.scan();
    scan.project(&["amount"])?;
    let batches: Vec<RecordBatch> = scan.try_into_stream().await?.try_collect().await?;
    let rows = batches.iter().map(|b| b.num_rows()).sum();
    let sum = batches
        .iter()
        .flat_map(|b| b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().iter().copied())
        .sum();
    Ok((rows, sum))
}

fn print_fragments(ds: &Dataset) -> Vec<Option<u64>> {
    let sizes: Vec<Option<u64>> = ds.get_fragments().iter().map(|f| fragment_bytes(f.metadata())).collect();
    let desc: Vec<String> = ds
        .get_fragments()
        .iter()
        .zip(&sizes)
        .map(|(f, s)| format!("#{}={}KB", f.id(), s.map_or("?".into(), |b| (b / 1024).to_string())))
        .collect();
    println!("  {} fragments: {}", sizes.len(), desc.join(" "));
    sizes
}

fn main() -> Result<()> {
    let cfg: Config = spike_a::load_config()?;
    spike_a::set_aws_env(&cfg.aws.profile, &cfg.aws.region);
    tokio::runtime::Runtime::new()?.block_on(run(cfg))
}

async fn run(cfg: Config) -> Result<()> {
    let c = &cfg.spikes.compaction;
    let run = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
    let uri = format!("s3://{}/{}/run-{run}/events.lance", cfg.storage.bucket, c.prefix);
    let limits = CompactLimits { max_file_bytes: c.max_file_mb << 20, max_files: c.max_files, max_tasks: c.max_tasks };
    println!("dataset: {uri}\nlimits: {} MB / {} files / {} tasks\n", c.max_file_mb, c.max_files, c.max_tasks);

    // 1. Fragmented table: one fragment per append, plus a BTREE index on id.
    let t = Instant::now();
    let create = WriteParams { mode: WriteMode::Create, ..append_params() };
    Dataset::write(batch(0, c.rows_per_append), uri.as_str(), Some(create)).await?;
    for a in 1..c.appends {
        Dataset::write(batch(a * c.rows_per_append, c.rows_per_append), uri.as_str(), Some(append_params())).await?;
    }
    let mut ds = Dataset::open(&uri).await?;
    ds.create_index(&["id"], IndexType::BTree, Some("id_btree".into()), &ScalarIndexParams::new("btree".into()), true)
        .await
        .context("creating BTREE index")?;
    println!("built {} appends + index in {:.1}s", c.appends, t.elapsed().as_secs_f64());

    let (rows_before, sum_before) = totals(&ds).await?;
    println!("before: v{}, {rows_before} rows, sum(amount)={sum_before}", ds.version().version);
    let sizes = print_fragments(&ds);
    let (groups, more) = plan_groups(&sizes, &limits);
    println!("  plan: {groups:?} (more={more})\n");

    // 2. Compaction racing a concurrent append from another handle.
    let extra_start = c.appends * c.rows_per_append;
    let t = Instant::now();
    let (compacted, appended) = tokio::join!(
        compact(&mut ds, limits),
        Dataset::write(batch(extra_start, c.rows_per_append), uri.as_str(), Some(append_params())),
    );
    let report = compacted.context("compaction failed")?;
    appended.context("concurrent append failed")?;
    println!("compaction + concurrent append: {:.1}s, {:?}", t.elapsed().as_secs_f64(), report);

    // 3. Verify on a fresh handle.
    let ds = Dataset::open(&uri).await?;
    let (rows_after, sum_after) = totals(&ds).await?;
    let (_, extra_sum) = totals_of_batch(extra_start, c.rows_per_append);
    println!("\nafter: v{}, {rows_after} rows, sum(amount)={sum_after}", ds.version().version);
    let after_sizes = print_fragments(&ds);
    ensure!(rows_after == rows_before + c.rows_per_append as usize, "LOST ROWS: {rows_before} + {} != {rows_after}", c.rows_per_append);
    ensure!(sum_after == sum_before + extra_sum, "sum mismatch: {sum_before} + {extra_sum} != {sum_after}");
    ensure!(after_sizes.len() < sizes.len() + 1, "fragment count did not drop");
    ensure!(report.metrics.fragments_added == groups.len(), "expected one output file per group");
    for s in after_sizes.iter().flatten() {
        ensure!(*s <= limits.max_file_bytes, "a fragment is {} bytes, over the limit", s);
    }

    // 4. The index survived the rewrite and still answers point lookups.
    let indices = ds.load_indices().await?;
    ensure!(indices.iter().any(|i| i.name == "id_btree"), "index id_btree missing after compaction");
    for id in [0, c.rows_per_append + 7, extra_start + 3] {
        let n = ds.count_rows(Some(format!("id = {id}"))).await?;
        ensure!(n == 1, "id = {id} returned {n} rows");
    }
    println!("  index id_btree present; point lookups OK");

    println!("\nversions:");
    for v in ds.versions().await?.iter().rev().take(4).rev() {
        println!("  v{} at {}", v.version, v.timestamp);
    }
    println!("\nCOMPACTION SPIKE PASSED");
    Ok(())
}

fn totals_of_batch(start: i64, rows: i64) -> (i64, i64) {
    (rows, (start..start + rows).map(|i| i % 1000).sum())
}
