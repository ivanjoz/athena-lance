//! Spike: atomically replace whole partitions of a Lance table on S3 Express.
//!
//! "Replace partitions P" = delete every row whose partition columns equal any P, and
//! insert the new rows, in ONE commit (Operation::Update), so no reader ever sees a
//! partition missing or duplicated. A partition sent with zero rows is a deletion.
//! The batch id and partition hashes travel in the commit's transaction properties, so
//! the commit itself records which batch it applied (used for idempotent retries).
//!
//! Settings: [aws], [storage] and [spikes.partitions] in config.toml.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::transaction::{Operation, TransactionBuilder};
use lance::dataset::write::delete::DeleteBuilder;
use lance::dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use serde::Deserialize;

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
    partitions: SpikePartitions,
}
#[derive(Deserialize)]
struct SpikePartitions {
    prefix: String,
    rows_per_partition: i64,
}

/// Partition = (filial_id, fecha). Rows carry a `version` marker so we can tell old from new.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("filial_id", DataType::Int32, false),
        Field::new("fecha", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("version", DataType::Int64, false),
    ]))
}

fn rows(parts: &[(i32, &str)], n: i64, version: i64, id_base: i64) -> RecordBatch {
    let (mut f, mut d, mut ids, mut v) = (vec![], vec![], vec![], vec![]);
    for (p, (filial, fecha)) in parts.iter().enumerate() {
        for i in 0..n {
            f.push(*filial);
            d.push(fecha.to_string());
            ids.push(id_base + p as i64 * 1_000_000 + i);
            v.push(version);
        }
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(f)),
            Arc::new(StringArray::from(d)),
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(v)),
        ],
    )
    .expect("valid batch")
}

fn reader(b: RecordBatch) -> RecordBatchIterator<std::vec::IntoIter<Result<RecordBatch, arrow_schema::ArrowError>>> {
    RecordBatchIterator::new(vec![Ok(b)].into_iter(), schema())
}

fn params(mode: WriteMode) -> WriteParams {
    WriteParams { mode, data_storage_version: Some(LanceFileVersion::V2_2), ..Default::default() }
}

/// SQL predicate matching any of the given partitions.
fn partitions_predicate(parts: &[(i32, &str)]) -> String {
    parts
        .iter()
        .map(|(f, d)| format!("(filial_id = {f} AND fecha = '{d}')"))
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Replaces `parts` with `new_rows` (rows of other partitions are rejected) in one commit.
async fn replace_partitions(
    ds: Arc<Dataset>,
    parts: &[(i32, &str)],
    new_rows: Option<RecordBatch>,
    properties: HashMap<String, String>,
) -> Result<Dataset> {
    let read_version = ds.version().version;

    // 1. Uncommitted delete of the old partition rows: fragments fully deleted are removed,
    //    partially deleted ones get a deletion file.
    let del = DeleteBuilder::new(ds.clone(), partitions_predicate(parts)).execute_uncommitted().await?;
    let Operation::Delete { updated_fragments, deleted_fragment_ids, .. } = del.transaction.operation else {
        anyhow::bail!("unexpected delete operation");
    };

    // 2. Uncommitted write of the new rows as new fragments.
    let new_fragments = match new_rows {
        Some(b) if b.num_rows() > 0 => {
            let txn = InsertBuilder::new(ds.clone())
                .with_params(&params(WriteMode::Append))
                .execute_uncommitted(vec![b])
                .await?;
            let Operation::Append { fragments } = txn.operation else {
                anyhow::bail!("unexpected insert operation");
            };
            fragments
        }
        _ => vec![],
    };

    // 3. One Update transaction = delete + insert, committed atomically.
    let op = Operation::Update {
        removed_fragment_ids: deleted_fragment_ids,
        updated_fragments,
        new_fragments,
        fields_modified: vec![],
        compacted_sstables: vec![],
        fields_for_preserving_frag_bitmap: vec![],
        update_mode: None,
        inserted_rows_filter: None,
        updated_fragment_offsets: None,
    };
    let txn = TransactionBuilder::new(read_version, op).transaction_properties(Some(Arc::new(properties))).build();
    Ok(CommitBuilder::new(ds).execute(txn).await?)
}

async fn count(ds: &Dataset, filter: &str) -> Result<usize> {
    Ok(ds.count_rows(Some(filter.to_string())).await?)
}

fn main() -> Result<()> {
    let cfg: Config = spike_a::load_config()?;
    spike_a::set_aws_env(&cfg.aws.profile, &cfg.aws.region);
    tokio::runtime::Runtime::new()?.block_on(run(cfg))
}

async fn run(cfg: Config) -> Result<()> {
    let n = cfg.spikes.partitions.rows_per_partition;
    let run = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
    let uri = format!("s3://{}/{}/run-{run}/ventas.lance", cfg.storage.bucket, cfg.spikes.partitions.prefix);
    println!("dataset: {uri}\n");

    let (p1, p2, p3, p4) = ((1, "2026-10-01"), (1, "2026-10-02"), (2, "2026-10-01"), (2, "2026-10-02"));

    // Initial data, v1 for all partitions. The second write mixes P1 and P3 rows in one
    // fragment, so replacing P1 must partially delete a fragment (deletion file) while
    // fully dropping the fragment that only holds P2.
    Dataset::write(reader(rows(&[p1], n, 1, 0)), uri.as_str(), Some(params(WriteMode::Create))).await?;
    Dataset::write(reader(rows(&[p1, p3], n, 1, 10_000_000)), uri.as_str(), Some(params(WriteMode::Append))).await?;
    Dataset::write(reader(rows(&[p2], n, 1, 20_000_000)), uri.as_str(), Some(params(WriteMode::Append))).await?;
    Dataset::write(reader(rows(&[p4], n, 1, 30_000_000)), uri.as_str(), Some(params(WriteMode::Append))).await?;
    let mut ds = Dataset::open(&uri).await?;
    ds.create_index(&["id"], IndexType::BTree, Some("id_btree".into()), &ScalarIndexParams::new("btree".into()), true)
        .await?;
    let before = ds.version().version;
    println!(
        "before: v{before}, {} rows in {} fragments (P1={}, P2={}, P3={}, P4={})",
        ds.count_rows(None).await?,
        ds.get_fragments().len(),
        count(&ds, &partitions_predicate(&[p1])).await?,
        count(&ds, &partitions_predicate(&[p2])).await?,
        count(&ds, &partitions_predicate(&[p3])).await?,
        count(&ds, &partitions_predicate(&[p4])).await?,
    );

    // Replace P1 with new rows (version 2), delete P2 (zero rows), all in one commit.
    let props = HashMap::from([
        ("svc.batch_id".to_string(), "20261022T040000Z-abc123".to_string()),
        ("svc.partitions".to_string(), r#"[{"key":"filial_id=1/fecha=2026-10-01","hash":"a1b2c3d4e5f60718"},{"key":"filial_id=1/fecha=2026-10-02","hash":"0"}]"#.to_string()),
    ]);
    let new_p1 = rows(&[p1], n / 2, 2, 40_000_000);
    let t = Instant::now();
    let ds = replace_partitions(Arc::new(ds), &[p1, p2], Some(new_p1), props).await.context("replace failed")?;
    println!("replace P1 (new rows) + delete P2 in {:.1}s -> v{}", t.elapsed().as_secs_f64(), ds.version().version);

    // Verify on a fresh handle.
    let ds = Dataset::open(&uri).await?;
    let after = ds.version().version;
    ensure!(after == before + 1, "expected exactly one commit, got v{before} -> v{after}");
    let (c1, c2, c3, c4) = (
        count(&ds, &partitions_predicate(&[p1])).await?,
        count(&ds, &partitions_predicate(&[p2])).await?,
        count(&ds, &partitions_predicate(&[p3])).await?,
        count(&ds, &partitions_predicate(&[p4])).await?,
    );
    println!("after: v{after}, P1={c1}, P2={c2}, P3={c3}, P4={c4}, fragments={}", ds.get_fragments().len());
    ensure!(c1 == (n / 2) as usize, "P1 should hold only the new rows");
    ensure!(count(&ds, "filial_id = 1 AND fecha = '2026-10-01' AND version = 1").await? == 0, "old P1 rows survived");
    ensure!(c2 == 0, "P2 should be deleted");
    ensure!(c3 == n as usize && c4 == n as usize, "P3/P4 must be untouched");

    // The old version is still readable (time travel) — readers mid-query are unaffected.
    let old = ds.checkout_version(before).await?;
    ensure!(count(&old, &partitions_predicate(&[p1])).await? == 2 * n as usize, "old version changed");

    // The commit carries the batch id: this is how a retry detects an already-applied batch.
    let txn = ds.read_transaction_by_version(after).await?.context("no transaction file")?;
    let props = txn.transaction_properties.context("no transaction properties")?;
    println!("commit properties: batch_id={}", props["svc.batch_id"]);
    ensure!(props["svc.batch_id"] == "20261022T040000Z-abc123", "batch id not stored");

    // Index still answers lookups for untouched and new rows.
    ensure!(ds.load_indices().await?.iter().any(|i| i.name == "id_btree"), "index lost");
    ensure!(count(&ds, "id = 10000005").await? == 0, "deleted P1 row still found by id");
    ensure!(count(&ds, &format!("id = {}", 10_000_000 + 1_000_000 + 5)).await? == 1, "P3 row not found by id");
    ensure!(count(&ds, "id = 40000003").await? == 1, "new P1 row not found by id");

    println!("\nPARTITION REPLACE SPIKE PASSED");
    Ok(())
}
