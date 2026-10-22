//! Spike A: create, append to and read a Lance dataset on an S3 Express One Zone
//! directory bucket, and check that Lance's conditional-put commit protocol is
//! actually enforced there (concurrent writers must not lose each other's data).
//!
//! Usage: spike-a
//! All settings (bucket, AWS profile, region, sizes) come from the repo's `config.toml`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{WriteDestination, WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use object_store::PutPayload;
use object_store::path::Path;
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    aws: AwsConfig,
    storage: StorageConfig,
    spikes: SpikesConfig,
}

#[derive(Deserialize)]
struct AwsConfig {
    profile: String,
    region: String,
}

#[derive(Deserialize)]
struct StorageConfig {
    bucket: String,
}

#[derive(Deserialize)]
struct SpikesConfig {
    a: SpikeAConfig,
}

#[derive(Deserialize)]
struct SpikeAConfig {
    prefix: String,
    rows_per_batch: i64,
    concurrent_writers: i64,
}

/// `$LANCE_CONFIG` if set, otherwise the first `config.toml` found walking up from the cwd.
fn config_path() -> Result<PathBuf> {
    if let Some(p) = std::env::var_os("LANCE_CONFIG") {
        return Ok(p.into());
    }
    let cwd = std::env::current_dir()?;
    cwd.ancestors()
        .map(|d| d.join("config.toml"))
        .find(|p| p.is_file())
        .ok_or_else(|| anyhow!("config.toml not found above {}", cwd.display()))
}

fn load_config() -> Result<Config> {
    let path = config_path()?;
    let text = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, true),
    ]))
}

/// One batch of rows with ids in `[start, start + rows)`.
fn batch(start: i64, rows: i64) -> RecordBatchIterator<std::vec::IntoIter<Result<RecordBatch, arrow_schema::ArrowError>>> {
    let ids: Vec<i64> = (start..start + rows).collect();
    let regions = ["PE", "CL", "MX", "US"];
    let region: Vec<&str> = ids.iter().map(|i| regions[(*i % 4) as usize]).collect();
    let amount: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let rb = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(region)),
            Arc::new(Int64Array::from(amount)),
        ],
    )
    .expect("valid batch");
    RecordBatchIterator::new(vec![Ok(rb)].into_iter(), schema())
}

fn params(mode: WriteMode) -> WriteParams {
    WriteParams {
        mode,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    }
}

macro_rules! timed {
    ($label:expr, $e:expr) => {{
        let t = Instant::now();
        let r = $e;
        println!("{:<44} {:>8.1} ms", $label, t.elapsed().as_secs_f64() * 1000.0);
        r
    }};
}

fn main() -> Result<()> {
    let cfg = load_config()?;
    // Lance resolves credentials and region through the default AWS chain, which reads
    // these variables. Set them before the runtime starts any threads.
    // SAFETY: still single-threaded here.
    unsafe {
        if !cfg.aws.profile.is_empty() {
            std::env::set_var("AWS_PROFILE", &cfg.aws.profile);
        }
        std::env::set_var("AWS_REGION", &cfg.aws.region);
    }
    tokio::runtime::Runtime::new()?.block_on(run(cfg))
}

async fn run(cfg: Config) -> Result<()> {
    let rows = cfg.spikes.a.rows_per_batch;
    let writers_n = cfg.spikes.a.concurrent_writers;
    ensure!(rows >= 100 && writers_n >= 1, "rows_per_batch must be >= 100 and concurrent_writers >= 1");
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let uri = format!("s3://{}/{}/run-{run}/sales.lance", cfg.storage.bucket, cfg.spikes.a.prefix);
    println!("dataset: {uri}  (profile={:?}, region={})\n", cfg.aws.profile, cfg.aws.region);

    // 1. Create.
    let ds = timed!(format!("create ({rows} rows, v2.2)"), Dataset::write(batch(0, rows), uri.as_str(), Some(params(WriteMode::Create))).await)
        .context("create failed")?;
    ensure!(ds.version().version == 1, "expected version 1 after create");

    // 2. Append.
    let ds = timed!(format!("append ({rows} rows)"), Dataset::write(batch(rows, rows), uri.as_str(), Some(params(WriteMode::Append))).await)
        .context("append failed")?;
    ensure!(ds.version().version == 2, "expected version 2 after append");

    // 3. Read back from a fresh handle.
    let ds = timed!("open (cold handle)", Dataset::open(&uri).await)?;
    let n = timed!("count_rows()", ds.count_rows(None).await)?;
    ensure!(n == 2 * rows as usize, "expected {} rows, got {n}", 2 * rows);
    let filtered = timed!("count_rows(id < 100)", ds.count_rows(Some("id < 100".into())).await)?;
    ensure!(filtered == 100, "expected 100 filtered rows, got {filtered}");
    let lo = rows * 3 / 2;
    let mut scan = ds.scan();
    scan.project(&["id", "amount"])?.filter(&format!("region = 'PE' AND id >= {lo}"))?;
    let batches: Vec<RecordBatch> = timed!(format!("scan(region='PE' AND id>={lo})"), scan.try_into_stream().await?.try_collect().await)?;
    let scanned: usize = batches.iter().map(|b| b.num_rows()).sum();
    let expected_scan = (lo..2 * rows).filter(|i| i % 4 == 0).count();
    ensure!(scanned == expected_scan, "expected {expected_scan} scanned rows, got {scanned}");

    // 4. Concurrent appends from handles that all start at the same version. Each
    //    writer races to create the same manifest file; with working put-if-absent
    //    the losers detect the conflict and rebase, so no rows are lost.
    let start_version = ds.version().version;
    let shared = Arc::new(ds);
    let writers = (0..writers_n).map(|w| {
        let dest = shared.clone();
        async move {
            Dataset::write(
                batch((2 + w) * rows, rows),
                WriteDestination::Dataset(dest),
                Some(params(WriteMode::Append)),
            )
            .await
        }
    });
    let results = timed!(format!("{writers_n} concurrent appends"), futures::future::join_all(writers).await);
    for (w, r) in results.iter().enumerate() {
        if let Err(e) = r {
            bail!("concurrent writer {w} failed: {e}");
        }
    }
    let ds = Dataset::open(&uri).await?;
    let expected = (2 + writers_n) as usize * rows as usize;
    let n = ds.count_rows(None).await?;
    let end_version = ds.version().version;
    println!("  versions {start_version} -> {end_version}, rows {n} (expected {expected})");
    ensure!(n == expected, "LOST WRITES: expected {expected} rows, got {n}");
    ensure!(end_version == start_version + writers_n as u64, "expected one version per writer");

    // 5. Raw put-if-absent on the bucket: the second create must be rejected.
    let store = ds.object_store(None).await?;
    let probe = Path::from(format!("{}/run-{run}/sales.lance/_probe", cfg.spikes.a.prefix));
    timed!("put_if_absent #1", store.put_if_absent(&probe, PutPayload::from_static(b"one")).await)?;
    match timed!("put_if_absent #2 (must fail)", store.put_if_absent(&probe, PutPayload::from_static(b"two")).await) {
        Err(object_store::Error::AlreadyExists { .. }) => println!("  second create rejected with AlreadyExists"),
        Err(e) => bail!("unexpected error on second create: {e}"),
        Ok(()) => bail!("second create SUCCEEDED: put-if-absent is not enforced"),
    }

    println!("\nversions:");
    for v in ds.versions().await? {
        println!("  v{} at {}", v.version, v.timestamp);
    }
    println!("\nSPIKE A PASSED");
    Ok(())
}
