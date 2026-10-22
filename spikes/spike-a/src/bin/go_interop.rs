//! Interop check: read an Arrow IPC stream written by the Go client (lancego,
//! Zstd-compressed buffers), write it to a local Lance dataset (v2.2) and query it.
//!
//! Usage: go_interop [path/to/sales.arrows]   (default: lancego/testdata/sales.arrows
//! found by walking up from the cwd)

use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, ensure};
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchIterator;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;

fn input_path() -> Result<PathBuf> {
    if let Some(p) = std::env::args().nth(1) {
        return Ok(p.into());
    }
    let cwd = std::env::current_dir()?;
    cwd.ancestors()
        .map(|d| d.join("lancego/testdata/sales.arrows"))
        .find(|p| p.is_file())
        .ok_or_else(|| anyhow!("lancego/testdata/sales.arrows not found; run the Go test with LANCEGO_WRITE_TESTDATA=1"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let path = input_path()?;
    let reader = StreamReader::try_new(File::open(&path)?, None).context("opening IPC stream")?;
    let schema = reader.schema();
    println!("schema from Go ({}):", path.display());
    for f in schema.fields() {
        println!("  {:<12} {:<40} nullable={}", f.name(), f.data_type().to_string(), f.is_nullable());
    }
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().context("reading batches")?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("read {} batches, {rows} rows", batches.len());

    let dir = std::env::temp_dir().join(format!("go-interop-{}", std::process::id()));
    let uri = dir.join("sales.lance").to_string_lossy().into_owned();
    let params = WriteParams {
        mode: WriteMode::Create,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let ds = Dataset::write(iter, uri.as_str(), Some(params)).await.context("writing Lance")?;
    ensure!(ds.count_rows(None).await? == rows, "row count mismatch after write");
    ensure!(ds.schema().fields.len() == schema.fields().len(), "column count mismatch");

    let checks = [
        ("region = 'PE'", rows / 3),
        ("amount IS NULL", rows / 3),
        ("paid = true", 2 * rows / 3),
        ("ship.city = 'Lima'", rows / 3),
        ("ts > TIMESTAMP '2026-10-21 12:30:00'", 2 * rows / 3),
    ];
    for (filter, expected) in checks {
        let n = ds.count_rows(Some(filter.to_string())).await.with_context(|| format!("filter {filter}"))?;
        println!("  count_rows({filter:<40}) = {n:>5} (expected {expected})");
        ensure!(n == expected, "filter {filter}: expected {expected}, got {n}");
    }
    std::fs::remove_dir_all(&dir).ok();
    println!("\nGO INTEROP PASSED");
    Ok(())
}
