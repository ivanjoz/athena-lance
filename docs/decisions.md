# Decisions (ADRs)

## ADR-001: AWS account, region and data bucket — 2026-10-21

- Account `<aws-account-id>` (Hortifrut), CLI profile `<aws-cli-profile>` (temporary MFA session credentials
  that must be refreshed when they expire), region `us-west-2` (Oregon).
- Data bucket: `smartberry-lance-data--usw2-az1--x-s3`, an S3 Express One Zone directory bucket in AZ ID
  `usw2-az1`. This account only allows bucket names starting with `smartberry`; `lance-data--…` was denied.
  Zone names differ between accounts, so always refer to the AZ ID.
- Superseded the same day: a first bucket `lance-data--use1-az4--x-s3` in account `<personal-aws-account-id>`
  (us-east-1, default profile). It's no longer used.
- All of these values live in `config.toml` at the repo root, the single source of truth. Code must not
  hardcode them. Programs find the file through `$LANCE_CONFIG` or by walking up from the cwd.

## ADR-002: No VPC for the MVP — 2026-10-21

All three Lambdas run outside a VPC and reach the bucket through its public zonal endpoint
(`s3express-usw2-az1.us-west-2.amazonaws.com`). This replaces §2 decision 1–2 and the VPC and endpoint
items of §9 in the plan.

- **Trade-off:** Lambda chooses the AZ, so most invocations run cross-AZ from the bucket, adding about 1–2 ms per S3 request.
- **Later:** if read latency needs it, move only `lance-query` into a `usw2-az1` subnet with the free
  S3 Express gateway endpoint. `lance-ingest` (fed by an SQS event source mapping) and `lance-control`
  can stay outside the VPC, so no SQS interface endpoint or NAT is needed.

## ADR-003: Version pins — 2026-10-21

| Crate | Version | Note |
|---|---|---|
| `lance`, `lance-*` | `=13.0.0` | latest stable at the time of writing |
| `datafusion` | `54.x` | the version `lance 13.0.0` depends on (`^54.0.0`); **not** the latest 55.x |
| `arrow` / `arrow-*` | `58.x` | the version `lance 13.0.0` depends on |
| `object_store` | `0.14.x` | the version `lance 13.0.0` depends on |

Toolchain: Rust 1.98.1, `protoc` 36.2, `cargo-lambda` 1.9.2, Zig (system).

Upgrade `lance` and `datafusion` together, never independently.

## ADR-004: Spike A — Lance on S3 Express One Zone — 2026-10-21

**Result: PASSED.** Code: `spikes/spike-a`. Ran from a workstation outside AWS, so the timings below
include the internet round trip and are **not** representative of Lambda latency.

Verified with Lance 13.0.0 and file format v2.2, first on `lance-data--use1-az4--x-s3` (us-east-1) and
then, with identical results, on `smartberry-lance-data--usw2-az1--x-s3` (us-west-2, profile
`<aws-cli-profile>` with MFA session credentials):

1. **S3 Express detection is automatic.** `lance-io` sets object_store's `S3Express` flag when the bucket
   name ends with `--x-s3` (it can also be forced with the storage option `s3_express=true`). The store then
   uses `CreateSession` auth and the zonal endpoint. No extra configuration was needed; credentials and
   region come from the default AWS chain.
2. **Commits are safe.** For `s3://` URIs Lance uses `ConditionalPutCommitHandler`, which writes each
   manifest with `PutMode::Create` (`If-None-Match: *`). S3 Express enforces it: a raw second
   create of the same key failed with `AlreadyExists`.
3. **Concurrent writers don't lose data.** 4 appends started from the same version all succeeded
   (losers rebased), producing versions 3–6 and exactly 60,000 rows. No lock service or DynamoDB
   commit store is needed. FIFO per-table serialization stays in place to avoid wasted retries, not for
   correctness.
4. Create, append, cold open, `count_rows`, filtered `count_rows` and filtered projected scans all
   returned correct results.
5. S3 Express lists keys in no particular order; Lance 13 handles this (`list_is_lexically_ordered = false`).

Workstation timings (indicative only), us-east-1 / us-west-2: create 2.9 s / 4.7 s, append 2.5 s / 4.1 s,
cold open 1.0 s / 1.6 s, filtered count 0.5 s / 0.7 s, filtered scan 0.2 s / 0.5 s,
single conditional PUT ~105 ms / ~175 ms.

**Still open from Phase 0:** Spike B (scalar index on an empty table), Spike C (table metadata and
transaction properties), ledger choice, staging bucket type. Real latency must be measured from a Lambda in us-west-2.

## ADR-005: Staging area and Go client — 2026-10-21

- **Staging lives in the data bucket**, under `storage.staging_prefix` (`staging/`), separate from
  `tables/`. This closes the plan's open "staging bucket type" item: no second bucket, and the
  catalog only lists `tables/`, so staged files are never mistaken for tables.
- **Go client: `lancego/`** (module `github.com/ivanjoz/athena-lance/lancego`). Converts `[]T` to Arrow
  from `lance:"name|*,index=…,sort=…,compress=…"` tags. Its wire types in `lancego/protocol.go`
  are the contract the Rust service must implement (plan §4).
- **`lance:"*"` converts the Go field name to snake_case** (`CustomerID` → `customer_id`). All column
  names must match `^[a-z][a-z0-9_]{0,62}$`, checked in the client as well as the service. Lowercase
  names never need quoting in DataFusion SQL; mixed-case names would (verified in the interop check).
- Upload from Go with aws-sdk-go-v2 to the S3 Express bucket works (CreateSession handled by the SDK).

## ADR-006: Compaction (`compact` op) — 2026-10-21

- **Request** (Go `Table.Compact` → SQS FIFO, group = table):
  `{"op":"compact","table":…,"operation_id":…,"max_file_mb":50,"max_files":20,"max_tasks":10}`.
  Zero/omitted limits use `[compaction]` in config.toml.
- **Rule:** adjacent fragments are grouped; a group closes when the next fragment would exceed
  `max_file_mb` OR `max_files`, so both limits hold. Fragments at or above the size limit, or with an
  unknown size, are boundaries and never rewritten. Single-fragment groups are skipped. At most
  `max_tasks` groups per run; the report says whether more remain.
- **Implementation:** a custom Lance `CompactionPlanner` (`spikes/spike-a/src/compaction.rs`, moves to
  the service in Phase 2). Lance does the rewrite, index remap and commit. `target_rows_per_fragment` is
  set above any group so each group becomes exactly one file; `max_bytes_per_file` is the hard cap.
  Fragment sizes come from the manifest (`file_size_bytes`), so planning makes no S3 requests.
- *(Superseded by ADR-008: compaction now holds the per-table lease lock, as there is no SQS.)*
- **Concurrency: no lock.** Compaction goes through the table's FIFO group, so it never overlaps an
  ingest of the same table. As a safety net, Lance's conflict resolver treats Rewrite vs Append as
  compatible (rebased automatically) and Rewrite vs Delete/Update/Rewrite on the same fragments as a
  retryable conflict, so data is never lost. Readers keep using the old version, and old files are only removed by
  `cleanup_old_versions` after a retention period.
- **A lock is possible if ever needed:** S3 Express supports `If-None-Match: *` create, `If-Match`
  overwrite and `If-Match` delete (all verified on our bucket on 2026-10-21), enough for a lease lock.
- **Verified end to end** on S3 Express: 10 fragments (~120 KB) with limits 1 MB / 4 files → plan
  `[0..4, 4..8, 8..10]` → 3 files, while a concurrent append committed. 55,000 rows and sum preserved,
  every file under the limit, BTREE index remapped and point lookups correct.

## ADR-007: Partitions, partition registry and write confirmation — 2026-10-22

Modeled on the smartberry datalake (`backend-golang/aws/glue.go`, `parquet.go`): a table is declared
like a `GlueTable`, rows are grouped by `PartitionKeys`, each partition has a hash, unchanged partitions are
skipped, and the ledger records hash + `parquet_id` + `records_updated` per partition.

- **A partition is logical, not a file.** One Arrow file sent by Go is one *batch* and may hold
  many partitions. Lance stores it as fragments, and compaction later merges fragments across batches.
  Partitions are identified by the values of the `PartitionKeys` columns, keyed as
  `"filial_id=1/fecha=2026-10-01"` (Hive style, values URL-path-escaped).
- **Go API** (`lancego`): `TableDef{Name, PartitionKeys, SortKey, Indexes}` + `NewTable[T]`.
  `Table.Save(rows)` groups by partition, hashes each (Table.HashFunc, default `DefaultHash`: an
  order-independent sum of per-row FNV-1a 64), reads the registry, skips equal hashes, and sends the
  rest as `replace_partitions` requests (≤ `ingest.max_rows_per_request` rows; a partition is never
  split). It then waits for each batch to be committed. `DeletePartitions`, `Partitions` and `Wait` complete the API.
  Optional `GetParquetID() int64` on the row type → partition ID; `SaveOptions.RecordsUpdated` →
  stored as is, like `GlueTable.RecordsUpdatedByParquetID`. Hash on the wire: 16 hex digits.
- **Replace = one atomic Lance commit**: an uncommitted delete of the partitions' rows plus an
  uncommitted insert of the new fragments, committed as one `Operation::Update`. Fully covered fragments
  are dropped, partially covered ones get deletion files, and no reader sees a partition missing or
  duplicated. A partition with 0 rows is a delete. Verified on S3 Express
  (`spikes/spike-a/src/bin/partition_replace.rs`): v5 → v6 in one commit, other partitions untouched,
  old version still readable, BTREE index correct.
- **Registry and batch status: DynamoDB `smartberry-lance-ledger`** (on-demand, us-west-2, created
  2026-10-22). Keys are `pk = <table>` with `sk = "p#<partition key>"` → {values, id, hash, records_updated, rows,
  committed_at (unix s), version, batch_id}, or `sk = "b#<batch_id>"` → {status pending|committed|failed,
  version, committed_at, error}.
- **Consistency between Lance and DynamoDB:** the Lance commit carries `svc.batch_id` and the partition
  hashes in its transaction properties, so it is the source of truth. DynamoDB is written after it. If the
  Lambda dies in between, SQS redelivers. The service finds `svc.batch_id` in the recent
  transactions, skips the write and only completes DynamoDB.
- **Confirmation:** Go waits by polling `get_batch` (250 ms, doubling up to 2 s, timeout
  `ingest.wait_timeout_seconds`), keeping the SQS FIFO queue (per-table ordering, retries, DLQ). The
  confirmed `version` lets queries require a minimum version per table.
- Partitioned tables reject `Insert` (append would duplicate partitions); use `Save`.
- *Superseded in part by ADR-008: no SQS; writes are synchronous invocations under a table lock.*

## ADR-008: No SQS — synchronous writes under a per-table lease lock; the service crate — 2026-10-22

- **Why:** in account <aws-account-id> our user cannot create SQS queues, CloudFormation stacks or IAM roles.
  Functions must be named `smartberry*` and use the shared role `<lambda-role>`.
  That role's policies grant `s3:*Object`, DynamoDB and `lambda:Invoke*` on `smartberry*`, but not
  `s3express:CreateSession`, so our bucket has a bucket policy granting it to that role (`deploy/setup.sh`).
- **Writes** (`ingest`, `replace_partitions`, `compact`) are synchronous `Invoke`s of
  `smartberry-lance-ingest` (timeout 900 s). The response arrives after the commit, so it is the
  confirmation; `get_batch` polling remains only for `SaveOptions.NoWait` (async `Event` invoke).
- **Per-table serialization:** a lease lock `s3://<bucket>/locks/<table>.json` (`service/src/lock.rs`).
  It is acquired with `If-None-Match` create, or by an `If-Match` takeover of an expired or released lease.
  It is renewed every lease/3 with `If-Match`. `lock.check()` runs right before the commit (`LOCK_LOST`)
  and release sets `expires_at = 0`. A writer waits up to `lock_wait_seconds`, then gets `TABLE_BUSY`
  with the holder in `details`. Lance's conditional commit still protects data if a lease is lost.
- **Idempotency:** `batch_id` / `operation_id` in the ledger. A `committed` batch is returned with
  `already_applied`. For a `pending` batch, `svc.batch_id` is looked up in the last 50 transactions, and
  if found only the ledger is completed. The Go client retries unknown outcomes (transport, `INTERNAL`,
  `LOCK_LOST`) with the same ID.
- **Service crate** `service/` (lib + `bootstrap` + `lance-cli`). It runs the same handler locally
  against the real bucket and ledger; Go uses it when `LANCEGO_CLI=<path to lance-cli>`. Both functions
  use the same binary. The write path:
  - validate the staged IPC against the table schema (`SCHEMA_MISMATCH`);
  - sort by `sort_key`;
  - check every row belongs to a declared partition and every partition's row count matches
    (`PARTITION_MISMATCH`);
  - delete + insert in one `Operation::Update` commit;
  - build missing spec indexes or `optimize_indices`;
  - write the partition registry and the committed batch.
- **`table_info`** (and `describe_table` = without detail) reports:
  - version, rows and deleted rows;
  - files: count, size distribution in buckets, unknown sizes;
  - storage: objects under `data/` not referenced by the current version;
  - versions eligible for cleanup;
  - index coverage (fragments not indexed, segments) and missing spec indexes;
  - the compaction preview with the default limits;
  - the registry summary;
  - the lock and the last 20 batches (pending / failed counts).
  - `detail=true` adds, per fragment, the files, rows, deletions and min/max of the partition and sort
    columns, plus overlapping ranges per column (pruning quality) and how many fragments each
    partition spans.
- **`compact`** also runs `cleanup_old_versions(version_retention_hours)`, which is what deletes
  replaced files.
- **Verified 2026-10-22 with `lance-cli` against S3 Express + DynamoDB** (Go example `examples/sales`):
  - create_table: 4 indexes built on the first write.
  - Save of 6 partitions / 3000 rows: one commit.
  - Unchanged re-save: 0 sent, 0.7 s.
  - One changed row: only its partition was replaced (`rows_deleted` 500).
  - Delete of a partition: a fully deleted fragment was dropped.
  - compact: 4 fragments became 1, deletions materialized, indexes remapped, 3000 rows.
  - A re-sent committed `batch_id` returned `already_applied`.
  - A held lock gave `TABLE_BUSY` with the holder; an expired lock was taken over.
  - Two concurrent Saves were both committed, serialized by the lock.
  - Version history checked independently with pylance.
- **Target:** AWS Lambda arm64 (Graviton), `provided.al2023`, built with
  `cargo lambda build --profile lambda --arm64` (`deploy/deploy.sh`).
- **Deployed 2026-10-22:** `smartberry-lance-control` (1024 MB, 120 s) and `smartberry-lance-ingest`
  (3008 MB, 900 s, 2 GB /tmp), with the same zip.
  - The binary is 139 MB and the zip 56 MB, over the 50 MB direct-upload limit, so the code is
    uploaded to `s3://smartberry-artifacts/lance-lambda/function.zip` (`lambda.code_bucket` /
    `code_key`).
  - Go → Lambda end to end:
    - table_info 2.0 s, including a cold start (init 260 ms).
    - Save of 3 partitions: 3.4 s client side; the commit invocation takes 0.8–1.1 s and 114 MB.
    - compact: 3 fragments became 1.
  - The bucket policy for `s3express:CreateSession` was enough; no IAM change was needed.

## ADR-009: Query (`query` / `explain` ops, `smartberry-lance-query`) — 2026-10-22

- **Function:** `smartberry-lance-query` (3008 MB, 30 s), same zip and binary as the other two,
  deployed by `deploy/deploy.sh`. Limits and defaults live in `[query]` in config.toml.
- **Dynamic catalog without a SchemaProvider:** each request parses the SQL, takes the table names
  it references (`resolve_table_references`, CTEs excluded) and registers each table in a fresh
  `SessionContext` as a `LanceTableProvider` (unordered scans). Datasets are cached per process
  (shared Lance session: index and metadata caches stay warm), so a table created at runtime is
  queryable with no redeploy, and the response can report the exact version read per table.
  Only bare names (or `public.<name>`) matching the table-name rule are accepted, so SQL can never
  name a path, another catalog or `information_schema`.
- **Freshness:** a cached table is checked (`latest_version_id`) at most every
  `query.freshness_seconds`; when it moved, `checkout_latest`. `min_versions: {table: v}` forces the
  check, giving read-your-writes with the version `Save` confirmed (`SaveResult.Version()`), or
  `VERSION_NOT_AVAILABLE`.
- **Read-only:** `SQLOptions` without DDL, DML or statements (`SET`), so only queries plan.
  Errors in the SQL → `SQL_ERROR`; memory pool exhausted → `RESOURCES_EXHAUSTED`; timeout →
  `QUERY_TIMEOUT`.
- **Limits:** `max_rows` is pushed as `LIMIT max_rows+1` (the extra row only detects truncation)
  and rows stop being added once the JSON reaches `query.max_response_bytes` (sync Lambda responses
  are capped at 6 MB); both set `truncated`. The DataFusion memory pool is `memory_fraction` of
  `AWS_LAMBDA_FUNCTION_MEMORY_SIZE`; sorts and joins spill to /tmp beyond it.
- **Response:** `columns` (name, Arrow type, nullable), `rows` (arrow-json with explicit nulls;
  timestamps RFC 3339 with the zone, binary as hex), `row_count`, `truncated`, `stats`
  (elapsed / plan / exec ms, `bytes_read` and `get_requests` from a Lance `IOTracker` wrapped
  around the query datasets' object store, `cold_start`, `dataset_versions`). `explain` runs
  `EXPLAIN ANALYZE` and returns the text in `plan`.
- **Go:** `Client.Query`, `Client.Explain(ctx, sql, opts)`, `Table[T].Query` → `[]T`, and
  `DecodeRows[R]` for any lance-tagged struct (aggregates).
- **Verified locally** (unit tests on a local Lance table with a BTREE index): JSON rows and
  aggregates, truncation by rows and by bytes, DDL/DML/SET and qualified names rejected, and
  EXPLAIN ANALYZE of `WHERE id = 42` showing `ScalarIndexQuery ... @id_btree(BTree)` under the
  `LanceRead` (the filter is answered by the index: 1 row scanned).
- **Verified with `lance-cli` against S3 Express** (table `ventas`, version 30, from a workstation,
  so every GET includes the internet round trip, ~175 ms):
  - group by region: 4 rows, 6 GETs, ~100 KB read; cold 2.8 s (1.8 s opening the dataset).
  - `WHERE id = 42` warm: `ScalarIndexQuery @id_btree`, 1 GET, ~180 ms.
  - `max_rows: 3` → 3 rows, `truncated`; `DELETE` → `SQL_ERROR`; unknown table → `TABLE_NOT_FOUND`.
  - Go `examples/sales -query -explain` through `LANCEGO_CLI`: aggregate decoded with `DecodeRows`,
    `SELECT *` decoded into `[]Venta` (nullable float, UTC timestamps), plan printed.
- **Not yet deployed:** `smartberry-lance-query` does not exist yet; Lambda latency is unmeasured.
- **Superseded by ADR-011:** queries run on `smartberry-lance-ingest`; there is no query function.

## ADR-010: The Go library takes its configuration from the backend — 2026-10-22

There are three actors: the **backend** (the application consuming this project), the **Go
library** `lancego` (the bridge that stages files and invokes the functions) and the **Rust
service** on Lambda. The configuration belongs to the backend, which keeps it wherever it wants.

- `lancego` reads no files or environment variables. `LoadConfig`, the `$LANCE_CONFIG` lookup and
  `LANCEGO_CLI` / `LANCEGO_CLI_LOGS` were removed from the library.
- `lancego.Config` is a flat struct: `ControlFunction`, `IngestFunction`, `QueryFunction` and
  `Bucket` are required (`QueryFunction` removed in ADR-011); `Region` / `Profile` (used by `New`), `StagingPrefix`, `BatchRows`,
  `MaxRowsPerRequest`, `WaitTimeout` have defaults; `LocalCLI` / `LocalCLILogs` replace the env
  vars for development. `New` and `NewWithAWS` validate it and return an error naming every
  missing field (`NewWithAWS` now returns `(*Client, error)`).
- `config.toml` stays the configuration of the Rust service, the deploy scripts and the examples.
  `examples/sales` plays the backend: it reads `config.toml` (and `LANCEGO_CLI`) itself and
  builds the `Config`. The TOML parser remains in `go.mod` only because the example lives in the
  same module; the `lancego` package does not import it.

## ADR-011: Two functions; queries run on lance-ingest — 2026-10-22

- **Functions:** `smartberry-lance-control` (1024 MB, 120 s: metadata ops) and
  `smartberry-lance-ingest` (4096 MB, 900 s, 2 GB /tmp: writes, compaction, `query` / `explain`).
  `smartberry-lance-query` (ADR-009) was never deployed and is dropped.
- **Why:** both data paths want the same thing, memory (= CPU) and /tmp to spill to. A third
  function only added a deploy target and a config field. The one thing the query function gave,
  a short timeout, does not need a function: the service already cuts each query with
  `tokio::time::timeout` (`ops/query.rs`), so `query.default_timeout_ms` = `max_timeout_ms` =
  30000 is the query limit, independent of the 900 s that writes need.
- **Memory:** 3008 → 4096 MB. The DataFusion pool is `memory_fraction` of the function memory.
  Queries and writes run in separate invocations (separate containers), so they do not share it.
- **Go:** `lancego.Config.QueryFunction` is removed; `Client.Query` / `Explain` invoke
  `IngestFunction`. `ControlFunction`, `IngestFunction` and `Bucket` are the required fields.
- **Trade-off accepted:** queries and writes share the function's concurrency and its logs. If
  queries ever need their own concurrency limit or provisioned concurrency, a second function
  name pointing at the same zip is a config change again.

## ADR-012: One file per partition; partition keys are indexed — 2026-10-22

Plan and measurements: `docs/plan-archivo-por-particion.md`. **Not compiled or deployed yet.**

- **Problem:** the first real load (smartberry weather, ~10.9 M rows) left one file per *write* (one per
  ubicación, all its years), and a query for one ubicación and one day opened all 163 files (73 MB,
  ~385 ms) because Lance 13 skips files only through scalar indexes and the tables had none.
- **Write:** `replace_partitions` splits the sorted batch by partition key and writes each group with
  its own `InsertBuilder`; all files go into the one `Update` commit (still atomic). A partition is one
  file, and replacing it removes the old file whole (no deletion files). `ingest` is unchanged.
- **Spec:** `validate` adds a BITMAP index on every partition key that has no BTREE/BITMAP declared.
  `ensure_indexes` builds it on the first write and refreshes it on every write.
- **Compaction:** partitioned tables are never merged (it would mix partitions); `compact` only cleans up
  old versions for them and `table_info` no longer recommends it.
- **Replace:** `create_table` with `replace: true` overwrites an existing table with an empty version
  (`WriteMode::Overwrite`, which also drops its indexes) and deletes its partition registry. Overwrite
  rather than deleting the prefix keeps versions increasing, so the query engine's dataset cache never
  sees an old version number with new data. `lancego.CreateOptions.Replace`.
- **Cost accepted:** more, smaller files (1289 per weather table instead of 163): full-table scans do
  more GETs. The screens always read one ubicación.

## ADR-013: Query result cache in the data bucket — 2026-10-22

**Not compiled or deployed yet.**

- **Where:** `s3://<bucket>/<query.cache_prefix>/query/<sha256>.json` in the S3 Express bucket, so
  every container of lance-ingest shares it (an in-memory cache would be per container and lost on a
  cold start). One object per key, overwritten when the query runs again.
- **Key:** SHA-256 of a format version, the resolved max_rows, `query.max_response_bytes` and the SQL
  text as sent (no normalization: different whitespace is a different entry). The SQL is stored in
  the entry and compared on read.
- **Freshness by age, chosen per request:** `cache_seconds` 0 = `query.cache_default_seconds` (300),
  negative = off (not read, not written), > 0 = that age capped at `query.cache_max_seconds` (86400).
  A write does not invalidate entries (that would need a per-table index of entries); a request with
  `min_versions` skips an entry computed at an older version, so read-your-writes still holds.
- **Scope:** the `query` op only. `explain` always runs (it is for looking at a run). The Trino API
  does not use it.
- **Never fails a query:** a read or write error is logged and the query runs as if there were no entry.
  Errors and timeouts are not cached; truncated results are (the same request truncates the same way).
- **Cleanup:** a bucket lifecycle rule expires the prefix after 1 day (`deploy/setup.sh`).
- **Cost accepted:** a miss costs one GET (404) before running and one PUT after it (up to 5 MB).

