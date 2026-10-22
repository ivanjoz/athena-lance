# Plan: Schema-Driven Lance Service on AWS Lambda (Amazon Linux 2023) + S3 Express One Zone

> **Audience:** an engineering agent starting implementation from zero.
> **Status:** greenfield. Verify every item marked **[VERIFY]** against current docs/crate versions before relying on it — the Lance, DataFusion and AWS SDK ecosystems move fast.

---

## 1. Goal

Build a **serverless, schema-driven analytical data service** in Rust. Its main traits:

- **No hardcoded tables.** No table, schema or index is declared in code. Tables are **created on request** from a separate **Go backend**, which sends the schema and table spec.
- **The service owns the schemas.** Once a table is created, its schema lives in the **Lance manifest**, and the service enforces it on every ingest. The Go backend *requests* tables and schema changes; the service validates and applies them.
- **It does three jobs:**
  1. **Control**: create tables, add indexes, evolve schemas, describe/list tables.
  2. **Ingest**: load data sent by the Go backend (**Arrow IPC** or **CSV + schema sidecar**) into Lance tables.
  3. **Query**: serve read-only SQL (`SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, joins) through **Apache DataFusion**, with tables discovered dynamically.
- **Storage:** Lance datasets (file format **2.2 or newer**) in an **S3 Express One Zone directory bucket**.
- **Compute:** AWS Lambda, runtime **`provided.al2023`**, **arm64 (Graviton)**, deployed as a **zip** (no container image). Layers may be used if needed.
- **Priority:** **read latency and bytes read** are the primary metrics. Write/ingest cost is not a priority. Spend write-side effort freely (sorting, re-encoding, index building, compaction, re-clustering) whenever it makes reads faster.

### Non-goals (v1)
- No container images, ECS/EKS, or long-running servers.
- No vector search (Lance supports it; can be added later).
- No DuckDB inside the Lambda (may be benchmarked separately later).
- No upserts/deletes in v1: append-only ingest. Upserts via Lance `merge_insert` are a v2 candidate (§13).

---

## 2. Architecture

One Rust codebase, **one binary (`bootstrap`)**, routing requests by an `op` field. It is deployed as **three Lambda functions** from the same zip, so each gets its own IAM role, memory, timeout and concurrency settings:

| Function | Invocation | Operations | Concurrency |
|---|---|---|---|
| `lance-control` | Synchronous (Go backend → Function URL with IAM auth, or `Invoke`) | `create_table`, `describe_table`, `list_tables`, `add_index`, `add_columns`, `get_upload_target` | Low |
| `lance-ingest` | Asynchronous via **SQS FIFO**, `MessageGroupId = table name` | `ingest`, `optimize` | Effectively **one writer per table** (FIFO group ordering) |
| `lance-query` | Synchronous (Function URL / API Gateway) | `query`, `explain` | High; read-only |

A scheduled job (EventBridge → `lance-ingest`, op `maintain`) runs compaction, index optimization and optional re-clustering per table.

```
 Go backend
   │ 1. create_table (schema + spec)  ─────────────►  lance-control ──► creates s3://<express-bucket>/tables/<t>.lance
   │                                                                  (schema + spec stored in Lance manifest)
   │ 2. upload data file(s) ─────────────────────►  Staging bucket (S3)  staging/<t>/<batch_id>.arrows | .csv (+ .schema.arrows)
   │ 3. ingest request ──► SQS FIFO (group=<t>) ──►  lance-ingest ──► validate → sort → append → optimize_indices
   │                                                                   └► ingest ledger (idempotency)
   │ 4. queries ─────────────────────────────────►  lance-query  ──► DataFusion + dynamic Lance catalog ──► S3 Express
   ▼
 All functions VPC-attached in the SAME AZ as the S3 Express directory bucket.
```

Key decisions:
1. **Same AZ** for all functions and the directory bucket. S3 Express One Zone is zonal; cross-AZ access erases much of its latency benefit.
2. **Gateway VPC endpoint for S3 Express** (and one for S3 Standard if staging is a standard bucket) **[VERIFY]**, so no NAT is needed. SQS access from the VPC needs an **interface endpoint** (or NAT).
3. **Data never travels in the Lambda payload.** Lambda payload limits are small (sync ~6 MB, SQS/async much smaller) **[VERIFY]**. Data goes through the **staging bucket**; requests carry only S3 URIs.
4. **Per-table serialized writes** through SQS FIFO message groups. This avoids Lance commit conflicts without a lock service.
5. **Single source of truth for schemas = the Lance manifest.** No schema in code, no separate schema database for v1.

---

## 3. Schema & Table Spec Model

### 3.1 What the Go backend sends
A **table spec** with two parts:

1. **Arrow schema, canonical form:** a **schema-only Arrow IPC stream** (zero record batches), base64-encoded in the request (`schema_ipc_b64`). This preserves exact types (timestamp unit/timezone, decimal precision/scale, nested types), nullability and field metadata. arrow-go and arrow-rs both produce and consume it natively.
2. **Layout spec**, read-performance settings decided by the requester:
   - `sort_key`: ordered list of columns, e.g. `["col_1", "col_2", "ts"]`.
   - `indexes`: list of `{column, type, name?}` with type ∈ `BTREE | BITMAP | ZONEMAP | BLOOMFILTER | LABEL_LIST | NGRAM | INVERTED`.
   - `storage_version`: default `"2.2"`.
   - `field_compression` (optional): per-column hints, e.g. `{"description": "zstd"}`.

### 3.2 Where it is stored (service-owned)
- **Schema:** in the Lance manifest (automatic on dataset creation).
- **Layout spec:** in **Lance table metadata/config** under a service prefix (e.g. `svc.sort_key`, `svc.spec_version`). Keys prefixed `lance.` are reserved by Lance. **[VERIFY]** the API for reading/writing table metadata in the pinned version. If it is unavailable, fall back to `tables/<t>.lance/_svc/spec.json`, written *before* the first commit and treated as immutable per spec version.
- **Indexes:** in the manifest (Lance index metadata). The spec records the *intended* indexes; `describe_table` reports both intended and actual (with fragment coverage).

### 3.3 Validation rules (enforced by the service)
- Table and column names match `^[a-z][a-z0-9_]{0,62}$` (configurable); no reserved words.
- Allowed Arrow types: an explicit allowlist (ints, floats, decimal128, utf8/large_utf8, binary, bool, date32, timestamp[us/ms/ns, tz], list/struct of allowed types). Reject unknown or extension types in v1.
- Every `sort_key` column and every index column exists, with an index type compatible with the column type (e.g. LABEL_LIST only on lists, INVERTED/NGRAM only on strings).
- **Index names are always explicit:** default `<column>_<type>`, lowercase. Reject a duplicate name, and reject a second index of the same *filter class* on the same column (BTREE vs BITMAP), since it adds build cost with no read gain.
- Max columns, max indexes per table, and max string sizes are configurable limits.

### 3.4 Schema evolution (`add_columns`)
| Change requested | Action |
|---|---|
| New **nullable** column(s) | Allowed: Lance schema evolution, no rewrite of existing data |
| New non-nullable column | Rejected unless a default value is provided **[VERIFY]** support |
| Type change / drop / rename | **Rejected in v1** (explicit error); handled later via a migration op |
| Ingest file whose schema ≠ table schema | Rejected, with a diff in the error message. Exception: missing nullable columns may be null-filled if `allow_missing_nullable=true` in the spec |

---

## 4. API Contracts

All requests are JSON with an `op` field. All responses include `request_id`, `ok`, and either a `result` or an `error {code, message, details}`.

### 4.1 `create_table` (lance-control)
```json
{
  "op": "create_table",
  "table": "sales",
  "schema_ipc_b64": "<base64 schema-only Arrow IPC stream>",
  "sort_key": ["col_1", "col_2", "ts"],
  "indexes": [
    {"column": "col_1", "type": "ZONEMAP"},
    {"column": "col_2", "type": "BITMAP"},
    {"column": "customer_id", "type": "BTREE"}
  ],
  "storage_version": "2.2",
  "field_compression": {"description": "zstd"},
  "if_not_exists": true
}
```
Behavior: validate (§3.3) → create an empty Lance dataset with the schema at `tables/<table>.lance` → store the spec → return the table descriptor. Indexes on an empty table are either created immediately or deferred to the first ingest **[VERIFY]** whether empty-table scalar index creation is supported; if not, record them as `pending` and build after the first ingest.

Idempotency: if the table exists with an **identical** spec and `if_not_exists=true`, return `ok` with `created=false`. If it exists with a different spec, return error `TABLE_EXISTS_SPEC_MISMATCH`.

### 4.2 `describe_table` / `list_tables` (lance-control)
Returns the schema (as both `schema_ipc_b64` and a readable JSON rendering), spec, current version, row count, fragment count, and indexes with coverage (indexed vs unindexed fragments).

### 4.3 `add_index` / `add_columns` (lance-control → enqueued)
Validated synchronously, then **enqueued to the table's FIFO group**, so they never race with ingests. They return an `operation_id`; the Go backend polls `describe_table` or an `get_operation` op.

### 4.4 `get_upload_target` (lance-control)
Returns the staging prefix for a new batch: `staging/<table>/<batch_id>/`. Optionally returns presigned PUT URLs if the Go backend should not hold direct S3 credentials.

### 4.5 `ingest` (SQS FIFO → lance-ingest)
```json
{
  "op": "ingest",
  "table": "sales",
  "batch_id": "2026-10-21T12-00-00Z-000123",
  "source": {
    "format": "arrow_ipc",
    "uris": ["s3://staging-bucket/staging/sales/2026-10-21T12-00-00Z-000123/part-0001.arrows"]
  }
}
```
CSV variant:
```json
{
  "source": {
    "format": "csv",
    "uris": ["s3://.../part-0001.csv"],
    "schema_uri": "s3://.../batch.schema.arrows",
    "csv": {
      "header": true,
      "delimiter": ",",
      "quote": "\"",
      "null_values": ["", "NULL", "\\N"],
      "timestamp_format": "%Y-%m-%dT%H:%M:%S%.fZ"
    }
  }
}
```
SQS settings: `MessageGroupId = table`, `MessageDeduplicationId = batch_id`.

Pipeline:
1. **Idempotency check:** if `batch_id` is already committed for this table, ack and return `ALREADY_INGESTED`.
2. **Read:**
   - Arrow IPC: `arrow_ipc::reader::StreamReader`. The schema is embedded in the file; validate it against the table schema.
   - CSV: load the schema from `schema_uri` (schema-only IPC), validate it against the table schema, then parse with `arrow_csv::ReaderBuilder::new(schema)` with **inference disabled** and explicit null/timestamp settings.
3. **Sort** by the table's `sort_key` (DataFusion sort, with spill to `/tmp` enabled for large batches).
4. **Append** with `Dataset::write(... WriteMode::Append ...)`, using `storage_version` and field compression from the spec.
5. **Record `batch_id` → committed Lance version** in the ingest ledger (§5.3).
6. **`optimize_indices`**, so new fragments are covered (build `pending` indexes on the first ingest).
7. Emit metrics: rows, bytes, duration, new version, index coverage.
8. Optionally delete or tag staging objects after a successful commit (configurable retention).

Failure handling: any failure before the commit → message returns to the queue (bounded retries → DLQ). A failure after the commit but before the ledger write is detected on retry by checking the table's recent transactions for `batch_id`. Write `batch_id` into the commit's transaction properties if supported **[VERIFY]**; otherwise reconcile through the ledger.

### 4.6 `query` / `explain` (lance-query)
```json
{
  "op": "query",
  "sql": "SELECT region, count(*) AS n, sum(amount) AS total FROM sales WHERE col_1 BETWEEN 100 AND 200 AND col_2 = 'PE' GROUP BY region",
  "format": "json",
  "max_rows": 10000,
  "timeout_ms": 20000
}
```
Response `stats`: `elapsed_ms`, `plan_ms`, `exec_ms`, `bytes_read`, `get_requests`, `cold_start`, `dataset_versions`. The `explain` op returns `EXPLAIN ANALYZE` output, which is used to confirm scalar-index usage.

---

## 5. Internals

### 5.1 Dynamic catalog for DataFusion (no tables in code)
- Implement a DataFusion **`SchemaProvider`** (`LanceNamespace`) backed by the namespace prefix `s3://<express-bucket>/tables/`:
  - `table_names()` lists `*.lance` directories that have a non-empty `_versions/` (Lance directory-namespace convention), cached with a TTL.
  - `table(name)` opens the dataset on first use (cached in a process-wide map), wraps it in `LanceTableProvider`, and returns it.
- Register it once at init in a `SessionContext`. Every SQL query resolves table names dynamically, so tables created by `create_table` become queryable without redeploying.
- **Freshness policy** (env `FRESHNESS_MODE = interval:<secs> | always | pinned`): periodically `checkout_latest` cached datasets and refresh the table list.
- Restrict resolution to the configured namespace prefix. Never resolve arbitrary S3 paths from SQL.

### 5.2 Process lifecycle (latency-critical)
- Initialize the Tokio runtime, AWS config, object-store config, and the catalog in the **init phase**; cache them in `static` cells.
- Never reopen datasets per request. Reuse across warm invocations.
- Open lazily per table to keep init fast.

### 5.3 Ingest ledger (idempotency)
- **Default:** a DynamoDB table `lance_ingest_ledger` (PK `table`, SK `batch_id`, attrs `version`, `rows`, `committed_at`), written with a conditional put. Add a gateway endpoint for DynamoDB.
- **Alternative (no extra service):** a per-table Lance system table `tables/_system_ingest_log.lance`. This is safe because FIFO groups serialize writes per table. Choose one in Phase 0 and record the decision.

### 5.4 S3 Express One Zone access
- Bucket name format: `<base>--<az-id>--x-s3`.
- Session-based auth (`CreateSession`). Confirm how the pinned Lance object-store backend enables S3 Express (storage option / flag) **[VERIFY]**.
- IAM:
  - `lance-query`: read-only S3 Express session permissions on the data bucket.
  - `lance-control` / `lance-ingest`: read-write S3 Express session permissions on the data bucket, read on staging, SQS send (control) / receive (ingest), and ledger access.
- Tune Lance I/O parallelism higher than for S3 Standard, and benchmark it.

### 5.5 Lance commit semantics on S3 Express
- Lance commits a new manifest atomically. **[VERIFY]** that the commit mechanism used by the pinned Lance version (conditional put / put-if-absent) is supported on S3 Express directory buckets. This is the **#1 spike in Phase 0**, alongside reads.
- FIFO per-table serialization means at most one writer per table in normal operation; conflicts can only come from misconfiguration and must fail loudly, not corrupt data.

### 5.6 Safety (query function)
- Read-only SQL only. Reject DDL/DML/COPY at the logical-plan level.
- Per-query timeout; DataFusion memory pool sized below Lambda memory; `max_rows` cap with a `truncated` flag.
- Resolve only tables in the namespace (optionally an allowlist or tenant prefix).

### 5.7 Observability
- Structured JSON logs (`tracing`), including `request_id`, `op`, `table`, `batch_id`, `version`.
- An instrumented object-store wrapper counts bytes read/written and GET/PUT requests.
- CloudWatch metrics: ingest rows/sec, ingest lag (enqueue → commit), query p50/p95/p99, bytes read per query, index coverage per table, DLQ depth.

---

## 6. Data Layout Rules (read-first, applied by the service from the spec)

- **Sort** every ingest batch by `sort_key`. Low-cardinality columns go first in the key, which makes dictionary columns RLE-friendly and keeps ZONEMAP stats tight.
- **Appends sort only within each batch.** Global order degrades over many appends, so the scheduled `maintain` op **re-clusters** tables by rewriting them fully sorted when fragment count or overlap crosses a threshold. This is heavy on writes, which is acceptable by design.
- **Compaction** in `maintain`, targeting well-sized fragments (benchmark 1M vs 4M rows).
- **`optimize_indices`** after every ingest and in `maintain`.
- Storage version **2.2+** (general block compression, block-level RLE, LZ4 dictionary values). Use per-field zstd where the spec requests it (field metadata keys prefixed `lance-encoding:` **[VERIFY]** exact names).
- **[VERIFY]** whether a compound (multi-column) BTREE index exists in the pinned Lance version. If so, expose it as index type `COMPOUND_BTREE` with `columns: [...]` and benchmark it against single-column indexes.

---

## 7. Go Backend Contract (summary to share with the Go team)

1. Build the Arrow schema with `github.com/apache/arrow-go`. Serialize it as a **schema-only IPC stream** → base64 → `create_table`.
2. For each data batch:
   - **Preferred:** write an **Arrow IPC stream** (`.arrows`) with Zstd buffer compression (`ipc.WithZstd()`) and record batches of 64k–1M rows. The schema is embedded.
   - **CSV:** write `.csv` **plus** `batch.schema.arrows` (schema-only IPC). CSV must follow the declared dialect (header, delimiter, quoting, null markers, timestamp format).
3. Upload to `staging/<table>/<batch_id>/`, then send an `ingest` message to SQS FIFO (`MessageGroupId=table`, `MessageDeduplicationId=batch_id`).
4. Use stable, unique `batch_id`s. Retries with the same `batch_id` are safe.
5. Send plain types and **do not dictionary-encode**; Lance chooses encodings itself.
6. Poll `describe_table` to observe the new version and index coverage after ingest.

---

## 8. Lambda Build & Configuration

### 8.1 Build
- `cargo lambda build --release --arm64` → `bootstrap`.
- Prerequisites: Rust stable, **`protoc`** (Lance builds protobufs) **[VERIFY]**, and Zig (cargo-lambda cross-compilation) or a native arm64 AL2023 build host/CI runner.
- Release profile:
  ```toml
  [profile.release]
  lto = "fat"
  codegen-units = 1
  opt-level = 3
  strip = true
  panic = "abort"
  ```
- **Pin `lance`, and use exactly the `datafusion` version it depends on.** Mismatches break `LanceTableProvider`. Record the pins in `docs/decisions.md`.

### 8.2 Function settings (starting points; tune by benchmark)
| Setting | lance-query | lance-ingest | lance-control |
|---|---|---|---|
| Memory | 3008 MB (sweep 1769–10240) | 4096–10240 MB (sort memory) | 512–1024 MB |
| Timeout | 30 s | up to 15 min | 30 s |
| Ephemeral storage | 512 MB | up to 10 GB (sort spill) | 512 MB |
| Concurrency | high; provisioned ≥1 if p99 matters | SQS-driven; FIFO serializes per table | low |
| Env | `NAMESPACE_URI`, `FRESHNESS_MODE`, `IO_PARALLELISM` | `NAMESPACE_URI`, `LEDGER`, `SPILL_DIR=/tmp` | `NAMESPACE_URI`, `INGEST_QUEUE_URL` |

### 8.3 Layers
The Rust binary should not need layers. Use one only if a native shared library becomes necessary, or to share one binary across the three functions. Each function must remain deployable without it.

### 8.4 Very large ingests
If a single batch exceeds what one Lambda can sort and write within 15 minutes and 10 GB of memory, the Go backend must split it into multiple `batch_id`s (document the max recommended batch size after benchmarking). Re-clustering in `maintain` restores global order later.

---

## 9. Infrastructure (IaC: AWS SAM or CDK; agent's choice, documented)

- VPC with one private subnet in the **directory bucket's AZ ID** (assert AZ-ID equality in IaC).
- S3 Express directory bucket (data) and staging bucket (S3 Standard or Express; decide in Phase 0).
- Gateway endpoints: S3 Express **[VERIFY]**, S3 (if staging is standard), DynamoDB (if ledger). Interface endpoint: SQS.
- SQS FIFO queue `lance-ingest.fifo` + DLQ; event source mapping to `lance-ingest`.
- DynamoDB ledger table (if chosen).
- Three Lambda functions from one artifact, with separate IAM roles (least privilege, §5.4).
- Function URLs with IAM auth for `lance-control` and `lance-query` (API Gateway later if needed).
- EventBridge schedule → `lance-ingest` `maintain`.
- CloudWatch log groups with retention, alarms (errors, DLQ depth, ingest lag, query p95).

---

## 10. Repository Structure

```
lance-service/
├── Cargo.toml                    # workspace
├── crates/
│   ├── service/                  # bin: bootstrap; op router
│   │   └── src/{main.rs, router.rs, ops/{control.rs, ingest.rs, query.rs, maintain.rs}}
│   ├── catalog/                  # LanceNamespace SchemaProvider, dataset cache, freshness
│   ├── spec/                     # table spec model, validation, schema compatibility/diff
│   ├── ingest-io/                # Arrow IPC + CSV readers, schema sidecar loading, sort pipeline
│   ├── storage/                  # S3 Express config, instrumented object store
│   ├── ledger/                   # idempotency ledger (DynamoDB / Lance impl)
│   └── bench/                    # benchmark harness
├── go-client-example/            # minimal Go program: build schema, create_table, write .arrows/.csv, ingest
├── infra/                        # SAM template or CDK app
├── testdata/                     # synthetic data generators
├── docs/
│   ├── decisions.md              # ADRs: version pins, ledger choice, staging bucket type
│   ├── api.md                    # full request/response contracts + error codes
│   └── benchmarks.md
└── README.md                     # runbook
```

---

## 11. Phased Implementation Plan

### Phase 0: Spikes & decisions
- [ ] **Spike A:** a minimal Rust binary that **creates, appends to, and reads** a Lance dataset in an S3 Express directory bucket from a Lambda in the same AZ. This confirms auth, IAM, the S3 Express flag, and **commit support**.
- [ ] **Spike B:** create a scalar index on an empty table vs after the first append. Confirm behavior.
- [ ] **Spike C:** confirm the API for writing/reading Lance table metadata (for the spec) and transaction properties (for `batch_id`).
- [ ] Pin `lance` + `datafusion`. Choose the ledger (DynamoDB vs Lance) and staging bucket type. Write the ADRs.
- **Exit:** `docs/decisions.md` committed; spikes pass.

### Phase 1: Spec & validation library (local)
- [ ] Table spec model, JSON parsing, Arrow IPC schema decode/encode.
- [ ] Validation rules (§3.3), schema compatibility diff (§3.4), unit tests (including bad types, duplicate indexes, incompatible index types).
- **Exit:** full test coverage of validation and diff logic.

### Phase 2: Control + ingest (local filesystem)
- [ ] `create_table`, `describe_table`, `list_tables`, `add_index`, `add_columns` against local Lance datasets.
- [ ] Ingest pipeline: Arrow IPC and CSV+sidecar → validate → sort → append → optimize indices. Idempotency via the ledger (local impl).
- [ ] `go-client-example` producing both formats; end-to-end local test (Go writes files → Rust ingests).
- **Exit:** Go-produced Arrow and CSV batches ingest correctly; retries with the same `batch_id` are no-ops.

### Phase 3: Query with dynamic catalog (local)
- [ ] `LanceNamespace` SchemaProvider, dataset cache, freshness policy.
- [ ] `query` / `explain` ops with SQL guard, limits and stats.
- [ ] Prove that tables created at runtime become queryable without a restart, and that scalar indexes are used (`EXPLAIN ANALYZE`).
- **Exit:** all workload queries correct locally; index use demonstrated.

### Phase 4: AWS deployment
- [ ] IaC (§9); deploy three functions from one artifact.
- [ ] End-to-end on AWS: Go client → `create_table` → upload → SQS → ingest → query.
- **Exit:** full flow works on S3 Express; metrics and logs visible.

### Phase 5: Benchmark & tune (core)
- [ ] Bench harness: each workload query N times cold/warm, recording p50/p95/p99, bytes read, GET count, billed duration, memory.
- [ ] Sweeps: Lambda memory; Lance I/O parallelism; fragment size; LZ4 vs zstd; index types; sorted vs unsorted; re-clustered vs append-only; compound index (if available).
- [ ] Baselines: read-tuned Parquet via DataFusion on the same bucket; optionally S3 Standard to quantify the Express gain.
- [ ] Ingest benchmarks: max batch size per Lambda (§8.4), Arrow vs CSV throughput.
- **Exit:** `docs/benchmarks.md` with chosen configuration and documented limits.

### Phase 6: Hardening
- [ ] `maintain` op: compaction, index optimization, re-clustering thresholds, old-version cleanup/retention.
- [ ] DLQ handling and replay runbook; alarms.
- [ ] Provisioned-concurrency decision for `lance-query`.
- [ ] `docs/api.md` finalized with error codes; README runbook (create table, ingest, add index, add column, roll back to a previous version, replay DLQ).

---

## 12. Acceptance Criteria

1. **No table, schema or index is defined in code.** All are created via `create_table` / `add_index` / `add_columns` requests from the Go backend.
2. The service **owns and enforces** schemas: ingests with incompatible schemas are rejected with a clear diff; allowed evolution (nullable column adds) works without rewriting data.
3. Ingest accepts **Arrow IPC** (embedded schema) and **CSV + schema-only IPC sidecar**, is idempotent per `batch_id`, and is serialized per table.
4. Tables created at runtime are **queryable immediately** via DataFusion's dynamic catalog, with no redeploy.
5. Filters on indexed columns demonstrably use scalar indexes (`EXPLAIN ANALYZE` evidence in docs).
6. Runs on `provided.al2023` arm64 from a **zip** (no container), against an **S3 Express One Zone** bucket in the **same AZ**.
7. Every query response includes latency, bytes read and GET stats; `docs/benchmarks.md` compares against a read-tuned Parquet baseline with versions and settings recorded.
8. Infrastructure is fully reproducible from IaC; runbook documented.

**Targets** (refine in Phase 0 once data sizes are known): warm p95 in the low tens of milliseconds for selective indexed queries; bytes read for selective queries a small fraction of the Parquet baseline.

---

## 13. Risks, Open Questions & Future Work

| Item | Mitigation / plan |
|---|---|
| Lance commit mechanism on S3 Express directory buckets | Phase 0 Spike A, before anything else |
| S3 Express config path in Lance's object store changes by version | Pin versions; isolate in the `storage` crate |
| DataFusion ↔ Lance version coupling | Use the exact DataFusion version Lance depends on; upgrade together |
| Index creation on empty tables | Spike B; fall back to `pending` indexes built after the first ingest |
| Global sort degrades over appends | Scheduled re-clustering (writes are cheap by design) |
| Large batches exceed Lambda limits | Benchmark max batch size; Go splits batches |
| Lambda payload limits | Data only via the staging bucket; results capped, with response streaming later |
| Stale reads after ingest | Freshness policy; return `dataset_versions` in every response |
| Redundant same-column indexes have had edge-case bugs in connectors | Validation forbids same-class duplicates; correctness tests after append + partial index refresh |
| Cross-AZ misconfiguration | IaC assertion: subnet AZ ID == bucket AZ ID |
| **Future:** upserts/deletes | Lance `merge_insert` keyed by a declared `primary_key` in the spec (v2) |
| **Future:** streaming ingest | Arrow Flight from Go directly to an ingest service, skipping staging files |
| **Future:** multi-tenant | Tenant prefix in the namespace; per-tenant IAM / allowlists |

---

## 14. First Tasks for the Agent (in order)

1. **Spike A** (§11 Phase 0): create, append to and read a Lance dataset on S3 Express from a same-AZ Lambda. Write the findings to `docs/decisions.md`.
2. Spikes B and C; pin versions; decide the ledger and staging bucket type.
3. Scaffold the workspace (§10).
4. Implement the spec model + validation (Phase 1), then control + ingest (Phase 2), then query with the dynamic catalog (Phase 3).
5. Deploy (Phase 4), benchmark (Phase 5), harden (Phase 6), updating `docs/benchmarks.md` and `docs/api.md` as you go.
