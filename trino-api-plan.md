# Plan: Trino-compatible HTTP API on `smartberry-lance-ingest`

> **Audience:** the engineer (or agent) implementing it.
> **Goal:** Tableau, DBeaver, DataGrip and any Trino client (JDBC, ODBC, CLI, Python `trino`) connect to
> the Lance tables as if they were a Trino cluster, and run read-only SQL on them.
> **Constraints:** exactly two Lambdas (ADR-011): no new function. The API lives in
> `smartberry-lance-ingest` behind its Function URL. Read-only. Items marked **[VERIFY]** must be
> checked against the client/driver versions actually used before relying on them.

### Decisions (2026-10-22)

1. **One credential, read-only:** a single `user` + password (API key) that can query every table.
   It is configured in `config.toml` (`[trino]`, §4). No user management.
2. **Names:** catalog `lance`, schema `default`.
3. **Exposure:** password only, no IP allowlist.
4. **Target client:** Tableau's native Trino connector (plus DBeaver / `trino` CLI for testing).
5. **No pagination:** every result is one response. Results that don't fit are cut (§6).
6. **Response streaming:** the Function URL uses invoke mode `RESPONSE_STREAM`, so one response can
   carry up to 200 MB instead of 6 MB, and rows are sent while the query produces them (§3, §6).

---

## 1. Summary

```
 Tableau / DBeaver / trino CLI
   │  HTTPS  POST /v1/statement  (Basic auth, X-Trino-* headers, SQL in the body)
   ▼
 Function URL (auth NONE, RESPONSE_STREAM) ──► smartberry-lance-ingest (same bootstrap binary)
                                                 │
                                                 ├─ event has requestContext.http ──► trino::handle_http  (read-only)
                                                 │     auth → statement router → QueryEngine → StreamingResponse
                                                 └─ any other event (Go backend Invoke) ──► handler::handle → BufferedResponse
```

- **One binary, two entry shapes, two response modes.** `main.rs` looks at the event: a Function
  URL event (payload format 2.0) goes to the new `trino` module and answers with a streamed HTTP
  response; a raw `{"op": ...}` request keeps going to `handler::handle` and answers with the
  buffered JSON it returns today. `lambda_runtime` supports both from one handler: it returns
  `FunctionResponse::BufferedResponse(..)` or `FunctionResponse::StreamingResponse(StreamResponse
  { metadata_prelude, stream })` per invocation **[VERIFY with the pinned `lambda_runtime` 1.x:
  issue aws-lambda-rust-runtime#995 reports compile errors with some body types]**.
- **The HTTP path can only reach read-only code:** it never builds a `Request`, so writes,
  compaction and control ops are unreachable from the URL.
- **`control` gets no URL.** The Go backend keeps calling both functions with `Invoke` (IAM),
  buffered, unchanged.
- **The engine is the existing one.** `ops/query.rs` (`QueryEngine`, dataset cache, read-only
  `SQLOptions`, timeout, memory pool, I/O stats) is reused. The new code is protocol, metadata,
  dialect and streaming glue around it.
- **No heavy new dependencies.** `flate2`, `sha2`, `base64`, `sqlparser` are already in
  `Cargo.lock`; streaming uses types `lambda_runtime` already has. The binary is ~201 MB of 262 MB
  (lambda-dev); keep watching it.

---

## 2. What the Trino client protocol needs (v1, "direct" protocol)

Reference: Trino client REST protocol docs and `StatementClientV1` in `trino-client` **[VERIFY]**
against the driver version Tableau/DBeaver ship.

### 2.1 Requests

| Request | Meaning |
|---|---|
| `POST /v1/statement` | Body = SQL text. Starts a query. |
| `GET <nextUri>` | Next page. Not used here: every response is final (no `nextUri`, §6). |
| `DELETE <queryUri>` | Cancel. Answer `204` (the query already finished). |
| `GET /v1/info` | Server info (`nodeVersion.version`, `coordinator`, `starting`, `uptime`). Some clients probe it. |

Request headers to honour: `Authorization: Basic …`, `X-Trino-User` (required), `X-Trino-Source`,
`X-Trino-Catalog`, `X-Trino-Schema`, `X-Trino-Time-Zone`, `X-Trino-Session`,
`X-Trino-Prepared-Statement` (`name=urlencoded-sql`, sent on every request: prepared statements are
client-side state, which suits a stateless Lambda), `X-Trino-Client-Info`, `X-Trino-Client-Tags`,
`Accept-Encoding`. Ignore unknown ones. The legacy `X-Presto-*` names are not supported (old
Presto drivers are out of scope).

### 2.2 Response (`QueryResults` JSON)

```json
{
  "id": "20261022_181500_00001_abcde",
  "infoUri": "https://<url>/ui/query.html?20261022_181500_00001_abcde",
  "columns": [{"name": "region", "type": "varchar",
               "typeSignature": {"rawType": "varchar", "arguments": [{"kind": "LONG", "value": 2147483647}]}}],
  "data": [["PE", 250], ["CL", 250]],
  "stats": {"state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1,
            "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1,
            "cpuTimeMillis": 0, "wallTimeMillis": 140, "queuedTimeMillis": 0, "elapsedTimeMillis": 484,
            "processedRows": 3000, "processedBytes": 5676, "physicalInputBytes": 5676,
            "peakMemoryBytes": 0, "spilledBytes": 0},
  "warnings": []
}
```

- **Finishing in the first response is allowed:** `POST` may answer with `columns`, all `data`,
  `state = FINISHED` and no `nextUri` **[VERIFY with JDBC, CLI and Python clients]**. That is the
  only path here.
- **Field order matters for streaming:** `id`, `infoUri` and `columns` are written first (known
  after planning), then `data` row by row, and `stats`, `warnings` and `error` last, once the
  outcome is known. JSON objects are unordered and the client parses the whole document before
  using it, so this is valid **[VERIFY the driver doesn't depend on `stats` coming first]**.
- `columns` must be present whenever `data` is, and also on an empty result.
- `id` uses Trino's format `YYYYMMDD_HHMMSS_NNNNN_xxxxx` (derive it from the Lambda request id).
- Errors: `stats.state = FAILED` and
  `error: {message, errorCode, errorName, errorType, failureInfo: {type, message, stack: []}, errorLocation?}`.
  HTTP status stays `200`; `401` only for authentication. An error after rows were already sent
  (a failure mid-execution) is reported the same way, at the end of the document, next to the
  partial `data` (§6) **[VERIFY the driver raises it instead of returning the rows]**.
- Response headers for session changes: `X-Trino-Set-Catalog`, `X-Trino-Set-Schema`,
  `X-Trino-Set-Session`, `X-Trino-Clear-Session`, `X-Trino-Added-Prepare`,
  `X-Trino-Deallocated-Prepare`. They go in the streaming prelude, so they must be decided before
  the first byte of the body (they always are: they depend on the statement, not on the rows).

### 2.3 Value encoding in `data` (Arrow → Trino)

The existing `collect_json` (arrow JSON writer, objects keyed by column) can't be reused as is: Trino
wants positional arrays and its own text formats. New encoder `trino/encode.rs`, which writes one
`RecordBatch` at a time into the output stream:

| Arrow | Trino type | JSON value |
|---|---|---|
| Boolean | `boolean` | `true` |
| Int8 / Int16 / Int32 / Int64 | `tinyint` / `smallint` / `integer` / `bigint` | number |
| UInt8 / UInt16 / UInt32 | `smallint` / `integer` / `bigint` | number |
| UInt64 | `decimal(20,0)` | string |
| Float32 / Float64 | `real` / `double` | number; `"NaN"`, `"Infinity"`, `"-Infinity"` as strings |
| Decimal128(p,s) | `decimal(p,s)` | string `"123.45"` |
| Utf8 / LargeUtf8 / Utf8View | `varchar` | string |
| Binary / LargeBinary | `varbinary` | base64 string |
| Date32 / Date64 | `date` | `"2026-10-22"` |
| Timestamp(unit, None) | `timestamp(p)` (p = 0/3/6/9 from unit) | `"2026-10-22 13:45:00.123456"` |
| Timestamp(unit, Some(tz)) | `timestamp(p) with time zone` | `"2026-10-22 13:45:00.123456 UTC"` |
| Time64 | `time(p)` | `"13:45:00.123456"` |
| List / LargeList | `array(T)` | JSON array |
| Struct | `row(f T, …)` | JSON array (positional) |
| Map | `map(K,V)` | JSON object |
| Dictionary(K, V) | type of V | value |
| Null | `unknown` | `null` |

Other types (intervals, durations, unions): fail with `NOT_SUPPORTED` naming the column (checked at
planning, before anything is streamed).
`typeSignature` must match `type` (e.g. `timestamp(6) with time zone` → `rawType` and a `LONG`
argument) **[VERIFY: JDBC reads `typeSignature`, not `type`]**.

---

## 3. Function URL event handling and streaming

- **Request:** payload format 2.0: `requestContext.http.method`, `rawPath`, `rawQueryString`,
  `headers` (lower-case keys, repeated headers joined by commas), `body`, `isBase64Encoded`. The
  request is not streamed; SQL bodies are small.
- **Response:** with invoke mode `RESPONSE_STREAM` every response coming through the URL is
  streamed, including `401`, `404` and `/v1/info`. The handler returns a `StreamResponse`: a
  `MetadataPrelude` (status code, headers) and a body stream of byte chunks.
- **gzip:** when the client sends `Accept-Encoding: gzip` (the Trino JDBC driver does, through
  OkHttp **[VERIFY]**), the body goes through a streaming gzip encoder (`flate2`), flushed after
  each batch, with `Content-Encoding: gzip` in the prelude. No base64: a streamed body is raw bytes.
- **Producer:** a task runs the DataFusion stream and pushes encoded chunks into a bounded channel;
  the response body reads the channel. The bound gives backpressure: when the client or Lambda's
  bandwidth is slow, execution waits instead of buffering the result in memory.
- **Lambda limits for streamed responses:** up to **200 MB** per response; the first 6 MB go at
  full speed, beyond that throughput is capped at about **2 MB/s** **[VERIFY current numbers in
  the Lambda quotas page]**. Billed duration includes the time spent streaming.
- **Detection in `main.rs`:** `event.payload.get("requestContext").and_then(|r| r.get("http"))` →
  HTTP path. Everything else → `handler::handle` (unchanged), wrapped as a buffered response.
  A raw `Invoke` from the Go backend never gets a streaming response.
- **Routing table in `trino/http.rs`:**

| Method + path | Handler |
|---|---|
| `POST /v1/statement` | `statement::run` (whole query, one streamed response) |
| `DELETE /v1/statement/...` | `204` (nothing is ever left running) |
| `GET /v1/statement/...` | `410 Gone` (no pages exist; a client should never ask) |
| `GET /v1/info` | static info |
| anything else | `404` |

---

## 4. Authentication and exposure

The URL is public (auth type `NONE`: Trino drivers can't sign SigV4), so the service authenticates.

- **One credential** (decision 1): Basic auth `user:password`, required on every request. The
  password is the API key. It can run read-only SQL on every table and nothing else. JDBC only
  sends a password over TLS (`SSL=true`); the Function URL is HTTPS on 443.
- **In `config.toml`, as a hash:** `[trino] user` and `password_sha256` (hex SHA-256 of the key).
  `config.toml` goes inside the Lambda zip (and is gitignored, `config.example.toml` is the
  template), so it never holds the key itself. Because the
  key is random (32 bytes, base64url, generated once with `openssl rand -base64 32` or
  `lance-cli trino-key`), plain SHA-256 is enough: there is nothing to brute-force by dictionary,
  so no salt or slow hash is needed. The key is handed to Tableau users out of band.
- **Rotation:** generate a new key, replace `password_sha256`, `deploy/deploy.sh`. Old key stops
  working when the new zip is live.
- **Check:** constant-time comparison of `sha256(password)` with the configured hash; the user name
  must match too. `X-Trino-User` must equal the Basic user (no impersonation).
- **No IP allowlist** (decision 3). Failed logins are logged (user, source IP; never the password
  or the header); a CloudWatch metric filter alarms on bursts.
- **Concurrency:** queries share `ingest`'s concurrency with writes (ADR-011 trade-off). A flood of
  dashboard queries could delay `Save`; long streamed extracts hold a container for longer than a
  buffered query would. Mitigation if it happens: reserved concurrency on `ingest` sized for both,
  and the caps of §6.
- Logs: the SQL is logged (it is the main debugging tool); `Authorization` and
  `X-Trino-Extra-Credential` never are.

---

## 5. Statements the clients send, and how each is answered

Phase 0 records the real traffic of each client (§9). Expected set:

### 5.1 Session and transaction statements (answered without the engine)

| Statement | Answer |
|---|---|
| `USE catalog.schema` / `USE schema` | empty result + `X-Trino-Set-Catalog` / `X-Trino-Set-Schema` |
| `SET SESSION k = v` / `RESET SESSION k` | empty result + `X-Trino-Set-Session` / `X-Trino-Clear-Session` (values ignored) |
| `START TRANSACTION`, `COMMIT`, `ROLLBACK` | empty result (no-op: reads are snapshot-consistent per query anyway) |
| `PREPARE n FROM sql` | `X-Trino-Added-Prepare: n=<urlencoded sql>` |
| `DEALLOCATE PREPARE n` | `X-Trino-Deallocated-Prepare: n` |
| `EXECUTE n USING a, b` | SQL from the `X-Trino-Prepared-Statement` header, parameters bound (§7.3) |
| `EXECUTE IMMEDIATE 'sql' USING …` | same, inline **[VERIFY which form the pinned JDBC uses]** |
| `SHOW SESSION`, `SHOW FUNCTIONS` | small static result |
| `SELECT version()` / `/v1/info` | the configured `trino.server_version` |

### 5.2 Metadata (catalog emulation)

The namespace is fixed: catalog `trino.catalog` (default `lance`), one schema `trino.schema`
(default `default`), tables = `list_tables` (S3 listing of `tables/*.lance`, cached per container
for `query.freshness_seconds`). Columns come from the cached `Dataset` schemas.

Served as in-memory tables registered per query (DataFusion `MemTable`), only when the SQL
references them:

| Table | Used by |
|---|---|
| `system.jdbc.catalogs`, `schemas`, `tables`, `columns`, `table_types`, `types`, `attributes`, `procedures`, `pseudo_columns`, `super_tables`, `super_types`, `udts` | Trino JDBC `DatabaseMetaData` **[VERIFY column list per driver version]** |
| `<catalog>.information_schema.schemata`, `tables`, `columns`, `views` | ODBC drivers, Tableau, DBeaver navigator |
| `system.metadata.catalogs` | some clients |

Statements rewritten to queries over those tables: `SHOW CATALOGS`, `SHOW SCHEMAS [FROM c] [LIKE p]`,
`SHOW TABLES [FROM s] [LIKE p]`, `SHOW COLUMNS FROM t`, `DESCRIBE t`, `SHOW CREATE TABLE t`
(synthesized DDL text).

`system.jdbc.columns` needs Trino-style type names and JDBC `DATA_TYPE` codes (`Types.BIGINT = -5`,
…) from the same mapping table as §2.3.

### 5.3 Name resolution

`referenced_tables` (`ops/query.rs:193`) accepts today only `t` and `public.t`. It becomes:
`t`, `schema.t`, `catalog.schema.t` where catalog/schema are the configured names (case-insensitive),
plus the metadata tables of §5.2. Before planning, the SQL is rewritten so DataFusion sees bare
table names (the AST from `sqlparser` is walked and `ObjectName`s replaced), keeping the existing
"only tables of this service" rule.

### 5.4 Everything else

`SELECT` / `WITH` / `EXPLAIN` (as Trino `EXPLAIN`, not `EXPLAIN ANALYZE`) → the engine.
DML, DDL, `CALL`, `GRANT`… → `NOT_SUPPORTED` (`USER_ERROR`), as the engine already rejects them.

---

## 6. Result size and time: one streamed response, cut when needed (decisions 5 and 6)

No pagination: the POST runs the query and streams every row it can in that single response, with
no `nextUri`. The Lambda holds at most one channel's worth of encoded rows in memory, never the
whole result.

- **Size caps, whichever comes first:**
  - `trino.max_rows` rows (pushed into the plan as `LIMIT max_rows + 1`, like `collect_json` does,
    so the scan stops early).
  - `trino.max_result_mb` of **uncompressed** JSON. Counting before gzip keeps one rule for both
    cases: without gzip it stays below Lambda's 200 MB streaming limit, and with gzip it also
    bounds what the client has to hold. The Trino client parses each response fully in memory,
    so a huge document is a problem for Tableau before it is one for Lambda **[VERIFY a
    comfortable size with Tableau Desktop in Phase 0]**.
- **Time caps:** execution and sending overlap, and sending beyond 6 MB runs at ~2 MB/s, so the
  current single 30 s limit can't cover the whole response:
  - `query.max_timeout_ms` (30 s) bounds the time **until the first row is ready**: planning,
    table opening, and all blocking work (aggregations, sorts, joins finish before they emit).
    Past it the query fails with `EXCEEDED_TIME_LIMIT` and nothing has been streamed yet.
  - `trino.max_stream_seconds` bounds the **whole response**. Reaching it ends the stream as a
    cut result (below), well before the function's 900 s timeout.
- **When rows are cut** (any of the caps above): the document is closed properly, `state =
  FINISHED`, data valid but incomplete. The protocol has no "truncated" flag, so the service adds
  a Trino warning (`warnings: [{warningCode: {code, name: "RESULT_TRUNCATED"}, message: "result
  cut at N rows (max_rows / size / time)"}]`) and logs it with the SQL. **Tableau and most tools
  do not show warnings**, so a cut result looks complete there.
- **Failure mid-stream** (e.g. an S3 error after rows were sent): the document is closed with
  `state = FAILED` and `error` (§2.2). If the Lambda itself dies mid-stream, the client gets a
  broken JSON body, which it reports as an error.
- **Impact for users:** aggregations run in the service, so live dashboards are not affected. With
  streaming, detail views and medium extracts fit (≈100 MB of JSON is around a million narrow
  rows); only very large extracts are cut. The connect guide (Phase 4) states the caps.
- **Cost:** billed duration includes streaming time at the function's 4 GB. A 100 MB uncompressed
  result is roughly 10–20 MB gzipped, a few seconds; without gzip it is ~50 s.
- If this later proves too limiting, pagination through S3 Express (or Trino's spooling protocol)
  can be added without changing anything else in this design.

---

## 7. SQL dialect: Trino SQL on DataFusion

Tableau and DBeaver generate Trino SQL. DataFusion is close (ANSI, `"quoted"` identifiers,
lower-casing of unquoted names, `TRY_CAST`, `CASE`, `INTERVAL`, `TIMESTAMP '…'`) but not equal.

### 7.1 Parse

Parse with sqlparser's `GenericDialect` (or a small custom dialect if Trino-only syntax fails to
parse) and plan from the AST, so §5.3's rewrite and §7.2 run on the same tree.

### 7.2 Function shims (scalar UDFs / rewrites)

Start from the functions Phase 0 captures from Tableau. Likely list:

| Trino | DataFusion |
|---|---|
| `date_add(unit, n, ts)` | `ts + n * INTERVAL '1 unit'` |
| `date_diff(unit, a, b)` | UDF |
| `date_trunc(unit, ts)` | native (same argument order) |
| `date_format(ts, fmt)` / `format_datetime` | `to_char` with MySQL→chrono format translation |
| `from_iso8601_timestamp`, `from_unixtime`, `to_unixtime` | `to_timestamp*` / UDF |
| `day_of_week`, `day_of_year`, `week`, `quarter`, `year`, `month`, `day`, `hour`… | `date_part` |
| `approx_distinct` | `approx_distinct` (native) |
| `approx_percentile` | `approx_percentile_cont` |
| `strpos`, `length`, `substr`, `regexp_like`, `split_part` | mostly native; `regexp_like` → `regexp_like`/`~` |
| `CAST(x AS varchar)`, `CAST(x AS double)`, `CAST(x AS integer)` | native |
| `count_if`, `bool_and`, `bool_or`, `arbitrary` | UDAF / `bool_and` / `first_value` |
| `typeof` | `arrow_typeof` mapped to Trino names |

Unknown functions fail with `FUNCTION_NOT_FOUND`, which Tableau surfaces clearly; the list grows
from real usage.

### 7.3 Parameters

`?` placeholders (JDBC `PreparedStatement`) → DataFusion `$1…$n` with `with_param_values`, using the
literals from `EXECUTE … USING` (parsed as SQL literals, so types follow Trino's literal rules).

### 7.4 Types in CAST

`varchar(n)` → `varchar`, `double` → `double`, `real` → `float`, `timestamp(p) with time zone` →
`timestamp with tz` at the matching unit, `decimal(p,s)` native.

---

## 8. Errors

| Service condition | errorName | errorType | errorCode |
|---|---|---|---|
| `SQL_ERROR` (parse) | `SYNTAX_ERROR` | `USER_ERROR` | 1 |
| unknown table | `TABLE_NOT_FOUND` | `USER_ERROR` | 46 |
| unknown column | `COLUMN_NOT_FOUND` | `USER_ERROR` | 47 |
| unknown function | `FUNCTION_NOT_FOUND` | `USER_ERROR` | 6 |
| DML / DDL / unsupported statement or type | `NOT_SUPPORTED` | `USER_ERROR` | 13 |
| `QUERY_TIMEOUT` (no first row within 30 s) | `EXCEEDED_TIME_LIMIT` | `INSUFFICIENT_RESOURCES` | 131075 |
| `RESOURCES_EXHAUSTED` | `EXCEEDED_LOCAL_MEMORY_LIMIT` | `INSUFFICIENT_RESOURCES` | 131079 |
| internal (also mid-stream) | `GENERIC_INTERNAL_ERROR` | `INTERNAL_ERROR` | 65536 |

**[VERIFY]** every numeric code against `io.trino.spi.StandardErrorCode` of the pinned version.
`errorLocation {lineNumber, columnNumber}` from the parser when available (DBeaver highlights it).

---

## 9. Phases

### Phase 0 — Spike: streaming and real client traffic (2–3 days)
- **Streaming first, deployed:** a minimal `FunctionResponse` handler in the real binary (buffered
  for `Invoke`, streamed for URL events), deployed to `ingest` with a temporary Function URL in
  `RESPONSE_STREAM` mode. Check: it compiles with the pinned `lambda_runtime`; a raw `Invoke` from
  the Go client still gets its buffered JSON; `curl --compressed` receives a streamed gzip body
  larger than 6 MB; measured throughput past 6 MB. Local `cargo lambda watch` may not emulate
  streaming **[VERIFY]**, so this part runs in AWS.
- **Traffic capture:** `POST /v1/statement` logs method, path, headers (minus credentials) and SQL,
  and answers `SELECT 1`-style queries. Connect Tableau Desktop with its **native Trino connector**
  (decision 4: it bundles the Trino JDBC driver **[VERIFY driver version per Tableau release]**),
  plus DBeaver and the `trino` CLI.
- Output: the list of statements, metadata queries and functions each client sends on connect,
  browse and a typical dashboard. It fixes the exact scope of Phases 2–3.
- Confirm: finishing in the first response works on every client; `stats`/`error` after `data` is
  accepted; an error after rows is raised; `typeSignature` requirements; prepared-statement form
  used by JDBC; the driver asks for gzip; a comfortable `max_result_mb` for Tableau.

#### Phase 0 results (2026-10-22)

Done: streaming spike in the real binary, deployed to both functions; Function URL created on
`smartberry-lance-ingest` (`RESPONSE_STREAM`, auth `NONE`, public `InvokeFunctionUrl` +
`InvokeFunction` via URL); credential in `[trino]` (key in `.secrets/trino-key`, gitignored).
Implemented already (ahead of Phase 1): auth, `/v1/info`, `POST /v1/statement` with streamed
`QueryResults`, Arrow → Trino encoder, gzip, the caps and warnings of §6, `lance-cli` running URL
events locally.

Verified:
- Mixed handler compiles with `lambda_runtime` 1.5 (no #995 problem). Go-style `Invoke` on both
  functions still gets its buffered envelope.
- Streaming through the URL: 16 MB uncompressed in 2.6 s (~6 MB/s, faster than the documented
  2 MB/s); 1 M rows = 82 MB JSON = 3 MB gzipped, in 2 s. Warm small query: 0.57 s round trip.
- `trino` CLI 476 and JDBC 476: a single final response with no `nextUri` works; `stats` /
  `warnings` after `data` are accepted; a cut result shows its rows; errors are reported with the
  Trino error name.
- Both send `x-trino-query-data-encoding: json+zstd,json+lz4,json` (spooling); ignoring it falls
  back to the direct protocol.
- **Fixed:** `double` / `real` values must always look like floats (`561376.0`): the 476 client
  decodes by column type and rejects `561376`.

Captured from JDBC 476 (what Tableau's connector will send), scope of Phases 2–3:
- On connect: `SELECT version()` (today returns DataFusion's version string).
- Metadata: fixed queries over `system.jdbc.catalogs`, `schemas`, `tables`, `columns`,
  `table_types`, with `LIKE … ESCAPE '\'` filters and the exact column lists of the driver.
- `PreparedStatement`: `PREPARE statement1 FROM <sql with ?>` (Trino syntax; sqlparser expects
  `AS`), then `EXECUTE statement1 USING …` with the SQL in `X-Trino-Prepared-Statement`.
- Qualified names: `lance."default".ventas`.
- Unaliased expressions are named by DataFusion (`Int64(1)`); Trino names them `_col0`, `_col1`….

Pending in Phase 0: the same capture from Tableau Desktop's Trino connector (needs Tableau,
run by the user against `ingest_url`).

### Phase 1 — Protocol core
- `main.rs` event split with `FunctionResponse`; `trino/http.rs` (events, routing, prelude),
  `trino/stream.rs` (producer task, bounded channel, gzip, caps), `trino/auth.rs` (Basic auth
  against `[trino] user` / `password_sha256`), `trino/protocol.rs` (`QueryResults` writer in the
  order of §2.2, stats, warnings, error mapping), `trino/encode.rs` (§2.3).
- `statement::run` → the existing planning path of `query::run` (table resolution, read-only plan,
  memory pool), then the DataFusion stream instead of `collect_json`.
- Caps and time limits of §6, with the truncation warning.
- `lance-cli trino-key`: prints a new random key and its SHA-256 to paste into `config.toml`.
- Tests: encoder per type (incl. nulls, tz, decimals, NaN), document order and validity when cut by
  rows / size / time and on mid-stream errors, with and without gzip, event parsing, auth (wrong
  user, wrong key, missing header), `Invoke` path unchanged.
- Done when: `trino --server https://… --user u --password` runs `SELECT … FROM ventas`,
  aggregates, and a result larger than 6 MB.

### Phase 2 — Metadata
- §5.1 session statements, §5.2 `system.jdbc.*` / `information_schema` / `SHOW` / `DESCRIBE`,
  §5.3 name resolution and rewrite.
- Done when: DBeaver connects, lists catalog → schema → tables → columns with correct types, and
  previews data.

#### Phase 2 results (2026-10-22)

Done, deployed, verified with JDBC 476 and the `trino` CLI 476 against the URL:
- `trino/catalog.rs`: per query, a SessionContext with default catalog `lance` / schema `default`,
  so `t`, `default.t` and `lance."default".t` resolve natively (no SQL rewrite); `system.jdbc.*`
  (catalogs, schemas, tables, columns, table_types), `system.metadata.catalogs` and
  `lance.information_schema.*` (schemata, tables, columns, views) built in memory only when
  referenced. Column lists match the driver's queries; JDBC type codes, sizes and nullability per
  column. `version()` returns `trino.server_version`.
- `trino/session.rs`: `PREPARE … FROM`, `EXECUTE … USING` (SQL from `X-Trino-Prepared-Statement`,
  `?` replaced by the literals outside strings), `EXECUTE IMMEDIATE`, `DEALLOCATE PREPARE`, `USE`,
  `SET/RESET SESSION`, transactions, `SHOW CATALOGS/SCHEMAS/TABLES/COLUMNS/SESSION/FUNCTIONS`,
  `DESCRIBE`.
- Session statements answer like Trino: `updateType` plus one boolean `result` column with
  `true` (JDBC's `prepareStatement` fails with "Query has no columns" otherwise).
- Unaliased expressions are named `_col0`, `_col1`… like Trino.
- JDBC probe: `getDatabaseProductVersion`, `getCatalogs`, `getSchemas`, `getTables`,
  `getColumns`, `getTableTypes`, a statement, a `PreparedStatement` with `?`, and a fully
  qualified name all pass. Go `Invoke` path unchanged.

Not done (seen in no client yet): `system.jdbc.types` / `procedures` / `udts` … (`getTypeInfo`,
etc.) fail with "metadata table is not supported"; add them if Tableau or DBeaver ask.

### Phase 3 — Dialect and parameters
- §7 shims driven by Phase 0's capture; `?` parameters / `EXECUTE`.
- Done when: a Tableau workbook on `ventas` (live connection) renders: filters, date parts,
  aggregations, top-N, `COUNTD`; and an extract of `ventas` completes.

### Phase 4 — Deploy and operate
- Function URL on `smartberry-lance-ingest` (created in Phase 0, or now):
  `create-function-url-config --auth-type NONE --invoke-mode RESPONSE_STREAM` plus the public
  invoke permission (`add-permission --function-url-auth-type NONE`, and
  `lambda:InvokeFunction` if the account requires both **[VERIFY current AWS rule]**). Needs
  `lambda:CreateFunctionUrlConfig` / `AddPermission` for this user **[VERIFY]**.
- `config.toml`: `[lambda] ingest_url = "https://….lambda-url.us-west-2.on.aws/"` and the `[trino]`
  section (§10). `deploy.sh` stays code + configuration only; it does not create the URL.
- CloudWatch: metric filters for failed logins, `RESULT_TRUNCATED`, `EXCEEDED_*`, p95 latency and
  streamed bytes of HTTP queries.
- Docs: ADR-012 in `docs/decisions.md`, `STATUS.md`, a short "Connect Tableau" guide (Trino
  connector: server = the URL host, port 443, SSL on, catalog `lance`, schema `default`, user and
  key; DBeaver: `jdbc:trino://<host>:443/lance/default?SSL=true`), including the caps of §6.

---

## 10. Configuration (`config.toml`)

```toml
[lambda]
ingest_url = ""                 # filled once the Function URL exists (Phase 0 / 4)

[trino]
# The single read-only credential (§4). The key itself is never stored here, only its SHA-256.
user = "tableau"
password_sha256 = ""            # lance-cli trino-key
catalog = "lance"
schema = "default"
server_version = "476"          # reported by version() and /v1/info [VERIFY what drivers check]
max_rows = 1000000              # rows per result; beyond it the result is cut with a warning
max_result_mb = 100             # uncompressed JSON per result; Lambda's streaming limit is 200 MB
max_stream_seconds = 300        # whole response (execution + sending); the first row must still
                                # be ready within query.max_timeout_ms
```

---

## 11. Risks and open questions

| Risk / question | Plan |
|---|---|
| `lambda_runtime` mixed buffered/streaming handler doesn't compile or misbehaves (issue #995) | Phase 0, first item. Fallback: stream every response (the Go `Invoke` path would then need `InvokeWithResponseStream`), or go back to buffered with the 6 MB cap |
| Tableau's SQL uses functions DataFusion lacks | Phase 0 capture + §7.2 shims; unknown ones fail clearly |
| Clients that require multi-step `QUEUED → RUNNING → FINISHED` | Phase 0 verifies; if needed, the POST answers `QUEUED` with a `nextUri` whose GET runs and streams the query (still one execution, one data response; the `GET` route of §3 then serves it) |
| Driver rejects `stats` after `data`, or ignores an error that follows rows | Phase 0 verifies; fallback: buffer up to the 6 MB fast window before sending, so errors in small results are reported cleanly |
| Cut results look complete in Tableau (no visible warning) | Accepted (decision 5). Logged + metric; documented in the connect guide; live connections preferred |
| Large results are slow (~2 MB/s past 6 MB) and billed at 4 GB | gzip; `max_result_mb`; `max_stream_seconds` |
| Tableau memory on very large responses | `max_result_mb` sized in Phase 0 |
| Dashboard load and long extracts delay writes (shared concurrency) | Monitor; reserved concurrency; caps of §6 |
| Public URL with a single shared key | Random 32-byte key, only its hash in git, read-only code reachable, failed-login alarm; rotate by redeploying |
| Cold start per new Tableau connection (~0.5 s seen on `ingest`) | Acceptable; provisioned concurrency only if users complain |
| Permissions to create the URL in this account | Check at the start of Phase 0; otherwise the admin creates it |
