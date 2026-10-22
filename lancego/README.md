# lancego

Go client for the Lance service. Converts a `[]T` of structs to Apache Arrow using
`lance` struct tags and sends it to the service (plan §4, §7).

Tables are declared like `GlueTable` in the smartberry datalake: a `TableDef` with partition keys
and sort order, and a row struct whose tags define the columns.

```go
type Venta struct {
	FilialID int16     `lance:"filial_id"`
	Fecha    string    `lance:"fecha"`
	ID       int64     `lance:"id,index=btree"`
	Region   string    `lance:"region,index=bitmap"`
	Amount   *float64  `lance:"*"`            // column "amount" (field name in snake_case), nullable
	SoldAt   time.Time `lance:"sold_at,index=zonemap"`
	Internal string                           // no tag: not a column
}

// Optional, as ParqRegLedger: the partition as one integer, stored in the registry.
func (v Venta) GetParquetID() int64 { ... }

var VentasDef = lancego.TableDef{
	Name:          "ventas",
	PartitionKeys: []string{"filial_id", "fecha"},    // like GlueTable.PartitionKeys
	SortKey:       []string{"region", "sold_at"},     // like IcebergSortOrder
}

cfg, _ := lancego.LoadConfig("")               // repo config.toml (or $LANCE_CONFIG)
client, _ := lancego.New(ctx, cfg)
ventas, _ := lancego.NewTable[Venta](client, VentasDef)

ventas.Create(ctx, lancego.CreateOptions{IfNotExists: true})
res, _ := ventas.Save(ctx, rows, lancego.SaveOptions{})  // returns once the data is committed
parts, _ := ventas.Partitions(ctx)                       // key, hash, id, rows, committed_at, version
ventas.DeletePartitions(ctx, []any{1, "2026-10-01"})
client.Query(ctx, "SELECT region, sum(amount) FROM ventas GROUP BY region", lancego.QueryOptions{})
```

## Save: partitions, hashes and confirmation

`Save` works like `SaveRecordsToParquet`:

1. Groups rows by `PartitionKeys`. Each partition is keyed `filial_id=1/fecha=2026-10-01`.
2. Hashes each partition (8 bytes). `Table.HashFunc` sets the function; the default `DefaultHash`
   is an order-independent sum of per-row FNV-1a 64 hashes.
3. Reads the partition registry and skips partitions whose hash didn't change (`Force` sends all).
4. Sends the rest as `replace_partitions`: every old row of those partitions is deleted and the new
   ones inserted **in one atomic Lance commit**. A request carries up to
   `ingest.max_rows_per_request` rows and never splits a partition.
5. Returns when the service confirms each batch (`committed`, with the Lance version and unix time)
   or rejects it (`ErrBatchFailed`, wrapping the `*APIError`). `NoWait` sends asynchronous
   invocations instead; `Wait(batchID)` waits later.

The service records each partition in DynamoDB (`ledger.dynamodb_table`): hash, partition ID
(`GetParquetID`), `records_updated` (from `SaveOptions.RecordsUpdated`, stored as is), rows,
`committed_at` and version. `Partitions` returns it. Partitioned tables reject `Insert`, because appending
would duplicate partitions.

## Tags

`lance:"<name>[,option...]"`. `<name>` is the column name, or `*` to use the Go field name
converted to snake_case, keeping acronyms together:

| Field | Column |
|---|---|
| `Amount` | `amount` |
| `CustomerID` | `customer_id` |
| `HTTPServer` | `http_server` |
| `UserIDs` | `user_ids` |
| `Field2Name` | `field2_name` |

Every column name, explicit or derived, must match `^[a-z][a-z0-9_]{0,62}$` (the service's
naming rule), so it never needs quoting in SQL. Invalid or duplicate names fail when the
type is first used (`SchemaOf`, `NewTable`, `ToRecord`). Untagged fields and `lance:"-"` are
skipped; untagged embedded structs have their fields promoted.

| Option | Meaning |
|---|---|
| `index=<type>` | scalar index: `btree`, `bitmap`, `zonemap`, `bloomfilter`, `label_list`, `ngram`, `inverted` |
| `sort=<n>` | position in the table sort key (1 = first; put low-cardinality columns first) |
| `compress=<codec>` | per-column compression hint (e.g. `zstd`) |

## Type mapping

| Go | Arrow | Nullable |
|---|---|---|
| `bool` | bool | no |
| `int8..int64`, `int` | int8..int64 (`int` → int64) | no |
| `uint8..uint64`, `uint` | uint8..uint64 (`uint` → uint64) | no |
| `float32`, `float64` | float32, float64 | no |
| `string` (and named string types) | utf8 | no |
| `time.Time` | timestamp[us, UTC] | no |
| `[]byte` | binary | yes (nil → null) |
| `[]T`, `[N]T` | list\<T\> | slice yes (nil → null; empty → empty list), array no |
| struct (tagged fields) | struct | no |
| `*T` | same as `T` | yes (nil → null) |

Maps, interfaces, channels, funcs and complex numbers are rejected.

## Flow

1. `Create`: the schema is sent as a base64 schema-only Arrow IPC stream, plus sort key,
   indexes and compression hints from the tags.
2. Partitioned tables: `Save` (above). Append-only tables (no `PartitionKeys`): `Insert` = `Stage` + `Ingest`:
   - `Stage` encodes rows as an Arrow IPC stream (Zstd buffers, `ingest.batch_rows` rows per
     batch) and uploads it to `s3://<bucket>/<staging_prefix>/<table>/<batch_id>/part-0001.arrows`.
   - `Ingest` invokes `lambda.ingest_function` synchronously and returns the committed batch.
   - Re-sending the same `BatchID` is safe (idempotent on the service).
3. Writes go to `lambda.ingest_function` as synchronous invocations; the response comes after the
   commit, so a nil error means the data is visible. The service runs one write per table at a
   time with a lease lock in the bucket (`locks/<table>.json`); a write that finds the table busy
   waits up to `service.lock_wait_seconds`, then fails with `TABLE_BUSY`. Writes whose outcome is
   unknown (network error, `INTERNAL`, `LOCK_LOST`) are retried with the same batch ID.
4. `Compact(ctx, CompactOptions{MaxFileMB: 50, MaxFiles: 20})` runs one bounded compaction under
   the same lock, so it never overlaps a write of that table. Adjacent small files are merged
   into one file while the group stays within both limits (see `docs/decisions.md`, ADR-006).
   `result.More` = call it again. It also removes versions older than
   `service.version_retention_hours` and the files only they used.
5. `client.TableInfo(ctx, table, detail)` / `table.Info`: version, rows, deleted rows, files and
   size distribution, unused files and versions awaiting cleanup, index coverage, what a
   compaction would merge now, registry summary, lock and recent / pending / failed operations.
   `detail=true` adds per-file min/max of the partition and sort columns (pruning) and how many
   files each partition spans.

## Status

- Struct → Arrow conversion and IPC: done and tested (`go test ./...`). Go-written files are
  read by arrow-rs 58 + Lance 13: see `spikes/spike-a/src/bin/go_interop.rs`.
- `Stage`: tested against the S3 Express bucket.
- `Save`, `DeletePartitions`, `Partitions`, `Wait`: tested against an in-memory fake of the service
  contract (`save_test.go`): unchanged partitions skipped, replace not append, batch splitting,
  failed batches, deletes.
- Against the real service: see the end-to-end status in `docs/decisions.md` (ADR-008).
  `Query` / `Explain` need `lance-query`, which is not built yet.

Example: `go run ./examples/sales` (stage only) or `go run ./examples/sales -create -save`.
