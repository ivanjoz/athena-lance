# athena-lance

Schema-driven [Lance](https://lancedb.github.io/lance/) tables on S3 Express One Zone, served by
Rust functions on AWS Lambda (arm64 / Graviton, `provided.al2023`), with a Go client that declares
the tables and writes `[]T` structs as Apache Arrow.

| Path | What |
|---|---|
| `config.toml` | every environment value (account, bucket, functions, ledger, limits) |
| `service/` | Rust service: `create_table`, `replace_partitions`, `ingest`, `compact`, `table_info`, `list_partitions`, `get_batch`… (`bootstrap` = Lambda, `lance-cli` = same handler locally) |
| `lancego/` | Go client: `TableDef` + `NewTable[T]`, `Save` by partition with hashes, `Compact`, `TableInfo` |
| `deploy/` | `setup.sh` (bucket policy, ledger TTL), `deploy.sh` (build arm64 + create/update functions) |
| `docs/decisions.md` | design decisions and verification results (ADR-001…) |
| `spikes/` | verified experiments (S3 Express commits, compaction, atomic partition replace) |
| `lance-lambda-s3express-plan.md` | the original plan |

```sh
cargo build --release -p lance-service            # local lance-cli
target/release/lance-cli '{"op":"table_info","table":"ventas"}'
deploy/setup.sh && deploy/deploy.sh               # AWS
cd lancego && go test ./... && go run ./examples/sales -create -save -info
```
