# Status — 2026-10-22

Servicio de tablas Lance sobre S3 Express One Zone, en AWS Lambda arm64 (Graviton), con cliente Go.
Detalle de diseño y verificaciones: `docs/decisions.md` (ADR-001 … ADR-009).

## Hecho y verificado

### Infraestructura (cuenta <aws-account-id>, us-west-2, perfil `<aws-cli-profile>`)
- Bucket S3 Express: `smartberry-lance-data--usw2-az1--x-s3` (sin VPC en el MVP).
- DynamoDB `smartberry-lance-ledger` (on-demand, TTL en `ttl`): estado de batches + registro de particiones.
- Lambdas `smartberry-lance-control` (1 GB, 120 s) y `smartberry-lance-ingest` (3 GB, 900 s, 2 GB /tmp),
  `provided.al2023` arm64, mismo binario.
- Bucket policy en el bucket de datos que da `s3express:CreateSession` al rol compartido
  `<lambda-role>` (sin cambios de IAM).
- El zip (56 MB) supera el límite de subida directa (50 MB): se despliega desde
  `s3://smartberry-artifacts/lance-lambda/function.zip`.
- Todo valor de entorno del servicio y del despliegue vive en `config.toml` (la librería Go no lo lee).

### Restricciones de la cuenta y cómo se resolvieron
- **Sin SQS** (no se pueden crear colas): Go invoca `smartberry-lance-ingest` de forma síncrona; la
  respuesta llega después del commit, así que un error nil significa que la data ya está visible.
  `SaveOptions.NoWait` usa invocación asíncrona + `Wait` (polling de `get_batch`).
- **Una escritura por tabla a la vez:** lock lease en el mismo bucket (`locks/<tabla>.json`) con
  escrituras condicionales de S3 Express, renovado mientras trabaja. Si otro lo tiene, espera hasta
  300 s y luego `TABLE_BUSY` con el dueño en `details`.
- **Idempotencia:** reenviar el mismo `batch_id` no duplica (`already_applied`); Go reintenta solo
  cuando el resultado es incierto (red, `INTERNAL`, `LOCK_LOST`).
- Sin CloudFormation ni creación de roles IAM; nombres de Lambda deben empezar con `smartberry`.

### Servicio Rust (`service/`)
- Ops: `create_table`, `replace_partitions` (delete + insert atómico en un solo commit Lance),
  `ingest` (append, tablas sin particiones), `compact`, `table_info`, `describe_table`,
  `list_tables`, `list_partitions`, `get_batch`, `query`, `explain`.
- `query` / `explain` (ADR-009): SQL de solo lectura con DataFusion; las tablas se resuelven por
  nombre en cada consulta (sin catálogo que refrescar), con caché de datasets y chequeo de versión
  cada `query.freshness_seconds` o al pedir `min_versions`. Límites de filas, de bytes de respuesta
  (`truncated`), timeout y memoria; stats con bytes leídos, GETs y versión leída por tabla.
- `compact`: agrupa archivos pequeños contiguos respetando a la vez max MB por archivo y max
  archivos por grupo; además limpia versiones de más de 24 h (lo que borra archivos reemplazados).
- `table_info`: versión, filas y filas borradas; cantidad y tamaño de archivos por rangos; archivos no
  usados por la versión actual y versiones limpiables; cobertura de índices y los que faltan;
  preview de compactación; resumen del registro de particiones; lock y últimas 20 operaciones
  (pendientes / fallidas). Con `detail=true`: por archivo min/max de columnas de partición y orden
  (calidad de pruning) y en cuántos archivos queda cada partición.
- `lance-cli`: mismo handler en local contra el bucket y DynamoDB reales.

### Cliente Go (`lancego/`, módulo `github.com/ivanjoz/athena-lance/lancego`)
- `TableDef` + `NewTable[T]` con tags `lance:"col"` / `lance:"*"` (snake_case).
- `Save` por partición con hash de 8 bytes (salta particiones sin cambios), `DeletePartitions`,
  `Partitions`, `Insert`, `Compact`, `TableInfo`, `ListTables`.
- La librería no lee archivos ni variables de entorno: el backend arma `lancego.Config` (funciones,
  bucket, región, límites) desde su propia configuración y lo pasa a `New` / `NewWithAWS`.
  `Config.LocalCLI=<ruta a lance-cli>` ejecuta el servicio en local en vez de Lambda; el ejemplo
  `examples/sales` lo toma de `LANCEGO_CLI` y lee el `config.toml` del repo como lo haría un backend.

### Pruebas
- Local con `lance-cli` contra el bucket y DynamoDB: Save de 6 particiones / 3000 filas en un commit;
  re-save sin cambios no envía nada (0.7 s); cambiar una fila reemplaza solo su partición; borrado de
  partición; compactación 4 → 1 archivo con índices remapeados; `batch_id` repetido →
  `already_applied`; `TABLE_BUSY` y toma de lock vencido; dos escrituras concurrentes serializadas.
  Historial de versiones verificado aparte con pylance.
- Desde Go contra las Lambdas desplegadas: `table_info` 2.0 s con cold start (init 260 ms); Save de 3
  particiones 3.4 s en el cliente (commit 0.8–1.1 s dentro de la Lambda, 114 MB); `compact` 3 → 1.
- 14 tests unitarios Rust (4 de query sobre una tabla Lance local) y tests Go (fake del servicio y
  decodificación de filas de query) pasan.

### Por qué dos Lambdas
Mismo binario; se separan por configuración: timeout (900 s escrituras vs 120 s metadata), memoria y
costo (3 GB vs 1 GB, Lambda cobra por GB·ms), concurrencia aislada (si el ingest se satura, las
consultas de estado siguen respondiendo) y logs/métricas separados. Se pueden unificar sin tocar
código apuntando `control_function` e `ingest_function` al mismo nombre en `config.toml`.

## Pendiente

### Archivo por partición (ADR-012, `docs/plan-archivo-por-particion.md`)
- Código escrito en el servicio (`write.rs`, `spec.rs`, `compact.rs`, `info.rs`, `control.rs`,
  `ledger.rs`), **sin compilar ni desplegar**. Falta:
  `cargo test -p lance-service`, `deploy/deploy.sh`, recrear las tablas de meteorología
  (`agroclima/cmd lance -recrear -ejecutar`, que ahora debe usar `DropTable` + `Create` porque
  `CreateOptions.Replace` ya no existe) y repetir las queries de prueba.

### Cambios de esquema (ADR-015)
- `update_schema`, `create_or_update`, `drop_table` en el servicio (`ops/schema.rs`) y
  `UpdateSchema`, `CreateOrUpdate`, `Client.DropTable` en lancego; `Replace` eliminado. Tests Go
  pasan; Rust **sin compilar ni desplegar**. Falta `cargo test -p lance-service`, desplegar y probar
  contra una tabla real: agregar una columna, quitar otra, cambiar un tipo (debe rechazarse), y
  que un Save con el struct nuevo funcione sobre los archivos viejos.

### Funcionalidad
- **Query en AWS**: el código está hecho (servicio + Go: `Query`, `Explain`,
  `Table.Query` → `[]T`, `DecodeRows`, `MinVersions` con `SaveResult.Version()`). Las queries
  corren en `smartberry-lance-ingest` (ADR-011: dos funciones, timeout de 30 s interno); falta
  redesplegar (`deploy/deploy.sh`, pasa a 4096 MB) y medir latencia real dentro de AWS.
  Ya verificado en local contra S3 Express con `lance-cli` y con el ejemplo Go
  (`LANCEGO_CLI=… go run ./examples/sales -query -explain`): agregados, `[]Venta` tipado,
  truncado, DML rechazado, y el `explain` muestra el índice BTREE en `WHERE id = 42`.
- **Limpieza de staging**: los archivos `.arrows` subidos a `staging/` no se borran después del commit
  (borrarlos al confirmar el batch o con una expiración).
- **Compactación periódica**: hoy solo corre cuando Go llama `Compact`; la limpieza de versiones viejas
  depende de eso. Evaluar un disparo programado (EventBridge) si la cuenta lo permite, o llamarlo desde
  el backend Go tras los Save.
- **Escrituras async (`NoWait`)**: configurar un destino on-failure / reintentos para invocaciones
  asíncronas fallidas.
- Carrera a nivel de aplicación: dos `Save` concurrentes del mismo conjunto de filas pueden re-enviar
  la partición del otro (último en escribir gana). El servicio serializa correctamente; evitarlo es
  responsabilidad del backend (un solo proceso por tabla) o comparar hash/versión antes de enviar.

### Calidad y operación
- Tests de integración automatizados del servicio (hoy son pruebas manuales con `lance-cli` y el
  ejemplo `lancego/examples/sales`) y CI.
- Alarmas / métricas en CloudWatch (errores, duración, `TABLE_BUSY`).
- Pruebas de carga: particiones grandes (cerca de `max_rows_per_request`), muchas tablas, memoria real
  necesaria en ingest.
- Binario de 139 MB (límite de Lambda 250 MB descomprimido): vigilar el tamaño si se agregan
  dependencias.

### Limpieza y administración
- Push del repo a GitHub (`origin` = github.com/ivanjoz/athena-lance) — no hecho.
- Borrar datos de prueba del bucket: `spike-a/`, `spike-compaction/`, `spike-partitions/`, `staging/`,
  tabla `ventas`.
- Bucket antiguo `lance-data--use1-az4--x-s3` (cuenta personal <personal-aws-account-id>, us-east-1): ya no se usa,
  se puede eliminar.
- Las credenciales MFA del perfil `<aws-cli-profile>` son temporales; renovarlas cuando venzan.
