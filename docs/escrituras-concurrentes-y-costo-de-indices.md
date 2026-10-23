# Escrituras concurrentes y costo de los índices — diagnóstico y alternativas

Fecha: 2026-10-23. Origen: la copia histórica de monitoreos de Iceberg a Lance (smartberry,
`backend-golang/handlers_monitoreos/sync-monitoreos-iceberg-a-lance.go`). Se programaron 592
ejecuciones, con concurrencia 3, sobre `san_monitoreos_detalle` (493 M filas) y
`san_monitoreos_semana` (161 M filas).

Versión analizada: el commit `95ac4ef`, que es el desplegado (`smartberry-lance-ingest` y
`smartberry-lance-control`, LastModified 2026-10-22 22:15 UTC). Lance 13.0.0, DataFusion 54.

## 1. Síntomas

Datos de la ventana 2026-10-23 01:30–14:00 UTC.

| Dónde | Qué | Cantidad |
|---|---|---|
| `smartberry-3-go` (copia) | Ejecuciones fallidas | 26 |
| | — `create_table` → `smartberry-lance-control` `Sandbox.Timedout` (120 s) | 14 |
| | — `replace_partitions` → `TABLE_BUSY` | 12 |
| `smartberry-lance-ingest` | Invocaciones (REPORT) | 5.965 |
| | — `Runtime.OutOfMemory` | 60 |
| | — `timeout` (900 s) | 18 |
| | — "table lock renewal failed; lock lost" | 16 |
| | — "AIMD throttle: rate reduced due to throttle errors" (S3 Express) | presente |
| | — Max Memory Used de 4.093 a 4.095 MB, incluso en invocaciones de 600 ms | habitual |
| `smartberry-lance-control` | Invocaciones que terminaron en `timeout` (120 s) | 14 de 123 |

### Latencia de `replace_partitions`

La latencia crece con el tamaño de la tabla. Es la columna `elapsed_ms` de la línea `ok` de
`handler.rs`. Cada escritura lleva siempre una sola partición, de tamaño parecido.

| Tramo (UTC) | `_detalle` p50 / p90 (s) | `_semana` p50 / p90 (s) |
|---|---|---|
| 00–02 | 0,9 / 1,5 | 1,9 / 3,7 |
| 02–04 | 1,6 / 3,1 | 6,8 / 23,6 |
| 04–06 | 2,3 / 5,6 | 20,2 / 60,6 |
| 06–08 | 2,4 / 9,5 | 30,5 / 91,0 |
| 08–10 | 2,9 / 38,3 | 35,1 / 123,6 |
| 12–14 | 5,1 / 67,7 | 63,0 / 133,2 |

`_semana` tiene un tercio de las filas de `_detalle` y tarda más de diez veces. La diferencia está
en los índices:
- `_semana` tiene 5: BITMAP en `filial_id`, `semana` y `productor_id`; BTREE en `modulo_id` y
  `sector_id`.
- `_detalle` tiene 3: BITMAP en `filial_id`, `semana` y `productor_id`.

## 2. Causas

### 2.1 Cada escritura reescribe todos los índices: costo O(tabla) por commit

`service/src/ops/write.rs` → `apply` → `ensure_indexes` llama, después de cada commit y con el lock
tomado, a `ds.optimize_indices(&OptimizeOptions::default())`.

En Lance 13, `OptimizeOptions::default()` deja `num_indices_to_merge = None`. La función
`select_segments_to_merge` (`lance-13.0.0/src/index/append.rs`) lo toma como `unwrap_or(1)`: fusiona
el último segmento con los fragmentos nuevos. Como la tabla tiene un solo segmento por índice, cada
escritura vuelve a escribir el índice completo:
- BTREE: `open_and_merge_segments` sobre el segmento entero;
- BITMAP: `reference_index.update(...)` sobre el índice entero.

El trabajo bajo el lock crece con la tabla, y la carga completa resulta cuadrática. Esto explica:
- la curva de latencia del §1;
- que `_semana` sea la más lenta, porque tiene dos BTREE;
- probablemente los OOM, porque la fusión de un índice de cientos de millones de filas no cabe en
  4 GB junto con el batch.

Lance ya ofrece la alternativa: `OptimizeOptions::append()` (`num_indices_to_merge = Some(0)`) crea
un segmento delta solo con los fragmentos nuevos, tanto para BTREE como para BITMAP. Lo verifica su
propio test en `append.rs` (≈ línea 3629: "Bitmap optimize append() must add a delta segment, not
merge").

### 2.2 `create_table(if_not_exists)` toma el lock y no cabe en la función control

`service/src/ops/control.rs` → `create_table`:
- hace `app.lock(...)` antes de comprobar si la tabla existe;
- si existe, calcula `info::table_info`, que incluye `count_rows`, `versions()`, `load_indices`,
  el listado de `data/` y el registro.

La espera del lock es `service.lock_wait_seconds = 300`, pero la función control tiene
`timeout_seconds = 120`. Si otra escritura retiene el lock (§2.1), la control muere a los 120 s
esperándolo.

En smartberry, `aws/lance.go` → `tablaLance` llama a `Create` (IfNotExists) la primera vez que
cada proceso escribe en una tabla. Por eso cada arranque en frío de `smartberry-3-go` pasa por aquí.

### 2.3 Concurrencia sin beneficio

El lock es por tabla, así que tres escritores sobre las mismas dos tablas se serializan igual. La
concurrencia solo agrega espera (`TABLE_BUSY`) y alarga el lease de quien muere (OOM, timeout): el
lock queda tomado hasta que vencen sus 120 s. Lo único que se gana en paralelo es la lectura de
Athena en el lado de smartberry.

### 2.4 Lock en S3 Express bajo throttling

`service/src/lock.rs` renueva el lease cada `lease/3` con un PUT `If-Match` en el mismo bucket donde
escriben los datos. Con el bucket en throttling (avisos AIMD), la renovación falla y el escritor
pierde el lock a mitad de trabajo ("lock lost"). Eso produce `LOCK_LOST` y un reintento.

### 2.5 La copia aborta su grupo ante el primer error

`CopiarMonitoreosIcebergALance` devuelve error en la primera partición que falla y no intenta las
demás semanas de la ejecución. Nada la reintenta: hay que volver a programarla.

## 3. Alternativas

### A. Servicio (athena-lance)

| # | Idea | Qué resuelve | Esfuerzo | Riesgo / costo |
|---|---|---|---|---|
| A1 | **Índices en modo append** en `ensure_indexes`: `OptimizeOptions::append()`. | §2.1: el costo por escritura pasa a O(partición escrita). Es la causa raíz de la latencia y probablemente de los OOM. | Una línea, más una prueba. | Cada escritura agrega un segmento por índice; miles de segmentos encarecen las consultas. Requiere A2. |
| A2 | **Fusión periódica de segmentos** en `compact`, o en una op `optimize_indices` aparte: `num_indices_to_merge = Some(n)` cuando `index_statistics` supere un umbral. Programarla después de cargas masivas y a diario. | Mantiene acotado el número de segmentos de A1. | Bajo; `compact` ya corre bajo lock. | Una fusión completa cuesta O(tabla), pero se hace una vez y no en cada escritura. |
| A3 | **Índices fuera del lock de escritura:** commitear los datos, liberar el lock y refrescar los índices en una invocación aparte (NoWait) o diferida. | §2.1 y §2.3: el lock dura solo lo que dura la escritura de datos. | Medio (nueva op, idempotencia). | Mientras tanto, los fragmentos nuevos quedan sin índice: Lance los recorre completos (flat scan) en las consultas. Los datos son correctos, pero las consultas son más lentas hasta el refresco. |
| A4 | **`create_table(if_not_exists)` sin lock y sin `table_info`:** abrir la tabla, comparar spec y esquema, y responder. El lock solo hace falta para crear. | §2.2. | Bajo. | Ninguno: comparar no escribe. |
| A5 | **Timeout de control > `lock_wait_seconds`**, o una espera de lock propia de la control (por ejemplo 60 s) que devuelva `TABLE_BUSY` antes de morir. | §2.2 como red de seguridad. | Config. | Un TABLE_BUSY explícito es reintentable; un `Sandbox.Timedout` no se distingue de un fallo real. |
| A6 | **Memoria de ingest a 8–10 GB** (más vCPU), y límites explícitos de los cachés de la `Session` de Lance (`index_cache_size`, `metadata_cache_size`). | OOM; los contenedores tibios quedan en 4.095 MB. | Config + medir. | Mayor costo por ms, compensado en parte si cada invocación dura menos. No arregla §2.1. |
| A7 | **No materializar el batch entero:** `read_source` concatena todo el staging en un solo `RecordBatch` y `sort` lo copia. Pasar a un stream ordenado por partición (ya se escribe un archivo por partición). | Pico de memoria de las particiones grandes (1,37 M filas de la filial 1). | Medio. | — |
| A8 | **Lock en DynamoDB** (escritura condicional con TTL, en `smartberry-lance-ledger`) en vez de S3 Express. | §2.4: el lock no compite con el tráfico de datos ni sufre su throttling. | Medio. | Un servicio más en el camino crítico, aunque el ledger ya depende de él. |
| A9 | **Carga masiva sin índices:** crear la tabla sin `indexes` en el spec, cargar todo y agregarlos al final con un solo `create_index` (o `update_schema`, ADR-015, aún sin desplegar). | Elimina el costo de índices durante una migración. | Bajo si ADR-015 está desplegado. | Las consultas no usan índices hasta el final; para una carga previa al uso, no importa. |
| A10 | **ZONEMAP en vez de BTREE** para `modulo_id` y `sector_id`. Las filas de cada archivo de partición ya van ordenadas por `productor_id, modulo_id`, y los sectores pertenecen a un módulo, así que quedan contiguos. Un zonemap guarda min/max por zona y se actualiza por fragmento. | Reduce el costo de los dos índices más caros (§1). | Cambiar el tag y recrear o actualizar índices. | Medir antes si el pruning por zona alcanza para Gráficos y Top Fenómenos. |
| A11 | **Pruning de particiones en la capa DataFusion, sin índices sobre las llaves de partición.** Con un archivo por partición (ADR-012), un `TableProvider` propio podría traducir los filtros sobre `PartitionKeys` (`=`, `IN`, `BETWEEN`) a la lista de fragmentos (`Scanner::with_fragments`, ya usado en `info.rs`) a partir de un mapa partición → fragmentos guardado al commitear (registro o config del dataset). Hoy `ops/query.rs` registra un `LanceTableProvider` genérico. | Quita los dos BITMAP de llave de partición que cada tabla reescribe en cada escritura (ADR-012). | Alto. | Un camino de consulta propio que mantener. Se justifica si A1 + A2 no alcanzan. |

### B. Cliente (smartberry)

| # | Idea | Qué resuelve | Esfuerzo |
|---|---|---|---|
| B1 | **Crear la tabla solo si falta:** escribir directo y llamar a `Create` únicamente ante `TABLE_NOT_FOUND`, en vez de en cada arranque de proceso (`aws/lance.go`, `tablaLance` / `tablasLanceCreadas`). | §2.2 sin tocar el servicio: la función control sale del camino de escritura. | Bajo. |
| B2 | **Reintentos con backoff** en `SaveRecordsToLance` ante `TABLE_BUSY`, timeout de control y fallos de función (`Runtime.OutOfMemory`; el batch es idempotente por `batch_id`, ADR-008). | §2.3, §2.4: los fallos transitorios dejan de abortar la ejecución. | Bajo. |
| B3 | **La copia sigue con las semanas siguientes** ante un error y reporta las fallidas al final, en vez de abortar el grupo (§2.5). | Menos reprogramaciones. | Bajo. |
| B4 | **Un Save por ejecución y no por semana:** juntar las semanas del grupo (≤ 3 M filas) en un solo `Save`. lancego lo parte en requests de ≤ `MaxRowsPerRequest` (1 M) sin cortar particiones. | Con §2.1 vigente, el costo de índices es por commit: de 3 a 5 veces menos commits. | Bajo. Sube el pico de memoria de la ingest (ver A6/A7). |
| B5 | **Concurrencia 1 para la escritura**, con lectura de Athena en paralelo (por ejemplo, una ejecución lee la semana siguiente mientras escribe la actual). | §2.3: sin esperas de lock entre copias. | Medio. Sin el pipeline, concurrencia 1 a secas solo pierde el paralelismo de Athena. |
| B6 | **El cron de monitoreos** (`SyncSanidadMonitoreosCotejo`) hace un Save por filial-semana y por tabla: con §2.1, cada corrida de 30 particiones son 60 commits O(tabla). Juntar en un Save por tabla por corrida. Antes existían los lotes Iceberg, que se quitaron por memoria (B4 tiene el mismo límite). | El costo del sync diario, que seguirá creciendo con la tabla. | Medio. |

### C. Índices locales por partición

Pregunta: con partición = filial-semana y un archivo por partición (ADR-012), ¿se pueden hacer los
índices y las operaciones locales a cada partición?

**Qué ofrece Lance 13.** No tiene un índice "por valor de partición". Lo que sí tiene son
**segmentos**: un índice con nombre está formado por segmentos y cada uno lleva su
`fragment_bitmap` (`lance-13.0.0/src/index/append.rs`). Con A1 (`OptimizeOptions::append()`), cada
escritura agrega un segmento que cubre solo su fragmento nuevo, es decir, solo su partición. Eso ya
hace local la **escritura**.

No hace local la **consulta**:
- el planificador evalúa el filtro sobre todos los segmentos del índice: 2.000 particiones son
  2.000 segmentos por índice;
- un BITMAP sobre una llave de partición, segmentado así, es un bitmap por archivo con un único
  valor;
- al reemplazar una partición, su segmento viejo queda cubriendo un fragmento borrado (enmascarado)
  hasta la siguiente fusión;
- por eso A1 necesita A2, y la fusión vuelve a ser global.

**Alternativa que encaja con este modelo: ZONEMAP en todas las columnas filtradas.** El ZONEMAP
(`lance-index-13.0.0/src/scalar/zonemap.rs`) ya está organizado por fragmento: guarda una fila por
zona (`fragment_id`, `zone_start`, `zone_length`, min, max), con `rows_per_zone` = 8.192 por
defecto. Esto implica:
- **Llaves de partición:** con un archivo por partición, cada fragmento tiene `min = max` en
  `filial_id` y `semana`. El ZONEMAP descarta los archivos de las demás particiones para `=`, `IN` y
  rangos (`SargableQuery::Equals`, `IsIn`, `Range`), sin el BITMAP global y sin un `TableProvider`
  propio (A11).
- **Dentro de la partición:** las filas van ordenadas por `productor_id, modulo_id` (agregando
  `sector_id` al sort key). El ZONEMAP de esas columnas salta las zonas de los demás productores,
  módulos o sectores. Que un productor no se repita entre filiales no es requisito: basta con que
  dentro del archivo esté contiguo.
- **Costo de mantenimiento:** es metadato chico, del orden de filas / 8.192: ~20 mil zonas en
  `_semana` y ~60 mil en `_detalle`. Aunque `update` reescriba la lista de zonas, son MB y no un
  BTREE o BITMAP de cientos de millones de entradas.
- **Columnas que no conviene indexar:** las que no están agrupadas por el orden (`indicador_id`,
  `cultivo_id`) no se indexan. Se filtran al recorrer las pocas zonas que quedan.

**Cambios necesarios:**
- `spec.rs`: hoy agrega un BITMAP a cada llave de partición salvo que se declare BTREE/BITMAP.
  Habría que aceptar también ZONEMAP como índice suficiente para una llave de partición.
- En smartberry: los tags `lance` de los structs y el sort key `productor_id, modulo_id, sector_id`.
- Cambiar los índices de una tabla ya creada obliga a recrearla (o a usar `update_schema`, ADR-015,
  aún sin desplegar) y a recargarla.

**Antes de decidir:** medir con `explain` (el servicio reporta `bytes_read` y `get_requests`) la
consulta de Gráficos sobre una tabla de prueba con ZONEMAP y compararla con la actual.

## 4. Recomendación

1. **Ahora (servicio): A1 + A4 + A5.** Son cambios chicos que atacan la causa raíz de la latencia
   (§2.1) y el error de `create_table` (§2.2). A2 va enseguida, en el mismo deploy o el siguiente,
   antes de que se acumulen los segmentos.
2. **Ahora (smartberry): B1 + B2 + B3.** No dependen del deploy del servicio y bastan para terminar
   la migración aunque el servicio siga igual.
3. **Medir después de A1:** latencia p50/p90 por tabla y Max Memory Used de la ingest. Si los OOM
   siguen, A6/A7.
4. **Evaluar:** A10 (ZONEMAP) para los índices de `_semana`, y A8 si los "lock lost" no bajan con
   menos tiempo bajo lock.
5. **A11** solo si, con A1 + A2, el mantenimiento de los BITMAP de llave de partición sigue pesando.

## 5. Estado de la copia al escribir esto

- En Lance: 2.221 de 2.279 particiones de `_detalle` y 2.174 de 2.281 de `_semana`.
- Pendientes: 105 particiones, reprogramadas en 35 ejecuciones el 2026-10-23 13:32 UTC.
- 10 de esas ejecuciones coinciden en hash con filas pendientes del 01:36, que `ScheduleExec` no
  renueva (`core/types.go`, `ScheduleExec`: un hash pendiente no se actualiza). Sin renovar su
  `updated`, el despachador deja de verlas a las 12 h.

## 6. Implementación (servicio desplegado el 2026-10-23, lancego v0.1.3)

Decisión: ZONEMAP en todos los índices de las tablas de monitoreos (sección C). Registro en ADR-016.

Servicio (athena-lance):

- `ensure_indexes` (`service/src/ops/write.rs`) refresca según el tipo. ZONEMAP se actualiza en su
  único segmento (`OptimizeOptions::merge(1)`): el `update` de Lance conserva las zonas viejas y
  entrena solo las filas nuevas. Fusionar dos segmentos de zone map, en cambio, relee la columna
  entera (`can_merge_segments` en lance `index/append.rs`), por lo que la alternativa A1 (append)
  no conviene para este tipo. BITMAP y BTREE usan append (A1) con fusión al pasar
  `compaction.max_index_segments` (32) segmentos (A2).
- Un ZONEMAP declarado sobre una llave de partición reemplaza el BITMAP automático
  (`spec::validate`).
- `create_table` if_not_exists responde sin tomar el lock cuando la tabla ya existe con el mismo spec
  (A4). `create_table` y `drop_table` esperan el lock como máximo
  `service.control_lock_wait_seconds` (60 s), por debajo del timeout de la función control (A5).
- `compact` construye los índices del spec que falten (`indexes_built`).
- Prueba: `zonemaps_stay_one_segment_and_bitmaps_merge_past_the_limit`.

Cliente (smartberry, `backend-golang/aws/lance.go`):

- Tags `index=zonemap` en `filial_id`, `semana`, `productor_id`, `modulo_id` y `sector_id` de
  `MonitoreoDetalleIceberg` y `MonitoreoSemanaIceberg`. El sort key no cambia.
- `tablaLance`: ante `TABLE_EXISTS_SPEC_MISMATCH` llama `CreateOrUpdate`, que lleva la tabla al spec
  del struct sin reescribir datos (ADR-015). Así se convierten las tablas existentes en la primera
  escritura después del deploy (B1).
- `saveLanceReintentando`: reintenta `TABLE_BUSY` a los 30 s y a los 90 s (B2). Los errores de
  resultado incierto ya los reintenta lancego.
- `CopiarMonitoreosIcebergALance` continúa con las semanas siguientes y con la otra tabla ante un
  error, y los reporta todos al final (B3).
- `RecrearTablaLance` pasa a `DropTable` + `Create`, porque `CreateOptions.Replace` ya no existe.

Orden de despliegue:

1. Deploy del servicio (ADR-015 + ADR-016).
2. Release de lancego (`CreateOrUpdate`, `DropTable`, `CompactResult.IndexesBuilt`) y bump de
   `go.mod` en smartberry. Sin ese bump, smartberry no compila con `GOWORK=off`, que es como compila
   CI.
3. Deploy de smartberry. La primera escritura de cada tabla ejecuta `update_schema` en la función
   ingest: elimina los BITMAP/BTREE y construye los cinco ZONEMAP sobre toda la tabla (lectura de cinco
   columnas enteras; en `_detalle`, 493 M filas). Si no termina en 900 s, cada paso es su propio
   commit y la siguiente llamada continúa desde donde quedó (ADR-015).
