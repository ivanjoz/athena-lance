# Plan: un archivo por partición e índices en las llaves de partición

Fecha: 2026-10-22. Estado: implementado, sin compilar el servicio (Rust) ni desplegar.

## Problema

Primera carga de `meteorologia_mediciones_climaticas` y `meteorologia_resumen_diario` (ETL Iceberg → Lance de
smartberry, `handlers_metereologia/agroclima/lance.go`):

| | Iceberg | Lance (hoy) |
|---|---|---|
| Archivos de la horaria | 1289, uno por (ubicacion_id, anio) | 163, uno por ubicación con sus 8 años |
| `WHERE ubicacion_id = 4 AND anio = 2026 AND fecha_hora …` (24 filas) | lee 1 archivo | abre los 163 archivos, 73 MB, 163 GET, ~385 ms |

Dos causas, las dos en el servicio:

1. **La partición es lógica, no física.** `apply()` (`service/src/ops/write.rs`) escribe todo el lote de
   un `replace_partitions` con UN `InsertBuilder`, que solo corta archivos por cantidad de filas (1 M).
   El ETL manda una ubicación con todos sus años por llamada → un archivo por ubicación.
2. **La query no sabe qué archivo tiene qué partición.** `LanceTableProvider` recorre el dataset entero.
   Lance 13 no descarta archivos por min/max de columna; solo con índices escalares, y las tablas no
   tienen ninguno. Cada query lee las columnas del filtro de las 10,9 M de filas.

## Lo que consultan las pantallas (smartberry)

`handlers_metereologia/meteorologia-satelite-campo.go` y `meteorologia-satelite-historico.go`: todas las
consultas son de **una sola ubicación**.

| Query | Tabla | Filtro |
|---|---|---|
| Ancla `max(fecha)` | diaria | `ubicacion_id = X` |
| Ventanas actual / anterior | diaria | `ubicacion_id = X AND anio IN (…) AND fecha` en dos rangos |
| ET0 de las ventanas | horaria | igual, `SUM(eto_fao) GROUP BY fecha` |
| Años de referencia (diaria y ET0) | ambas | `ubicacion_id = X AND anio IN (hasta N años)` + rangos de `fecha` |
| Histórico semanal | diaria | `ubicacion_id = X AND anio = Y` |

Ninguna filtra por `fecha_hora`.

## Cambios

### 1. Servicio: un archivo por partición (`ops/write.rs`)

En `replace_partitions`, el lote ya validado y ordenado se separa por llave de partición
(`split_by_partition`) y cada grupo se escribe con su propio `InsertBuilder::execute_uncommitted`. Todos
los fragmentos nuevos van en la MISMA transacción `Update` que el borrado, así que el reemplazo sigue
siendo un único commit atómico. `ingest` (tablas sin partición) no cambia.

Consecuencia: una partición = un archivo. Al reemplazarla, todas las filas del archivo viejo coinciden
con el filtro de borrado y Lance quita el fragmento entero (`deleted_fragment_ids`) en vez de escribir un
archivo de borrados. No se acumulan borrados con el sync diario.

### 2. Servicio: índice BITMAP automático en cada llave de partición (`spec.rs`)

`validate()` agrega `<llave>_bitmap` para cada llave de partición que no tenga ya un BTREE o BITMAP
declarado (y cuyo tipo admita índice escalar). Toda tabla particionada descarta archivos por partición
sin que nadie tenga que declararlo. BITMAP por la cardinalidad: ~162 ubicaciones, 8 años. El servicio ya
construye y refresca los índices declarados en cada escritura (`ensure_indexes`).

Con esto, `ubicacion_id = 4 AND anio IN (2025, 2026)` se resuelve en el índice y lee solo los 2 archivos
de esas particiones.

### 3. Servicio: la compactación no mezcla particiones (`ops/compact.rs`, `ops/info.rs`)

La compactación juntaba archivos chicos vecinos sin mirar la partición: deshacía el punto 1 en la
primera corrida. En tablas particionadas ya no fusiona nada: cada partición es un archivo y su reemplazo
quita el viejo entero, así que no hay nada que juntar. `compact` solo limpia versiones viejas, y
`table_info` deja de recomendarla. Las tablas sin partición siguen igual.

### 4. Servicio: recrear una tabla (`create_table` con `replace`)

(Reemplazado por ADR-015: `update_schema` y `drop_table`.) `create_table` acepta `replace: true` (excluyente con `if_not_exists`): si la tabla existe, la sobrescribe
con una versión vacía (`WriteMode::Overwrite`) con el esquema y spec nuevos, y borra su registro de
particiones en DynamoDB. Es `Overwrite` y no un borrado del prefijo porque así las versiones siguen
subiendo: el caché de datasets de las queries detecta la versión nueva como cualquier escritura, y una
"versión 3" nueva nunca se confunde con la 3 vieja. `Overwrite` vacía también los índices
(`lance-table` `manifest_build.rs`); los archivos viejos los borra la limpieza de versiones.

### 5. lancego

- `CreateOptions.Replace`.
- README: partición física, índice automático, `Replace`.

### 6. ETL de smartberry

- `fecha` con `index=zonemap` en `MedicionDatalake` y `DiarioDatalake`: las ventanas filtran rangos de
  `fecha` dentro del año. `fecha_hora` sin índice (ninguna query la filtra).
- Opción `Recrear` (`-recrear` en el CLI, `p7=recrear` en `fn_agroclima_lance`): recrea las tablas con
  `Replace` y, como el registro queda vacío, el cotejo marca todo pendiente y recarga.

## Despliegue y recarga

1. Compilar y probar el servicio (`cargo test -p lance-service`), desplegar (`deploy/deploy.sh`).
2. `go run ./handlers_metereologia/agroclima/cmd lance -env prod -recrear -paralelo 2 -ejecutar`.
3. Verificar con `table_info detail`: 1289 fragmentos por tabla, `max_fragments_per_partition = 1`,
   índices `ubicacion_id_bitmap`, `anio_bitmap`, `fecha_zonemap` construidos.
4. Repetir las 5 queries de prueba y comparar archivos leídos, bytes y tiempo.

## Costos asumidos

- Más archivos: 1289 por tabla en vez de 163. Una query de toda la tabla hace más GET (un recorrido
  completo de la horaria pasaría de ~486 a ~3900). Ninguna pantalla lo hace.
- Cada escritura construye o refresca 3 índices (un commit extra cada uno).
