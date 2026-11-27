# Conexión y Visualización en Power BI Desktop

Este directorio contiene las especificaciones y guía de conexión para los tableros de control de **Power BI Desktop** del Sistema Predictivo Big Data (La Libertad) desplegado en **Amazon Web Services (Amazon RDS PostgreSQL)** o en entorno Local.

---

## 1. Opciones de Conexión

### Opción A: Conexión a Amazon RDS PostgreSQL en AWS (Producción)
Tal como se describe en el diagrama de arquitectura, Power BI se conecta a la instancia administrada de **Amazon RDS PostgreSQL** a través del puerto 5432 (vía VPN o Security Group / ruta de red autorizada):

1. **Requisito previo en AWS Console**:
   * En la VPC de la instancia de **Amazon RDS PostgreSQL**, asegurar que el **Security Group** de entrada permita el tráfico por el puerto `5432` desde la IP pública autorizada del equipo o mediante conexión VPN corporativa/privada.
2. **Abrir Power BI Desktop**:
   * Seleccionar **Obtener datos** -> **Base de datos PostgreSQL**.
   * **Servidor:** `<ENDPOINT_RDS>:5432` (o `localhost:5432` en entorno local).
   * **Base de datos:** `epidemiologia_db`.
   * **Modo de conectividad:** `DirectQuery` (para visualizar en tiempo real datos y estado del pipeline) o `Importar` (con actualización periódica programada).
   * **Credenciales:** Usuario `postgres` y contraseña configurada en `.env`.
3. **Seleccionar las tablas del modelo**:
   * `dim_ubigeo`: Dimensión geográfica de distritos de La Libertad y presencia de estaciones SENAMHI.
   * `dim_tiempo`: Dimensión temporal estructurada por año, semana epidemiológica, trimestre y mes.
   * `fact_vigilancia_clima`: Tabla de hechos consolidada con casos de IRA, defunciones, Malaria y mediciones SENAMHI.
   * `predicciones_vigilancia`: Predicciones de Machine Learning frente a valores reales observados y error residual.
   * `lotes`: Registro de auditoría del pipeline (archivo, hash SHA-256, fecha de carga y estado actual del lote).

### Opción B: Conexión Directa a Archivos Parquet (Modo Local sin Servidor)
1. Abrir **Power BI Desktop**.
2. Seleccionar **Obtener datos** -> **Parquet**.
3. Navegar a las carpetas curadas en `data/curated/epidemiologia_clima/`:
   * `dim_ubigeo/`
   * `dim_tiempo/`
   * `fact_vigilancia/`
   * `features_ml/`

---

## 2. Modelo Relacional en Power BI (Esquema en Estrella)

Relacionar en la vista de modelo de Power BI:
* `dim_ubigeo[ubigeo]` (1) $\longrightarrow$ $(\infty)$ `fact_vigilancia_clima[ubigeo]`
* `dim_tiempo[tiempo_id]` (1) $\longrightarrow$ $(\infty)$ `fact_vigilancia_clima[tiempo_id]`
* `dim_ubigeo[ubigeo]` (1) $\longrightarrow$ $(\infty)$ `predicciones_vigilancia[ubigeo]`
* `dim_tiempo[tiempo_id]` (1) $\longrightarrow$ $(\infty)$ `predicciones_vigilancia[tiempo_id]`

---

## 3. Páginas del Dashboard de Power BI

De acuerdo con el diagrama de arquitectura del proyecto:

1. **Página 1: Vigilancia Epidemiológica Departamental**:
   * Mapa coroplético por distrito de La Libertad (filtrable por año y semana epidemiológica).
   * Curvas epidemiológicas semanales de IRA y Malaria.
   * Correlación climática: Gráficos multieje comparando Temperatura media / Precipitación rezagada vs Casos.
2. **Página 2: Estado del Pipeline (Auditoría de Ingesta y Lotes)**:
   * Tarjetas KPI con total de lotes procesados y última fecha de carga.
   * Tabla de auditoría (`lotes`): Nombre de archivo, hash SHA-256, fuente, estado (`REGISTERED`, `PROCESSED_SILVER`, `PROCESSED_GOLD`) y fecha de actualización.
3. **Página 3: Monitoreo de Modelos Predictivos y Alertas Tempranas**:
   * Comparativa de casos reales vs predicción del modelo de Spark MLlib.
   * Semáforo de alerta temprana distrital ante anomalías predictivas.

## Sustituir las fuentes JSON por PostgreSQL

Cuando la carga de AWS esté validada, editar las **dos consultas
existentes** en el Editor avanzado de Power Query. Conservar los nombres
de consulta, los pasos posteriores al origen, los alias de las columnas y
los tipos finales. Así las medidas DAX, las relaciones y los visuales
siguen apuntando a las mismas tablas y columnas. Usar el endpoint y la
base que ya funcionan mediante VPN y TLS, con un rol de lectura.

### Auditoría de lotes

Sustituir el paso que actualmente produce el registro JSON raíz por el
siguiente. `ServidorRDS` y `BaseRDS` son parámetros de Power Query.
Mantener el nombre real de ese paso en el PBIX.

```powerquery
Origen = Json.Document(
    PostgreSQL.Database(
        ServidorRDS, BaseRDS,
        [Query = "
            SELECT jsonb_build_object(
                'batches',
                COALESCE(jsonb_agg(to_jsonb(l) ORDER BY l.ingest_timestamp, l.batch_id), '[]'::jsonb)
            )::text AS documento
            FROM public.lotes AS l
        "]
    ){0}[documento]
),
```

Conservar la expansión de `batches` y los pasos de tipado. Si una lista
vacía rompe la inferencia de columnas existente, materializar la lista
con `Table.FromRecords(Origen[batches], ColumnasOriginales, MissingField.UseNull)`,
donde `ColumnasOriginales` es la lista explícita del paso de expansión
del PBIX. La tabla de origen incluye `batch_id`, `source`, `file_name`,
`file_hash`, `file_size_bytes`, `load_date`, `ingest_timestamp`,
`destination_path`, `status` y `updated_at`. Con RDS vacía, la auditoría
debe mostrar cero lotes.

### Métricas de evaluación

El paso siguiente mantiene el mismo registro anidado que leía el JSON.
Selecciona la evaluación más reciente por su fecha real, con un desempate
estable. Si el tablero debe mostrar una ejecución específica, añadir
`WHERE run_id = 'ID-DE-LA-EJECUCION'` antes de `ORDER BY`.

```powerquery
FilasMetricas = PostgreSQL.Database(
    ServidorRDS, BaseRDS,
    [Query = "
        SELECT (reporte - 'run_id')::text AS documento
        FROM public.metricas_evaluacion
        ORDER BY timestamp_evaluacion DESC, run_id DESC
        LIMIT 1
    "]
),
Origen = if Table.IsEmpty(FilasMetricas)
    then error "RDS todavía no contiene una evaluación verificable."
    else Json.Document(FilasMetricas{0}[documento]),
```

Conservar los pasos que expanden `split_temporal`, `metricas_modelo`,
`metricas_baseline_persistencia` y `ganancia_vs_baseline`. La conversión
`Json.Document` lee texto devuelto por PostgreSQL. Las consultas dejan de
depender de archivos JSON locales. Aplicar este cambio de métricas
cuando exista al menos una evaluación AWS verificable en RDS.

Antes de aplicar los cambios, comparar en vista previa los nombres,
tipos y número de columnas con el último paso de las consultas originales.
Después de actualizar, comprobar los totales y las medidas DAX en las
páginas de auditoría y monitoreo. Aprobar la consulta SQL nativa en Power
BI si aparece la solicitud. Consultar
[PostgreSQL.Database](https://learn.microsoft.com/en-us/powerquery-m/postgresql-database).
