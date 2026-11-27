# Sistema Predictivo Big Data en Power BI

El informe `Sistema_Predictivo_Big_Data.pbix` contiene tres páginas. La copia actual utiliza datos importados desde PostgreSQL local y dos archivos JSON del pipeline.

## Vigilancia epidemiológica

- **Fuente:** `public.fact_vigilancia_clima`, con las dimensiones `dim_tiempo` y `dim_ubigeo`.
- **Indicadores:** total de casos de IRA, total de casos de malaria, defunciones y total de distritos. La página muestra tendencias semanales, casos de IRA por grupo de edad y visuales de temperatura y precipitación promedio.
- **Filtros:** año, semana epidemiológica, provincia y distrito.

## Estado del pipeline

- **Fuente:** `Control_Lotes_Local`, consulta de Power Query que carga `data/control/batch_control.json`.
- **Indicadores:** total de lotes, lotes procesados, porcentaje de procesamiento y volumen de datos en MB. La tabla de detalle incluye ID del lote, fuente, estado, nombre de archivo, fecha de carga y hash SHA-256.
- **Filtros:** fuente, estado y fecha de carga. La tabla PostgreSQL `lotes` está en el modelo, pero esta página se alimenta de `Control_Lotes_Local`.

## Predicciones ML

- **Fuentes:** `public.predicciones_vigilancia` de PostgreSQL y `results/estadisticas/metricas_evaluacion.json`, importado como `metricas_evaluacion`.
- **Indicadores:** total de predicciones, casos reales, casos predichos y error residual promedio. Los visuales comparan casos reales y predichos y muestran RMSE y MAE del modelo y del baseline de persistencia, R² del modelo y reducciones de RMSE y MAE.
- **Filtros:** año, semana epidemiológica, provincia y distrito. Las métricas del JSON corresponden a la evaluación del entrenamiento y el JSON carece de claves para desglosarlas por esos filtros.

## Relaciones dimensionales

| Dimensión, lado 1 | Tablas, lado muchos | Clave |
|---|---|---|
| `dim_ubigeo` | `fact_vigilancia_clima` y `predicciones_vigilancia` | `ubigeo` |
| `dim_tiempo` | `fact_vigilancia_clima` y `predicciones_vigilancia` | `tiempo_id` |

`Control_Lotes_Local` y `metricas_evaluacion` son fuentes independientes de las dimensiones de vigilancia.

## Configuración y actualización en Power BI Desktop

El modelo parametrizado utiliza tres parámetros de texto en Power Query.

| Parámetro | Valor que debe configurar el operador | Consultas |
|---|---|---|
| `PostgresServidor` | Nombre del servidor PostgreSQL y puerto cuando corresponda, por ejemplo `localhost:5432` en desarrollo local. | `dim_tiempo`, `dim_ubigeo`, `fact_vigilancia_clima`, `lotes` y `predicciones_vigilancia`. |
| `PostgresBaseDatos` | Nombre de la base PostgreSQL donde se ejecutó Serving. | Las mismas cinco consultas de PostgreSQL. |
| `RutaProyecto` | Ruta absoluta del directorio raíz del proyecto en el equipo que actualizará el informe. | `Control_Lotes_Local` y `metricas_evaluacion`. |

1. Ejecuta Serving para poblar PostgreSQL y comprueba que existan `data/control/batch_control.json` y `results/estadisticas/metricas_evaluacion.json` bajo `RutaProyecto`.
2. En Power BI Desktop, abre el informe y ve a **Transformar datos → Administrar parámetros**. Ajusta los tres valores al entorno en el que vas a actualizar el informe.
3. Pulsa **Cerrar y aplicar**. Si Power BI solicita credenciales, configúralas en **Configuración de origen de datos**. Los parámetros contienen destinos y rutas. Las credenciales se administran por separado.
4. Pulsa **Actualizar**, verifica las tres páginas y guarda en `powerbi/Sistema_Predictivo_Big_Data.pbix`. Antes de compartir el archivo, confirma que la copia guardada contiene los cambios y revisa los valores de parámetros y los datos importados.

## Uso local y futura conexión a AWS

- **Local.** Los parámetros de PostgreSQL apuntan a la base de desarrollo. `RutaProyecto` se usa para encontrar los dos JSON en el equipo donde se actualiza el informe.
- **AWS.** Configura los mismos parámetros de PostgreSQL con el endpoint privado de RDS y la base que usa Serving. Con VPN y TLS habilitados, sigue la migración descrita abajo para que los lotes y las métricas provengan también de RDS.
- **Publicación.** El informe conserva datos importados, incluidos agregados epidemiológicos y metadatos de lotes como nombres de archivos, hashes y fechas. Revisa su autorización de difusión y los valores grabados en los parámetros. El modelo parametrizado elimina `destination_path` de las consultas de lotes.

## Sustituir las fuentes JSON por PostgreSQL

Cuando la carga de AWS esté validada, editar las **dos consultas
existentes** en el Editor avanzado de Power Query. Conservar los nombres
de consulta, los pasos posteriores al origen, los alias de las columnas y
los tipos finales. Así las medidas DAX, las relaciones y los visuales
siguen apuntando a las mismas tablas y columnas. Usar el endpoint y la
base que ya funcionan mediante VPN y TLS, con un rol de lectura.

### Auditoría de lotes

Sustituir el paso que actualmente produce el registro JSON raíz por el
siguiente. Reutiliza `PostgresServidor` y `PostgresBaseDatos`.
Mantener el nombre real de ese paso en el PBIX.

```powerquery
Origen = Json.Document(
    PostgreSQL.Database(
        PostgresServidor, PostgresBaseDatos,
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
    PostgresServidor, PostgresBaseDatos,
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

Para preparar el esquema de RDS y recuperar evidencia de EMR, consulta la
[guía de auditoría y métricas en AWS](../docs/arquitectura/aws_auditoria_metricas.md).
