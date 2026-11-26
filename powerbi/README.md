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
- **AWS.** Con acceso autorizado a RDS, configura los parámetros de PostgreSQL para ese entorno. Para que la página de lotes refleje el pipeline en AWS, habrá que adaptar sus consultas y visuales a la tabla `lotes` de RDS. El JSON de evaluación se genera en el driver y debe ubicarse en una ruta accesible para Power BI.
- **Publicación.** El informe conserva datos importados, incluidos agregados epidemiológicos y metadatos de lotes como nombres de archivos, hashes y fechas. Revisa su autorización de difusión y los valores grabados en los parámetros. El modelo parametrizado elimina `destination_path` de las consultas de lotes.
