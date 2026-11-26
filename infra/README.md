# Preparación para Amazon EMR en EC2

Este procedimiento prepara una ejecución futura en un clúster EMR con Spark 3.5 y YARN. Requiere un clúster existente, acceso a S3 mediante su identidad de ejecución y conectividad desde el nodo primario a RDS PostgreSQL. Los scripts de este directorio usan el entorno y los destinos que proporcione el operador.

## Dependencias y distribución de código

1. Prepara `requirements.txt` y `install_emr_dependencies.sh` de la misma revisión en cada nodo. Ejecuta el instalador con ambos argumentos. Si se incorpora a una acción de bootstrap, entrega primero el archivo de requisitos a cada nodo. Usa la misma versión de Python y la misma ruta absoluta del entorno virtual en el nodo primario y en los workers.

   ```bash
   bash infra/install_emr_dependencies.sh requirements.txt "$EMR_VENV"
   ```

   El instalador usa `EMR_PYTHON` si se configura y omite el PySpark de pip porque EMR provee Spark. Instala y comprueba `psycopg2-binary` para Serving. Spark resuelve el controlador JDBC `org.postgresql:postgresql:42.7.3` mediante la configuración existente de `get_spark_session`. El driver necesita acceso al repositorio Maven configurado o al artefacto en su caché.

2. Empaqueta `src` y el DDL desde una copia de la revisión que vas a ejecutar. El ZIP excluye la configuración local, los datos y el PBIX.

   ```bash
   python infra/package_emr.py "$EMR_SRC_ZIP"
   ```

   Usa una ruta existente para el directorio de salida. Coloca el ZIP y una copia del repositorio de esa revisión en el nodo primario. `run_emr.sh` pasa el ZIP a Spark con `--py-files`, incluido `src/serving/schema.sql`.

## Configuración y ejecución futura

Configura en el entorno privado del proceso del driver `S3_BUCKET`, `AWS_REGION`, `POSTGRES_HOST`, `POSTGRES_DB` y `POSTGRES_USER`. Inyecta `POSTGRES_PASSWORD` desde el mecanismo autorizado de secretos. Define `POSTGRES_PORT` y `POSTGRES_SCHEMA` si difieren de `5432` y `public`. El esquema de destino debe existir y el usuario debe poder crear tablas e insertar filas. Mantén las credenciales fuera del ZIP y de los argumentos de Spark.

Con los insumos Bronze disponibles, el siguiente comando está previsto para el nodo primario. El driver se ejecuta en modo cliente y los ejecutores en YARN.

```bash
bash infra/run_emr.sh "$EMR_VENV" "$EMR_SRC_ZIP" --silver --gold --model --serving
```

Si Gold y las predicciones ya existen, ejecuta el mismo comando con `--serving`. Este procedimiento usa el lanzador en el nodo primario. El despachador remoto `--emr` existente requiere aprovisionamiento independiente y queda fuera de este procedimiento.

| Artefacto | Destino en modo AWS |
|---|---|
| Datos Gold | `s3://<S3_BUCKET>/gold/epidemiologia_clima/` |
| Predicciones escritas por Modelado y leídas por Serving | `s3://<S3_BUCKET>/gold/epidemiologia_clima/predicciones_vigilancia` |
| Modelo Spark MLlib | `s3://<S3_BUCKET>/modelos/rf_ira_model` |
| JSON de evaluación | `results/estadisticas/metricas_evaluacion.json` desde el directorio de trabajo del driver |

Antes de la primera ejecución en AWS, verifica el acceso a los prefijos S3, la resolución del controlador JDBC, la instalación del entorno en los nodos y la conectividad a RDS.
