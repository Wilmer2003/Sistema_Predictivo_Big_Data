# Preparación para Amazon EMR en EC2

Estos pasos preparan un clúster existente con Spark 3.5 y YARN. El driver necesita acceso a los prefijos de S3 mediante su identidad de ejecución y conectividad privada a RDS PostgreSQL. Los comandos de migración, publicación y recuperación están en la [guía de auditoría y métricas en AWS](../docs/arquitectura/aws_auditoria_metricas.md).

## Dependencias y paquete de despliegue

1. Prepara `requirements.txt` e `install_emr_dependencies.sh` de la misma revisión en cada nodo. Usa la misma versión de Python y la misma ruta absoluta del entorno virtual en el primario y los workers. Si se usa una acción de bootstrap, entrega primero el archivo de requisitos a cada nodo.

   ```bash
   bash infra/install_emr_dependencies.sh requirements.txt "$EMR_VENV"
   ```

   El instalador usa `EMR_PYTHON` si está definido y omite el PySpark de pip porque EMR lo proporciona. Instala las dependencias de AWS y PostgreSQL y comprueba `psycopg2`. La configuración de Spark también resuelve `org.postgresql:postgresql:42.7.3` desde Maven o su caché.

2. Empaqueta el código y el esquema desde una copia de la revisión que vas a ejecutar. El ZIP incluye `src/serving/schema.sql` y excluye la configuración privada, los datos y el PBIX.

   ```bash
   python3 infra/package_emr.py "$EMR_SRC_ZIP"
   ```

   Para `run_emr.sh`, coloca el ZIP y una copia de la revisión en el primario. Para el despachador `--emr`, publica el ZIP como `pipeline_src.zip` junto a `run_pipeline.py` bajo el prefijo S3 definido en `EMR_SCRIPTS_PREFIX`. Usa `S3_BUCKET`, `AWS_REGION` y `EMR_CLUSTER_ID` del despliegue.

## RDS, TLS e identidad de ejecución

- **Esquema.** Antes de ejecutar etapas en AWS, aplica el DDL de `public.metricas_evaluacion` en una instalación existente. Para una base nueva, aplica todo `src/serving/schema.sql`. El rol de migración prepara el esquema. Concede al rol de lotes `SELECT, INSERT, UPDATE` sobre `public.lotes`, al de entrenamiento `SELECT, INSERT` sobre `public.metricas_evaluacion` y a ambos `USAGE` sobre `public`. Mantén los permisos de Serving sobre las tablas dimensionales y de predicciones. Power BI usa permisos de lectura.
- **Conexión.** Configura de forma privada `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER` y `POSTGRES_PASSWORD`. Define `POSTGRES_PORT` si difiere de `5432`. Usa el endpoint DNS con `POSTGRES_SSLMODE=verify-full` y `POSTGRES_SSLROOTCERT` apuntando a la CA de RDS. `POSTGRES_SCHEMA` configura el destino de Serving. Las tablas de auditoría y métricas usan `public`.
- **Credenciales.** Mantén los secretos fuera del ZIP y de los argumentos de Spark. En modo remoto, `EMR_PIPELINE_CONFIG_FILE` debe apuntar a un archivo protegido en la misma ruta en cada nodo candidato a alojar el driver. El usuario de YARN debe poder leer ese archivo y la CA. La identidad AWS necesita leer el código y los datos, escribir en los prefijos de salida y leer el JSON de métricas para verificar reintentos. Si se usa SSE-KMS, incluye los permisos de la clave.
- **Identificador.** Define `PIPELINE_RUN_ID` una vez y reutilízalo entre Bronze y los pasos EMR de la misma corrida. Si se omite, cada proceso que inicia sin un identificador genera uno. Usa un ID nuevo para un entrenamiento nuevo. Para reintentar el registro de un reporte ya publicado, conserva el mismo JSON y el mismo ID.

## Ejecución futura y destinos

En el nodo primario, con Bronze disponible y las variables del driver configuradas, `run_emr.sh` inicia el driver en modo cliente y los ejecutores en YARN.

```bash
export PIPELINE_RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
bash infra/run_emr.sh "$EMR_VENV" "$EMR_SRC_ZIP" --silver --gold --model --serving
```

Si el ID ya se definió al ingerir Bronze, conserva ese valor. Para cargar Gold y predicciones existentes, ejecuta el lanzador con `--serving`. Si usas el despachador `--emr`, el driver se ejecuta en modo clúster. Consulta el estado final del step en EMR y coteja el `run_id` y las URI de los artefactos en los logs.

| Artefacto | Destino en modo AWS |
|---|---|
| Registro operativo de lotes | `public.lotes` en RDS |
| Respaldo de lotes | `s3://<S3_BUCKET>/control/runs/<run_id>/<etapa>/batch_control.json`, con `bronze`, `silver` o `gold` según la etapa completada |
| Datos Gold | `s3://<S3_BUCKET>/gold/epidemiologia_clima/` |
| Predicciones escritas por Modelado y leídas por Serving | `s3://<S3_BUCKET>/gold/epidemiologia_clima/predicciones_vigilancia` |
| Modelo Spark MLlib | `s3://<S3_BUCKET>/modelos/rf_ira_model` |
| JSON de evaluación | `s3://<S3_BUCKET>/results/estadisticas/runs/<run_id>/metricas_evaluacion.json` |
| Métricas consultables | `public.metricas_evaluacion` en RDS, con `run_id` como clave primaria |

El entrenamiento publica el JSON en S3 y registra transaccionalmente el mismo reporte en RDS. Si falla el registro tras la publicación, la [guía de recuperación](../docs/arquitectura/aws_auditoria_metricas.md#2-recuperar-evidencia-de-una-ejecución-anterior) describe cómo cargar ese JSON sin volver a entrenar.
