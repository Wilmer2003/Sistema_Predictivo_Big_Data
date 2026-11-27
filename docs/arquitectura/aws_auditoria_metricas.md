# Despliegue de auditoría y métricas en AWS

En local se conservan `data/control/batch_control.json` y
`results/estadisticas/metricas_evaluacion.json`. En AWS, `public.lotes` es
el registro operativo y cada snapshot de auditoría contiene las filas reales
de RDS en el momento en que se publica. Se guarda en
`control/runs/<run_id>/<bronze|silver|gold>/batch_control.json`. El reporte
del entrenamiento se publica en
`results/estadisticas/runs/<run_id>/metricas_evaluacion.json` y luego se
registra transaccionalmente en `public.metricas_evaluacion`.

Un mismo reporte y `run_id` se pueden importar varias veces. Si el reporte
cambia, hace falta un nuevo ID para un nuevo entrenamiento. Si S3 acepta el
reporte y PostgreSQL falla, se puede reintentar el registro con el mismo
archivo e ID. Cuando falla el snapshot de lotes después de un commit, la
transacción confirmada en RDS sigue vigente y se puede exportar de nuevo.
S3 y PostgreSQL tienen confirmaciones independientes.

## 1. Aplicar únicamente el esquema y publicar el código

Ejecutar desde Bash en la cuenta que opera el clúster. Partir de una copia
que integre estos cambios con el `main` remoto. Revisar el diff antes de
publicar. El bloque prepara la tabla nueva y carga el código bajo un prefijo
versionado. No presenta steps ni escribe en las tablas dimensionales.

Usar una sesión `psql` con las mismas condiciones de red y TLS que pgAdmin.
El rol de migración debe tener permiso para crear la tabla. Usar la CA y el
endpoint DNS de RDS. Autenticar por el mecanismo habitual del equipo.

```bash
set -euo pipefail
export AWS_REGION='REGION-DEL-CLUSTER'
export S3_BUCKET='sistema-predictivo-bigdata-lalibertad'
export EMR_CLUSTER_ID='j-ID-DEL-CLUSTER'
export EMR_SCRIPTS_PREFIX='scripts/auditoria-metricas-v1'

export PGHOST='ENDPOINT-RDS'
export PGPORT=5432
export PGDATABASE='BASE'
export PGUSER='ROL-MIGRACION'
export PGSSLMODE=verify-full
export PGSSLROOTCERT='/ruta/ca-rds.pem'

aws sts get-caller-identity
sed -n '/^CREATE TABLE IF NOT EXISTS public.metricas_evaluacion (/,/^);/p' \
  src/serving/schema.sql | psql -X -v ON_ERROR_STOP=1 --single-transaction -f -
psql -X -v ON_ERROR_STOP=1 -c '\d+ public.metricas_evaluacion'

python3 -m zipfile -c pipeline_src.zip src
aws s3 cp pipeline_src.zip "s3://$S3_BUCKET/$EMR_SCRIPTS_PREFIX/pipeline_src.zip"
aws s3 cp src/orchestration/run_pipeline.py \
  "s3://$S3_BUCKET/$EMR_SCRIPTS_PREFIX/run_pipeline.py"
```

El administrador debe conceder `USAGE` sobre `public` y permisos
`SELECT, INSERT, UPDATE` en `public.lotes` al rol que ejecuta
Bronze/Silver/Gold. Para el rol de entrenamiento bastan `SELECT, INSERT` en
`public.metricas_evaluacion`. Para Power BI usar `SELECT` en ambas.
Conservar los permisos existentes de Serving para sus tablas. En AWS el
esquema se administra en el despliegue. Para una base completamente nueva,
el administrador debe aplicar el `schema.sql` completo antes de ejecutar
etapas del pipeline.

Preparar las dependencias de `requirements.txt` en el Python que usa
Spark en el clúster. En cada nodo candidato a alojar el driver, aprovisionar
el mismo archivo protegido de configuración, por ejemplo
`/etc/sistema-predictivo/pipeline.env`, legible por el usuario de aplicación
de YARN. Usar el mecanismo de secretos del equipo para proveer
`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_SSLMODE=verify-full` y
`POSTGRES_SSLROOTCERT`. La CA debe estar disponible también donde se ejecute
JDBC. El lanzador pasa a YARN la ruta del archivo y la configuración AWS
no secreta. Mantener las credenciales fuera del ZIP, los argumentos de EMR
y los logs. Los roles AWS necesitan acceso de lectura al código y datos
pertinentes, escritura en los prefijos de salida y lectura del JSON de
métricas para verificar reintentos. Si se usa SSE-KMS, aplicar también los
permisos de la clave correspondiente.

### Próxima ejecución autorizada

Ejecutar Bronze desde un equipo con los CSV de origen, acceso privado a
RDS, credenciales AWS y el archivo protegido de configuración del emisor.
Para nuevos lotes, la clave en S3 es estable por contenido
(`bronze/<fuente>/sha256=<hash>/lote.csv`). Antes de reingestar archivos
antiguos, reconciliar los objetos Bronze ya presentes. Con `lotes` vacía,
la detección de duplicados no conoce esos objetos históricos y ambos
prefijos podrían entrar al mismo glob de Silver. Evitar `--force` en una
primera conciliación.

```bash
export PIPELINE_ENV=aws
export PIPELINE_CONFIG_FILE='/ruta/protegida/emisor.env'
export EMR_PIPELINE_CONFIG_FILE='/etc/sistema-predictivo/pipeline.env'
export PIPELINE_RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"

# Solo si corresponde ingerir archivos nuevos.
python3 -m src.orchestration.run_pipeline --ingest

# Ejemplo de una ejecución futura acotada. Usa features_ml existentes en Gold.
# Genera un entrenamiento NUEVO y omite la carga dimensional de Serving.
python3 -m src.orchestration.run_pipeline --emr --model
```

El éxito que devuelve el lanzador confirma el registro del step.
Verificar el estado final en EMR y buscar `run_id`, las URI de los
artefactos y el mensaje de registro en PostgreSQL en los logs de la ejecución.
El modelo y las predicciones conservan sus destinos operativos. Si el
objetivo es solo instalar código, ejecutar únicamente el bloque de la
sección anterior.

El registro de lotes puede quedar vacío si el despliegue original hizo
solo Silver, Gold, modelo y Serving sobre objetos ya presentes en S3.
También podía quedar vacío si el emisor de Bronze carecía de acceso a RDS
y el código anterior usaba el respaldo del filesystem. Para distinguir
esos casos revisar el comando del emisor, los argumentos del step y sus
logs. Los estados históricos se mantienen tal como estén documentados
por las fuentes originales.

## 2. Recuperar evidencia de una ejecución anterior

Desde el primario, para un step existente:

```bash
STEP='s-ID-DEL-STEP'
sudo zgrep -hEo 'application_[0-9]+_[0-9]+' \
  /mnt/var/log/hadoop/steps/"$STEP"/* | sort -u
APP='application_ID'
yarn applicationattempt -list "$APP"
yarn logs -applicationId "$APP" 2>&1 |
  grep -Ei 'Reporte de métricas|Modelo ML|Baseline|registro local|lotes'
ATTEMPT='appattempt_ID'
yarn applicationattempt -status "$ATTEMPT"
```

En el host del ApplicationMaster, buscar en la ruta registrada por el
driver o en su disco. Si se usó deploy mode `client`, buscar en el host
que lanzó Spark. Buscar un respaldo histórico del control también en el
equipo que ejecutó la ingesta.

```bash
sudo find /mnt /var /tmp /home /opt -type f \
  \( -name metricas_evaluacion.json -o -name batch_control.json \) -print
```

Si el JSON original se conserva, verificar su hora y procedencia antes de
copiarlo con SSH/SCP. Si el nodo o contenedor desapareció, consultar los
logs archivados en el `LogUri` del clúster. Los valores sueltos de los logs
pueden servir para documentar la corrida. Si falta parte del reporte,
mantener el historial de métricas sin fila para esa ejecución.

Desde el emisor autorizado, después de preparar el esquema, importar
únicamente un JSON completo y atribuible a AWS. Reutilizar el mismo ID
para repetir la importación. Para reportes del código nuevo, conservar
el ID que ya contiene el reporte.

```bash
export PIPELINE_ENV=aws
export PIPELINE_CONFIG_FILE='/ruta/protegida/emisor.env'
export PIPELINE_RUN_ID='ID-ESTABLE-DE-LA-EJECUCION-AWS'
python3 -m src.serving.metricas /ruta/recuperada/metricas_evaluacion.json

# Exportar el estado actual de public.lotes a S3, bajo una ejecución de recuperación.
PIPELINE_RUN_ID='recovery-IDENTIFICADOR' python3 -c \
  'from src.ingestion.batch_manager import BatchControlManager; BatchControlManager().publish_snapshot("recovery")'
```

Si `public.lotes` está vacía, ese snapshot contiene una lista vacía. Si
existe un respaldo auténtico de la ingesta AWS, conservarlo como evidencia
histórica y cotejar cada SHA-256, URI y estado con el objeto y los logs
antes de considerar una carga histórica aprobada. Los objetos de Bronze
por sí solos no prueban que Silver o Gold procesaron un lote. La
recuperación no usa archivos de una ejecución local para poblar AWS.

Para copiar la evidencia si aún existe, ejecutar desde el equipo con SSH
al host correspondiente. La copia de auditoría recuperada se guarda en
un prefijo separado de los snapshots de RDS.

```bash
scp -i /ruta/llave.pem 'hadoop@HOST:/ruta/metricas_evaluacion.json' ./metricas_evaluacion.json
scp -i /ruta/llave.pem 'hadoop@HOST:/ruta/batch_control.json' ./batch_control.json
python3 -m json.tool batch_control.json >/dev/null
aws s3 cp batch_control.json "s3://$S3_BUCKET/control/recovered/$PIPELINE_RUN_ID/batch_control.json" \
  --content-type application/json
```

Para verificar lectura de ambos prefijos y los registros en PostgreSQL:

```bash
aws s3 ls "s3://$S3_BUCKET/control/runs/" --recursive
aws s3 ls "s3://$S3_BUCKET/results/estadisticas/runs/" --recursive
psql -X -v ON_ERROR_STOP=1 -c 'SELECT source, status, count(*) FROM public.lotes GROUP BY source, status'
psql -X -v ON_ERROR_STOP=1 -c 'SELECT run_id, timestamp_evaluacion, modelo, rmse, baseline_rmse, artifact_uri FROM public.metricas_evaluacion ORDER BY timestamp_evaluacion DESC'
```

La migración posterior del PBIX se describe en [Power BI](../../powerbi/README.md).
