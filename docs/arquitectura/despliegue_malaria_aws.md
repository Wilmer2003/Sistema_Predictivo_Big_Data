# Despliegue AWS de vigilancia de malaria

Este procedimiento se ejecuta desde el equipo autorizado después de aprobar
el cambio. Publica una revisión independiente y conserva activos los
artefactos Gold de IRA, las predicciones, las métricas y las tablas existentes.
Usar el perfil habitual de AWS y la conexión privada de RDS con verificación
TLS. La migración utiliza el esquema `public`.

## 1. Empaquetar y aplicar la migración

Desde la rama de corrección, con los cambios ya confirmados y revisados:

~~~powershell
$Bucket = 'sistema-predictivo-bigdata-lalibertad'
$Region = 'REGION-DEL-BUCKET-Y-EMR'
$Version = (git rev-parse HEAD).Trim()
$Run = "malaria-2015-2024-$($Version.Substring(0,12))-v1"
$Sha = '1996211cc340a20116f5566115723bf5cc361146a93f1e9de7ebc321247ce636'
$Bronze = "s3://$Bucket/bronze/malaria/fecha_ingesta=2026-11-26/malaria_limpio.csv"
$Scripts = "s3://$Bucket/scripts/malaria/$Version"
$RutaRun = "s3://$Bucket/gold/vigilancia_malaria/runs/$Run"

.\infra\publicar_artefactos_malaria.ps1 -Bucket $Bucket -Region $Region -Python .\venv\Scripts\python.exe
psql -X --single-transaction -v ON_ERROR_STOP=1 -f src/serving/migrations/001_vigilancia_malaria.sql
~~~

El SQL crea tablas específicas de malaria, la vista activa y la función
de recuperación. Usar el rol de migración de la base. El rol emisor
necesita TEMP en la base, SELECT e INSERT en publicaciones, dimensiones,
hechos e historial de cambios nuevos, SELECT y UPDATE sobre el puntero
activo y permiso de secuencia en el historial. Para rollback, el
operador necesita SELECT sobre publicaciones y puntero, UPDATE del
puntero e INSERT en historial con permiso de secuencia. El rol de BI
solo necesita SELECT sobre la vista nueva, además de sus permisos
vigentes. Los ejecutores de la función necesitan asimismo sus permisos
de tabla, dado que la función usa SECURITY INVOKER.

## 2. Preparar Silver, Gold y manifiesto

Usar el EMR **ya disponible** solo si la política de operación lo permite.
El paso requiere acceso al Bronze y los scripts, y permiso de escritura
en el prefijo de esa revisión. Usar la misma configuración de Spark y
Python aprobada para el clúster. El paso no requiere acceso a PostgreSQL.
Bronze debe permanecer inmutable durante la lectura. Usar una clave
protegida o una copia versionada del objeto cuyo SHA-256 fue verificado.

~~~powershell
$Cluster = 'j-ID-DEL-CLUSTER-EXISTENTE'
$SparkArgs = @(
  'spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster',
  '--conf', 'spark.yarn.appMasterEnv.PIPELINE_ENV=aws',
  '--conf', "spark.yarn.appMasterEnv.S3_BUCKET=$Bucket",
  '--conf', "spark.yarn.appMasterEnv.AWS_REGION=$Region",
  '--conf', 'spark.yarn.appMasterEnv.SPARK_MASTER=yarn',
  '--py-files', "$Scripts/pipeline_src.zip", "$Scripts/run_malaria.py",
  'preparar', '--run-id', $Run, '--bronze', $Bronze, '--sha256', $Sha,
  '--b50', '13', '--b51', '477'
)
$Steps = @(@{
  Name = "Malaria Silver y Gold $Run"
  ActionOnFailure = 'CONTINUE'
  HadoopJarStep = @{ Jar = 'command-runner.jar'; Args = $SparkArgs }
})
$StepFile = Join-Path ([IO.Path]::GetTempPath()) ("emr-malaria-" + [guid]::NewGuid() + ".json")
[IO.File]::WriteAllText($StepFile, (ConvertTo-Json -InputObject $Steps -Depth 8), [Text.UTF8Encoding]::new($false))
$Step = aws emr add-steps --region $Region --cluster-id $Cluster --steps "file://$StepFile" --query 'StepIds[0]' --output text
if ($LASTEXITCODE -ne 0) { throw 'No se registró el step.' }
aws emr wait step-complete --region $Region --cluster-id $Cluster --step-id $Step
if ($LASTEXITCODE -ne 0) { throw 'El step no terminó correctamente. Revisa los logs antes de continuar.' }
aws s3 ls "$RutaRun/manifest/"
~~~

Si no hay un clúster operativo, Spark local puede preparar la misma
revisión desde una descarga del Bronze. Ejecutar con PIPELINE_ENV=local y
DATA_DIR apuntando a una carpeta temporal nueva, y usar el comando
`python -m src.orchestration.run_malaria preparar` con el archivo local y los
mismos controles. Conservar la publicación en el entorno local para
validación. Para promover artefactos a AWS con su manifiesto y sus URIs
correctas, usar el driver con configuración AWS y Spark local en un host
con conector Hadoop S3 configurado y acceso a los objetos autorizados.

El driver crea `silver/`, `gold/` y `manifest/`
en la revisión. Si falla
la preparación antes de generar el manifiesto, guardar ese prefijo para
diagnóstico y repetir con otro run ID. Tras completarse el manifiesto,
los reintentos de Serving usan el mismo run ID. La revisión de malaria se
audita con el manifiesto y el historial transaccional de publicaciones.

## 3. Publicar únicamente malaria en RDS

Descargar los dos conjuntos finales al equipo autorizado. Configurar
previamente la conexión PostgreSQL por el mecanismo privado habitual de
AppConfig, incluida la CA de RDS y POSTGRES_SSLMODE. Verificar el destino
con `psql` y, después, consultar el puntero activo.

~~~powershell
$Cache = Join-Path ([IO.Path]::GetTempPath()) ("malaria-serving-" + [guid]::NewGuid())
aws s3 cp "$RutaRun/gold/" "$Cache/gold/" --recursive --only-show-errors
if ($LASTEXITCODE -ne 0) { throw 'Falló la descarga Gold.' }
aws s3 cp "$RutaRun/manifest/" "$Cache/manifest/" --recursive --only-show-errors
if ($LASTEXITCODE -ne 0) { throw 'Falló la descarga del manifiesto.' }

psql -X -v ON_ERROR_STOP=1 -c 'SELECT run_id FROM public.malaria_publicacion_activa WHERE unica'
$ActivaEsperada = 'ninguna'
# Si la consulta devolvió un run, configurar su ID exacto.
$env:PIPELINE_ENV = 'aws'
$env:SPARK_MASTER = 'local[2]'
$env:S3_BUCKET = $Bucket
$env:AWS_REGION = $Region
.\venv\Scripts\python.exe -B -m src.orchestration.run_malaria servir --run-id $Run --sha256 $Sha --b50 13 --b51 477 --activa-esperada $ActivaEsperada --artefactos-locales $Cache
if ($LASTEXITCODE -ne 0) { throw 'No se publicó la revisión.' }
~~~

El destino autoritativo es la vista activa. Cada publicación sustituye
la revisión completa de manera transaccional y conserva la anterior.
Tras corregir una clave, la nueva revisión tiene su propio valor.
El bloqueo del puntero evita que dos revisiones con la misma versión
esperada se activen simultáneamente.

## 4. Verificar y revertir

~~~sql
BEGIN READ ONLY;
SELECT * FROM public.malaria_publicacion_activa;
SELECT run_id, sha256_bronze, uri_gold, total_registros, publicado_en
FROM public.malaria_publicaciones ORDER BY publicado_en DESC;

SELECT ano, especie_codigo, count(*) AS claves_especie,
       sum(registros_fuente) AS registros_fuente
FROM public.vw_vigilancia_malaria
GROUP BY GROUPING SETS ((ano, especie_codigo), (especie_codigo), ())
ORDER BY ano NULLS LAST, especie_codigo NULLS LAST;

SELECT count(*) AS claves_sin_hechos_legacy,
       sum(m.registros_fuente) AS registros_sin_hechos_legacy
FROM public.vw_vigilancia_malaria m
WHERE NOT EXISTS (
  SELECT 1 FROM public.fact_vigilancia_clima v
  WHERE (v.ubigeo, v.tiempo_id) = (m.ubigeo, m.tiempo_id)
);
ROLLBACK;
~~~

Si RDS conserva la cobertura Gold histórica, se esperan 12 claves sin
hecho legado y 14 registros. El manifiesto de la primera revisión debe
reconciliar 490 registros fuente, 477 B51 y 13 B50.

Desde PowerShell, para restaurar una versión previa:

~~~powershell
$Anterior = 'RUN-ANTERIOR-VALIDADO'
"SELECT public.restaurar_publicacion_malaria(:'vigente', :'anterior');" |
  psql -X -v ON_ERROR_STOP=1 --single-transaction -v "vigente=$Run" -v "anterior=$Anterior"
# Para revertir la primera publicación y dejar la vista vacía:
"SELECT public.restaurar_publicacion_malaria(:'vigente', NULL);" |
  psql -X -v ON_ERROR_STOP=1 --single-transaction -v "vigente=$Run"
~~~

Repetir las verificaciones después del rollback. La activación previa y
las revisiones siguen accesibles para auditoría. El rollback de BI es
independiente y se describe en [Power BI](../../powerbi/vigilancia_malaria.md).

## Contrato del pipeline IRA

Las etapas generales de Gold IRA conservan sus columnas históricas de
malaria como columnas de compatibilidad. Para regenerar IRA, configurar
IRA_LEGACY_MALARIA_GOLD con un Parquet Gold histórico congelado en una
ruta independiente del destino del job. Si no se configura, esas
medidas quedan nulas. Las variables predictoras de IRA, sus claves,
el objetivo y el corte temporal siguen la misma definición en ambos casos.
Para esta publicación de malaria omitir las fases generales de ingesta,
Gold IRA, Serving legado y entrenamiento. Las etapas necesarias son
`preparar` y `servir`.

El orquestador general ejecuta Bronze y Silver de IRA y SENAMHI. Los
lotes nuevos de Bronze malaria pueden registrarse por separado con
`python -m src.ingestion.cargar_malaria --archivo <csv>`. La revisión de
vigilancia valida directamente el SHA-256 del objeto Bronze seleccionado.
