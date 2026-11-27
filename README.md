# Sistema Predictivo Big Data

## Sistema predictivo basado en Big Data para el soporte de la vigilancia epidemiológica de infecciones respiratorias agudas y malaria en La Libertad

Proyecto académico (UPAO, Big Data y Analítica de Datos) que procesa de forma incremental datos epidemiológicos y meteorológicos con Apache Spark, entrena modelos con Spark MLlib y publica los resultados en dashboards de Power BI, como soporte a la vigilancia epidemiológica en el departamento de La Libertad, Perú.

---

## 1. Descripción del proyecto

El sistema integra información histórica de:

* **Infecciones Respiratorias Agudas (IRA)** - MINSA.
* **Malaria** - MINSA.
* **Variables meteorológicas (TEMP, HR, PP)** - SENAMHI.

El periodo común de análisis es **2015 – 30/06/2024** y la unidad de integración es **Ubigeo (String 6 dígitos con padding) + Año + Semana epidemiológica**. Cuando llega un lote nuevo de datos, este atraviesa las capas Bronze, Silver y Gold de manera idempotente (verificando hash SHA-256) sin reprocesar particiones no afectadas, genera inferencias predictivas y actualiza la capa de Serving para visualización en Power BI.

---

## 2. Fuentes de datos

| Fuente | Entidad | Contenido | Periodo |
|---|---|---|---|
| **IRA** | MINSA | Episodios de IRA y neumonía por distrito y semana epidemiológica, grupos de edad y defunciones | 2000–2024 |
| **Malaria** | MINSA | Registros individuales de casos (diagnósticos B50 y B51): ubicación, año, semana, edad y sexo | 2009–2024 (La Libertad llega hasta 2022) |
| **SENAMHI** | SENAMHI | Temperatura (TEMP), humedad relativa (HR) y precipitación (PP) por estación, con fecha, hora, ubicación y Ubigeo | 01/01/2015 – 30/06/2024 |

*Los datos crudos no se suben al control de versiones (ver `data/README.md`).*

---

## 3. Arquitectura Medallion

Arquitectura de datos sobre Apache Spark (PySpark 3.5.3), compatible tanto en almacenamiento local como en Amazon Web Services (Amazon S3 vía `s3://`, Amazon EMR sobre EC2 con 1 nodo primario y 2 core en YARN, y Amazon RDS PostgreSQL):

| Capa | Carpeta local | Prefijo en Amazon S3 | Contenido y Transformaciones |
|---|---|---|---|
| **Bronze** | `data/raw/` | `s3://<bucket>/bronze/` | Lotes CSV crudos sin alterar, particionados por `fecha_ingesta=YYYY-MM-DD` con verificación de hash criptográfico SHA-256 (en tabla `lotes` de Amazon RDS o archivo local). |
| **Silver** | `data/processed/` | `s3://<bucket>/silver/` | Datos limpios, tipados estrictos en PySpark, filtrados para La Libertad (13), deduplicados por clave de negocio y guardados en **Parquet particionado por Año** (escritura dinámica de particiones). |
| **Gold** | `data/curated/` | `s3://<bucket>/gold/` | Esquema dimensional (`dim_ubigeo`, `dim_tiempo`, `fact_vigilancia_clima`) y dataset analítico con **variables rezagadas (lags de 1 a 3 semanas)**, deltas y medias móviles. |
| **Modelado** | `results/modelos/` | `s3://<bucket>/modelos/` | Pipeline de PySpark MLlib (Random Forest / GBT / Regresión Lineal) evaluado frente a un **baseline de persistencia** ($y_t = y_{t-1}$) con métricas RMSE, MAE y $R^2$. |
| **Serving** | `src/serving/` | `Amazon RDS (PostgreSQL)` | Script DDL relacional y cargador JDBC hacia Amazon RDS PostgreSQL para tableros analíticos y tabla de auditoría `lotes`. |

---

## 4. Estructura del repositorio

```text
Sistema_Predictivo_Big_Data/
├── README.md
├── CONTRIBUTING.md
├── requirements.txt
├── .env.example
├── .github/                 # CI con GitHub Actions, plantilla de PR e issues
├── data/                    # Almacenamiento Medallion local (ignorado por git)
│   ├── raw/                 # Bronze: ira/ malaria/ senamhi/ (particionado por fecha_ingesta)
│   ├── processed/           # Silver: ira/ malaria/ senamhi/ (Parquet particionado por Ano)
│   ├── curated/             # Gold: epidemiologia_clima/ (dim_ubigeo, dim_tiempo, fact_vigilancia, features_ml)
│   └── control/             # batch_control.json (auditoría local de lotes por SHA-256)
├── src/
│   ├── common/              # Configuración local/AWS (.env), sesión Spark 3.5.3, geo_utils, epi_utils y logger
│   ├── ingestion/           # BatchControlManager (JSON local o tabla lotes Amazon RDS), SHA-256 e ingesta cruda a Bronze
│   ├── preprocessing/       # Limpieza en PySpark, filtro La Libertad (13), bandeo de celda pequeña y Parquet Silver
│   ├── integration/         # Agregación semanal, lags temporales 1-3 semanas y modelo dimensional Gold
│   ├── modeling/            # Pipeline MLlib, evaluación frente a persistencia, entrenamiento e inferencia
│   ├── serving/             # schema.sql DDL (tablas Gold + lotes) y cargador JDBC a Amazon RDS PostgreSQL
│   ├── orchestration/       # run_pipeline.py (orquestador maestro local y Amazon EMR)
│   └── mapreduce/           # Scripts de referencia académica inicial
├── powerbi/                 # Guía de conexión e integración de tableros (.pbit) con Amazon RDS PostgreSQL
├── notebooks/               # Exploración interactiva de datos y gráficos
├── docs/                    # Diccionario de datos, arquitectura y evidencias
├── tests/                   # Suite de pruebas unitarias ejecutables con pytest o unittest
└── results/                 # Modelos serializados (.parquet/.ml) y métricas de evaluación (.json)
```

---

## 5. Guía Paso a Paso para Probar el Sistema en Modo Local (Windows y VS Code)

Sigue esta secuencia detallada para comprobar de inicio a fin el funcionamiento del sistema en tu propia computadora con Windows antes de conectarlo a la nube de AWS.

---

### Requisitos Previos en Windows
1. **Visual Studio Code**: Descargado e instalado.
2. **Python (64-bit)**: Versión 3.10, 3.11 o 3.12 instalada. Verifica abriendo una terminal de PowerShell y ejecutando:
   ```powershell
   python --version
   ```
   *(Asegúrate de que Python esté agregado a las variables de entorno PATH).*
3. **Java JDK 11 o 17**: Requerido por Apache Spark para las capas Silver, Gold y Modelado.
   * Recomendado: [Eclipse Adoptium Temurin JDK 11 o 17](https://adoptium.net/temurin/releases/).
   * Verifica ejecutando en tu terminal:
     ```powershell
     java -version
     ```
   * Si no tienes configurada la variable `JAVA_HOME`, agrégala en Windows o ejecútalo en la sesión de PowerShell:
     ```powershell
     $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.x.x-hotspot"
     ```
4. **Extensiones recomendadas en VS Code**:
   * *Python* y *Pylance* (Microsoft).

> [!IMPORTANT]
> **Aviso Importante sobre Rutas con Caracteres Especiales (`&`) en Windows:**
> Si la ruta donde tienes tu proyecto contiene el carácter `&` (por ejemplo: `D:\VII & VIII CICLO UPAO\...`), el comando interno de Windows `cmd.exe` (utilizado por los scripts de Apache Spark `spark-class2.cmd`) interpreta el `&` como un separador de comandos, arrojando el error:  
> `""D:\VII" no se reconoce como un comando interno o externo"`.  
>  
> **Solución rápida y limpia (sin mover tus archivos):**  
> Crea una unidad virtual temporal en Windows con el comando `subst`:
> ```powershell
> subst S: "D:\VII & VIII CICLO UPAO\BIG DATA\SISTEMA\Sistema_Predictivo_Big_Data"
> cd S:\
> ```
> O bien abre directamente la carpeta `S:\` desde VS Code (`File` -> `Open Folder` -> `S:\`). Esto elimina espacios y símbolos `&` de la ruta activa, permitiendo que PySpark funcione al 100%.  
> *(Para desmontar la unidad cuando termines: `subst S: /D`).*

---

### Paso 1: Abrir el Proyecto en VS Code
1. Abre **Visual Studio Code**.
2. Ve al menú superior: `File` -> `Open Folder...` (o `Archivo` -> `Abrir carpeta...`).
3. Selecciona la carpeta del proyecto (o la unidad `S:\` si usas la solución anterior).

---

### Paso 2: Abrir una Terminal Integrada de PowerShell
En VS Code, presiona ``Ctrl + ` `` (Ctrl + acento grave) o ve a `Terminal` -> `New Terminal`.  
Asegúrate de que la terminal sea de **PowerShell**.

---

### Paso 3: Crear y Activar el Entorno Virtual de Python
En la terminal de PowerShell, ejecuta:

```powershell
python -m venv venv
```

Si al intentar activar el entorno PowerShell te muestra el error:  
*`"la ejecución de scripts está deshabilitada en este sistema"`*, habilita la ejecución para tu sesión actual:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Ahora activa el entorno virtual:

```powershell
.\venv\Scripts\Activate.ps1
```

Verás el prefijo `(venv)` al inicio de la línea de comandos de tu terminal.

---

### Paso 4: Seleccionar el Intérprete de Python en VS Code
1. Presiona `Ctrl + Shift + P` en VS Code para abrir la paleta de comandos.
2. Escribe y selecciona: `Python: Select Interpreter`.
3. Selecciona el intérprete del entorno virtual:
   ```text
   .\venv\Scripts\python.exe
   ```

---

### Paso 5: Instalar las Dependencias
Con el entorno `(venv)` activado, actualiza `pip` e instala las dependencias del archivo `requirements.txt`:

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

### Paso 6: Configurar Variables de Entorno para Modo Local (`.env`)
El proyecto incluye la plantilla `.env.example`. Crea tu archivo `.env` ejecutando:

```powershell
Copy-Item .env.example .env
```

Abre el archivo `.env` en VS Code y confirma que la primera línea tenga:
```ini
PIPELINE_ENV=local
```
*(En modo `local`, el pipeline no requiere credenciales de AWS ni conectarse a internet; procesa todo en las carpetas de tu disco).*

---

### Paso 7: Prueba 1 - Ejecutar las Pruebas Unitarias Automatizadas
Ejecuta la suite completa de pruebas para verificar que toda la lógica de hashing, geografía, epidemiología y configuración funcione correctamente:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

En Windows, `requirements.txt` instala PySpark 3.5.9, que corrige el cierre inesperado de los workers con Python 3.12. Si Spark detecta la ruta original al usar la unidad `S:\`, configura `SPARK_HOME=S:/venv/Lib/site-packages/pyspark` y `PYSPARK_PYTHON=S:/venv/Scripts/python.exe` en `.env`.

Las pruebas de Spark ejecutan transformaciones en memoria. Para leer y escribir Parquet en Windows, coloca `winutils.exe` y `hadoop.dll` en `hadoop/bin/` y define `HADOOP_HOME=hadoop` en `.env`. Se comprobó la compatibilidad con PySpark 3.5.9 usando los binarios 3.3.5 que instala la [CI de .NET for Apache Spark](https://github.com/dotnet/spark/blob/main/azure-pipelines-pr.yml). La sesión agrega `hadoop/bin/` a la ruta de bibliotecas de la JVM al iniciar, y Git ignora ambos binarios.

*(O `pytest -q` si prefieres el reporte resumido de pytest).*

**Qué valida esta prueba:**
* **Criptografía:** Generación determinista y exacta del hash SHA-256 para archivos crudos.
* **Geografía:** Normalización y padding a 6 dígitos de Ubigeos, identificando correctamente el departamento 13 (La Libertad).
* **Epidemiología:** Reglas de asignación de semanas epidemiológicas de MINSA/SENAMHI según estándar internacional.
* **Serving y Mocks AWS:** Esquema relacional DDL (tablas dimensionales y tabla `lotes`), y mocks para Boto3 y PostgreSQL.

**Resultado esperado:**
```text
Ran 34 tests ... OK (skipped=5)
```
*(Las 5 pruebas en 'skipped' corresponden a tests de integración con clúster PySpark que requieren el runtime de Java levantado).*

---

### Paso 8: Prueba 2 - Ingesta Cruda Bronze y Verificación de Idempotencia (SHA-256)
Ejecuta la primera fase del pipeline para ingerir los datasets crudos hacia la capa Bronze:

```powershell
python -m src.orchestration.run_pipeline --ingest
```

**Qué sucede durante la ejecución:**
1. El sistema lee los archivos CSV fuente en `data/processed/` o `data/raw/`.
2. Calcula el hash criptográfico SHA-256 de cada dataset.
3. Copia los archivos a la partición Bronze correspondiente:
   * `data/raw/ira/fecha_ingesta=YYYY-MM-DD/`
   * `data/raw/malaria/fecha_ingesta=YYYY-MM-DD/`
   * `data/raw/senamhi/fecha_ingesta=YYYY-MM-DD/`
4. Registra los metadatos y el hash en el archivo de auditoría local `data/control/batch_control.json`.

**Demostración de Idempotencia (Detección de Duplicados):**  
Vuelve a ejecutar exactamente el mismo comando:
```powershell
python -m src.orchestration.run_pipeline --ingest
```
Verás que el sistema detecta que los hashes SHA-256 ya fueron registrados y emite mensajes informativos:
> `El archivo ya fue ingerido y procesado previamente con hash SHA-256... Omitiendo carga (SKIPPED_DUPLICATE).`  
Esto demuestra que el pipeline es idempotente: no duplica datos ni corrompe el historial si un lote se reenvía por error.

*(Si en algún momento deseas forzar la reingesta, puedes añadir la bandera `--force`: `python -m src.orchestration.run_pipeline --ingest --force`).*

---

### Paso 9: Prueba 3 - Limpieza y Capa Silver en PySpark
Ejecuta la limpieza, tipado estricto y deduplicación con Apache Spark:

```powershell
python -m src.orchestration.run_pipeline --silver
```

**Qué realiza esta etapa:**
* Filtra estrictamente los registros correspondientes al departamento de La Libertad (código Ubigeo que inicia en `13`).
* Aplica el rango de tiempo del periodo común (2015 al 30/06/2024).
* Aplica la regla de protección de celdas pequeñas para privacidad en salud pública.
* Escribe los datos limpios en formato **Parquet particionado por año (`Ano=YYYY`)** usando sobreescritura dinámica de particiones.
* **Dónde verificar:** Revisa las carpetas creadas en `data/processed/ira/`, `data/processed/malaria/` y `data/processed/senamhi/`.

---

### Paso 10: Prueba 4 - Integración Dimensional y Lags Temporales (Capa Gold)
Ejecuta la consolidación analítica y de ingeniería de características:

```powershell
python -m src.orchestration.run_pipeline --gold
```

**Qué realiza esta etapa:**
1. Agrega las observaciones climáticas diarias de SENAMHI (temperatura, humedad y precipitación) a nivel de semana epidemiológica por distrito.
2. Calcula variables con rezago temporal (*lags* de 1 a 3 semanas) para temperatura media, precipitación y casos de IRA previos, capturando el retardo biológico de incubación y transmisión.
3. Genera las tablas del modelo dimensional en estrella: `dim_ubigeo`, `dim_tiempo`, `fact_vigilancia` y el dataset consolidado de modelado `features_ml`.
* **Dónde verificar:** Revisa los directorios Parquet generados en `data/curated/epidemiologia_clima/`.

---

### Paso 11: Prueba 5 - Modelado Predictivo con Spark MLlib vs Baseline
Ejecuta el entrenamiento e inferencia predictiva:

```powershell
python -m src.orchestration.run_pipeline --model
```

**Qué realiza esta etapa:**
1. Divide el dataset respetando la serie temporal: Entrenamiento (2015 a 2022) y Evaluación/Prueba (2023 a 2024).
2. Entrena un modelo predictivo con Spark MLlib (Random Forest Regressor) para estimar los casos semanales de IRA.
3. Compara objetivamente las métricas de error (**RMSE, MAE y $R^2$**) del modelo frente a un **baseline de persistencia** ($y_t = y_{t-1}$).
4. Serializa el pipeline entrenado y guarda las predicciones con sus residuos.
* **Dónde verificar:**
  * Métricas de evaluación: Abre el archivo JSON `results/estadisticas/metricas_evaluacion.json`.
  * Modelo exportado: Revisa la carpeta `results/modelos/rf_ira_model/`.

---

### Paso 12: Prueba 6 - Ejecución Integral de Inicio a Fin
Si deseas ejecutar todas las fases anteriores de forma continua en una sola ejecución local:

```powershell
python -m src.orchestration.run_pipeline --all
```

---

### Paso 13: Solución de Problemas Comunes en Windows

* **Error `""D:\VII" no se reconoce como un comando interno o externo"` al ejecutar PySpark:**  
  *Causa:* La ruta de tu carpeta contiene el símbolo `&`.  
  *Solución:* Monta la unidad virtual limpia ejecutando en PowerShell:
  ```powershell
  subst S: "D:\VII & VIII CICLO UPAO\BIG DATA\SISTEMA\Sistema_Predictivo_Big_Data"
  cd S:\
  ```
  Y vuelve a correr el comando desde `S:\`.
* **Error `JAVA_HOME is not set` o `Java gateway process exited before sending its port number`:**  
  *Causa:* Falta instalar Java JDK o definir la variable `JAVA_HOME`.  
  *Solución:* Instala JDK 11 o 17 (Eclipse Adoptium) y define la variable en tu terminal:
  ```powershell
  $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.x.x-hotspot"
  ```
* **Error `No se puede cargar el archivo Activate.ps1 porque la ejecución de scripts está deshabilitada`:**  
  *Causa:* Restricción de políticas de seguridad por defecto de Windows PowerShell.  
  *Solución:* Ejecuta una sola vez en PowerShell:
  ```powershell
  Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
  ```
* **Reiniciar datos locales de prueba para volver a empezar desde cero:**  
  Si deseas limpiar los datos generados durante las pruebas locales y volver a probar la ingesta cruda:
  ```powershell
  Remove-Item -Recurse -Force data\raw\*\fecha_ingesta=* -ErrorAction SilentlyContinue
  Remove-Item data\control\batch_control.json -ErrorAction SilentlyContinue
  ```

---

## 6. Dónde Consultar y Visualizar los Resultados

1. **Auditoría e Idempotencia:**
   * **Modo Local:** Abre `data/control/batch_control.json` para ver el historial de lotes procesados con sus hashes SHA-256.
   * **Modo AWS:** Consulta la tabla `lotes` en Amazon RDS PostgreSQL para auditoría centralizada.
2. **Datasets en Parquet:**
   * Capa Silver: `data/processed/` (o `s3://<bucket>/silver/`)
   * Capa Gold: `data/curated/epidemiologia_clima/` (o `s3://<bucket>/gold/`)
3. **Métricas de Evaluación de Machine Learning:**
   * Abre `results/estadisticas/metricas_evaluacion.json` para consultar los valores exactos de RMSE, MAE y $R^2$ del modelo frente al baseline de persistencia.
4. **Modelos Persistidos:**
   * Consulta `results/modelos/rf_ira_model/` (o `s3://<bucket>/modelos/rf_ira_model/`) para ver el pipeline serializado de Spark MLlib listo para producción.
5. **Visualización en Power BI Desktop:**
   * Abre **Power BI Desktop**.
   * Conéctate directamente a Amazon RDS PostgreSQL (puerto `5432` vía VPN o Security Group autorizado) o a PostgreSQL local.
   * Consulta [`powerbi/README.md`](powerbi/README.md) para revisar las 3 páginas del dashboard (Vigilancia, Estado del Pipeline con tabla `lotes` y Predicciones ML).

---

## 7. Despliegue en la Nube (Amazon Web Services - AWS)

El código soporta de forma nativa la ejecución en Amazon Web Services sin alterar la lógica de negocio. Para desplegar en la nube según el diagrama de arquitectura:

1. **Configuración de Variables:** En tu archivo `.env`, configura:
   ```bash
   PIPELINE_ENV=aws
   AWS_REGION=us-east-1
   AWS_ACCESS_KEY_ID=tu_access_key
   AWS_SECRET_ACCESS_KEY=tu_secret_key
   S3_BUCKET=tu-bucket-s3
   EMR_CLUSTER_ID=j-XXXXXXXXXXXXX
   POSTGRES_HOST=tu-rds-endpoint.xxxxxx.us-east-1.rds.amazonaws.com
   POSTGRES_PORT=5432
   POSTGRES_DB=epidemiologia_db
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=tu_password
   ```
2. **Almacenamiento en Amazon S3:**
   El pipeline redirige automáticamente las rutas a `s3://<bucket>/bronze/`, `s3://<bucket>/silver/`, `s3://<bucket>/gold/` y `s3://<bucket>/modelos/`.
3. **Auditoría en Amazon RDS:**
   En modo `aws`, la verificación de hash SHA-256 e idempotencia de lotes se persiste en la tabla `lotes` de Amazon RDS PostgreSQL.
4. **Ejecución en Clúster Amazon EMR:**
   El pipeline se ejecuta sobre un clúster Amazon EMR sobre EC2 con Spark 3.5.3 (1 nodo primario + 2 core) gestionado por YARN:
   ```bash
   # Lanzar pipeline completo submitting steps a Amazon EMR
   python -m src.orchestration.run_pipeline --emr
   ```
5. **Consumo en Power BI:**
   Power BI Desktop se conecta a Amazon RDS PostgreSQL por el puerto 5432 (vía VPN o Security Group autorizado) mediante DirectQuery o Import para visualizar los datos consolidados y el estado de los lotes.

---

## 8. Estado del Proyecto

- [x] Selección de datasets, periodo común (2015-2024) y clave de integración (Ubigeo + Año + Semana Epidemiológica)
- [x] Esqueleto común, logging, sesión Spark 3.5.3 y configuración por entorno (`src/common/`)
- [x] Ingesta incremental Bronze con verificación de hash SHA-256 (en local y tabla `lotes` de Amazon RDS) (`src/ingestion/`)
- [x] Limpieza Silver incremental particionada por año en Parquet (`src/preprocessing/`)
- [x] Integración Gold, feature engineering con lags 1-3 semanas y modelo dimensional (`src/integration/`)
- [x] Modelado predictivo con Spark MLlib y evaluación frente al baseline de persistencia (`src/modeling/`)
- [x] Esquema DDL y Serving hacia Amazon RDS PostgreSQL (`src/serving/`)
- [x] Orquestador maestro de inicio a fin con soporte Amazon EMR (`src/orchestration/run_pipeline.py`)
- [x] Guía de conexión para dashboards en Power BI conectados a Amazon RDS (`powerbi/README.md`)
- [x] Suite completa de pruebas unitarias automatizadas (`tests/`)

---

## 9. Equipo

Proyecto del Programa de Estudio de Ingeniería de Computación y Sistemas (UPAO). Docente: Ing. Armando Javier Caballero Alvarado.

| Integrante | Frente |
|---|---|
| Silva López, Wilmer | Coordinación, AWS (Etapa 0) y orquestación |
| Caipo Trujillo, Sonia | Ingeniería de datos: Bronze y Silver |
| Principe Huamanchumo, Luis | Análisis y modelado: Gold y MLlib |
| Sandoval Vargas, Robert | Calidad, documentación, PostgreSQL y Power BI |

---
*Sistema Predictivo Big Data: Vigilancia Epidemiológica en La Libertad (Perú)*

### Auditoría y métricas en AWS

Las instrucciones para instalar la corrección sin volver a correr el pipeline,
recuperar artefactos y cambiar las fuentes de Power BI están en la
[guía de auditoría y métricas en AWS](docs/arquitectura/aws_auditoria_metricas.md).
