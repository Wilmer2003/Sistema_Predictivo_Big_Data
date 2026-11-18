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

Arquitectura de datos sobre Apache Spark (PySpark 3.5.3), compatible tanto en almacenamiento local como en Google Cloud Platform (Cloud Storage vía conector GCS `gs://` y clúster Dataproc 2.2 en YARN):

| Capa | Carpeta local | Prefijo en Cloud Storage (GCS) | Contenido y Transformaciones |
|---|---|---|---|
| **Bronze** | `data/raw/` | `gs://<bucket>/bronze/` | Lotes CSV crudos sin alterar, particionados por `fecha_ingesta=YYYY-MM-DD` con verificación de hash criptográfico SHA-256 (en tabla `lotes` de Cloud SQL o archivo local). |
| **Silver** | `data/processed/` | `gs://<bucket>/silver/` | Datos limpios, tipados estrictos en PySpark, filtrados para La Libertad (13), deduplicados por clave de negocio y guardados en **Parquet particionado por Año** (escritura dinámica de particiones). |
| **Gold** | `data/curated/` | `gs://<bucket>/gold/` | Esquema dimensional (`dim_ubigeo`, `dim_tiempo`, `fact_vigilancia_clima`) y dataset analítico con **variables rezagadas (lags de 1 a 3 semanas)**, deltas y medias móviles. |
| **Modelado** | `results/modelos/` | `gs://<bucket>/modelos/` | Pipeline de PySpark MLlib (Random Forest / GBT / Regresión Lineal) evaluado frente a un **baseline de persistencia** ($y_t = y_{t-1}$) con métricas RMSE, MAE y $R^2$. |
| **Serving** | `src/serving/` | `Cloud SQL (PostgreSQL)` | Script DDL relacional y cargador JDBC hacia Cloud SQL para PostgreSQL para tableros analíticos y tabla de auditoría `lotes`. |

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
│   ├── common/              # Configuración local/GCP (.env), sesión Spark 3.5.3, geo_utils, epi_utils y logger
│   ├── ingestion/           # BatchControlManager (JSON local o tabla lotes Cloud SQL), SHA-256 e ingesta cruda a Bronze
│   ├── preprocessing/       # Limpieza en PySpark, filtro La Libertad (13), bandeo de celda pequeña y Parquet Silver
│   ├── integration/         # Agregación semanal, lags temporales 1-3 semanas y modelo dimensional Gold
│   ├── modeling/            # Pipeline MLlib, evaluación frente a persistencia, entrenamiento e inferencia
│   ├── serving/             # schema.sql DDL (tablas Gold + lotes) y cargador JDBC a Cloud SQL PostgreSQL
│   ├── orchestration/       # run_pipeline.py (orquestador maestro local y Dataproc)
│   └── mapreduce/           # Scripts de referencia académica inicial
├── powerbi/                 # Guía de conexión e integración de tableros (.pbit) con Cloud SQL PostgreSQL
├── notebooks/               # Exploración interactiva de datos y gráficos
├── docs/                    # Diccionario de datos, arquitectura y evidencias
├── tests/                   # Suite de pruebas unitarias ejecutables con pytest o unittest
└── results/                 # Modelos serializados (.parquet/.ml) y métricas de evaluación (.json)
```

---

## 5. Guía Paso a Paso para Ejecutar en Visual Studio Code (VS Code)

### Requisitos Previos en tu Computadora
1. **Visual Studio Code**: Descargado e instalado en tu sistema.
2. **Python**: Versión 3.9, 3.10 o 3.11 instalada (asegúrate de marcar *"Add Python to PATH"* durante la instalación).
3. **Java JDK 11 o 17**: Requerido por el motor de Apache Spark. Verifica abriendo una terminal y ejecutando `java -version`.
4. **Extensiones recomendadas en VS Code**:
   * *Python* (de Microsoft).
   * *Pylance*.
   * *Power BI Desktop* (opcional para abrir los tableros de visualización).
   * *PostgreSQL* o *DBeaver* (opcional para inspeccionar la base de datos de Serving).

---

### Paso 1: Abrir el Proyecto en VS Code
1. Abre **Visual Studio Code**.
2. Ve al menú superior: `File` -> `Open Folder...` (o `Archivo` -> `Abrir carpeta...`).
3. Selecciona la carpeta raíz del repositorio:
   ```text
   Sistema_Predictivo_Big_Data
   ```

---

### Paso 2: Abrir la Terminal Integrada de VS Code
Presiona el atajo `Ctrl + ñ` (o `Ctrl + ~`), o ve al menú: `Terminal` -> `New Terminal`.

---

### Paso 3: Crear y Activar el Entorno Virtual de Python
En la terminal integrada de VS Code:

* **En Windows (PowerShell):**
  ```powershell
  python -m venv .venv
  .venv\Scripts\Activate.ps1
  ```
  *(Si PowerShell bloquea la activación de scripts, ejecuta una sola vez `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`)*.

* **En Linux / macOS / WSL2:**
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  ```

*En VS Code, presiona `Ctrl + Shift + P`, escribe `Python: Select Interpreter` y selecciona el interprete de tu entorno virtual recién creado (`.venv`).*

---

### Paso 4: Instalar las Dependencias
Con el entorno virtual activado (`(.venv)` al inicio de la línea de comandos):
```bash
pip install -r requirements.txt
```

---

### Paso 5: Configurar las Variables de Entorno (`.env`)
Crea tu archivo local a partir de la plantilla:

* **En Windows (PowerShell / CMD):**
  ```powershell
  copy .env.example .env
  ```
* **En Linux / macOS / Git Bash:**
  ```bash
  cp .env.example .env
  ```

*Para ejecución 100% local en tu laptop no necesitas modificar nada; los valores predeterminados apuntan al almacenamiento local de tu disco.*

---

### Paso 6: Ejecutar las Pruebas Unitarias
Verifica la integridad de todos los módulos del sistema ejecutando:
```bash
pytest -v
```
*O con el runner de la biblioteca estándar de Python:*
```bash
python -m unittest discover -s tests -p "test_*.py" -v
```
*(Deberás observar las 31 pruebas pasando exitosamente en verde).*

---

### Paso 7: Ejecutar el Pipeline Completo End-to-End
Ejecuta el orquestador principal:
```bash
python -m src.orchestration.run_pipeline --all
```

**¿Qué ocurre en tiempo real durante la ejecución?**
1. **Bronze (`src/ingestion/`):** Valida la integridad criptográfica de los datos entrantes mediante **SHA-256**. Si el archivo ya fue procesado, omite la recarga para garantizar idempotencia.
2. **Silver (`src/preprocessing/`):** Inicializa la sesión de Spark 3.5.1, filtra registros de La Libertad (`13`), limpia rangos físicos de SENAMHI, aplica bandeo de celdas pequeñas para privacidad de menores de edad y guarda en Parquet particionado dinámicamente por año.
3. **Gold (`src/integration/`):** Construye la agregación semanal, genera los retardos climáticos y epidemiológicos (**lags de 1 a 3 semanas**, deltas y medias móviles de 3 semanas) y ensambla las tablas dimensionales en estrella (`dim_ubigeo`, `dim_tiempo`, `fact_vigilancia_clima`).
4. **Machine Learning (`src/modeling/`):** Ejecuta división temporal cronológica (2015–2022 entrenamiento / 2023–2024 prueba), entrena un ensamble con Spark MLlib, calcula el **baseline de persistencia** y compara la reducción de error (RMSE y MAE).
5. **Serving (`src/serving/`):** Crea el esquema relacional en PostgreSQL y carga las tablas analíticas para BI (si PostgreSQL está apagado, emite una advertencia limpia y mantiene los datos seguros en Parquet).

---

### Paso 8: Comandos Modulares Adicionales

Si deseas ejecutar o depurar una fase específica de forma aislada:

```bash
# Solo Ingesta Bronze
python -m src.orchestration.run_pipeline --ingest

# Solo Limpieza Silver
python -m src.orchestration.run_pipeline --silver

# Solo Integración y Feature Engineering Gold
python -m src.orchestration.run_pipeline --gold

# Solo Entrenamiento e Inferencia de Machine Learning
python -m src.orchestration.run_pipeline --model

# Solo Carga a PostgreSQL (Serving)
python -m src.orchestration.run_pipeline --serving

# Forzar reingesta omitiendo la verificación de duplicados por hash
python -m src.orchestration.run_pipeline --force
```

---

## 6. Dónde Consultar y Visualizar los Resultados

1. **Auditoría e Idempotencia:**
   * **Modo Local:** Abre `data/control/batch_control.json` para ver el historial de lotes procesados con sus hashes SHA-256.
   * **Modo GCP:** Consulta la tabla `lotes` en Cloud SQL para PostgreSQL para auditoría centralizada.
2. **Datasets en Parquet:**
   * Capa Silver: `data/processed/` (o `gs://<bucket>/silver/`)
   * Capa Gold: `data/curated/epidemiologia_clima/` (o `gs://<bucket>/gold/`)
3. **Métricas de Evaluación de Machine Learning:**
   * Abre `results/estadisticas/metricas_evaluacion.json` para consultar los valores exactos de RMSE, MAE y $R^2$ del modelo frente al baseline de persistencia.
4. **Modelos Persistidos:**
   * Consulta `results/modelos/rf_ira_model/` (o `gs://<bucket>/modelos/rf_ira_model/`) para ver el pipeline serializado de Spark MLlib listo para producción.
5. **Visualización en Power BI Desktop:**
   * Abre **Power BI Desktop**.
   * Conéctate directamente a Cloud SQL para PostgreSQL (puerto `5432` con redes autorizadas configuradas) o a PostgreSQL local.
   * Consulta [`powerbi/README.md`](powerbi/README.md) para revisar las 3 páginas del dashboard (Vigilancia, Estado del Pipeline con tabla `lotes` y Predicciones ML).

---

## 7. Despliegue en la Nube (Google Cloud Platform - GCP)

El código soporta de forma nativa la ejecución en Google Cloud Platform sin alterar la lógica de negocio. Para desplegar en la nube según el diagrama de arquitectura:

1. **Configuración de Variables:** En tu archivo `.env`, configura:
   ```bash
   PIPELINE_ENV=gcp
   GCP_PROJECT_ID=tu-proyecto-gcp
   GCP_REGION=us-central1
   GCS_BUCKET=tu-bucket-gcs
   DATAPROC_CLUSTER_NAME=cluster-epidemiologia
   GOOGLE_APPLICATION_CREDENTIALS=/ruta/a/service-account-key.json
   POSTGRES_HOST=10.x.x.x  # IP pública o privada de Cloud SQL
   POSTGRES_PORT=5432
   POSTGRES_DB=epidemiologia_db
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=tu_password
   ```
2. **Almacenamiento en Cloud Storage:**
   El pipeline redirige automáticamente las rutas a `gs://<bucket>/bronze/`, `gs://<bucket>/silver/`, `gs://<bucket>/gold/` y `gs://<bucket>/modelos/`.
3. **Auditoría en Cloud SQL:**
   En modo `gcp`, la verificación de hash SHA-256 e idempotencia de lotes se persiste en la tabla `lotes` de Cloud SQL para PostgreSQL.
4. **Ejecución en Clúster Dataproc:**
   El pipeline se ejecuta sobre un clúster Dataproc con Spark 3.5.3 (imagen 2.2) gestionado por YARN:
   ```bash
   # Lanzar pipeline completo submitting PySpark jobs a Dataproc
   python -m src.orchestration.run_pipeline --dataproc
   ```
5. **Consumo en Power BI:**
   Power BI Desktop se conecta a Cloud SQL para PostgreSQL por el puerto 5432 mediante DirectQuery o Import para visualizar los datos consolidados y el estado de los lotes.

---

## 8. Estado del Proyecto

- [x] Selección de datasets, periodo común (2015-2024) y clave de integración (Ubigeo + Año + Semana Epidemiológica)
- [x] Esqueleto común, logging, sesión Spark 3.5.3 y configuración por entorno (`src/common/`)
- [x] Ingesta incremental Bronze con verificación de hash SHA-256 (en local y tabla `lotes` de Cloud SQL) (`src/ingestion/`)
- [x] Limpieza Silver incremental particionada por año en Parquet (`src/preprocessing/`)
- [x] Integración Gold, feature engineering con lags 1-3 semanas y modelo dimensional (`src/integration/`)
- [x] Modelado predictivo con Spark MLlib y evaluación frente al baseline de persistencia (`src/modeling/`)
- [x] Esquema DDL y Serving hacia Cloud SQL para PostgreSQL (`src/serving/`)
- [x] Orquestador maestro de inicio a fin con soporte Dataproc (`src/orchestration/run_pipeline.py`)
- [x] Guía de conexión para dashboards en Power BI conectados a Cloud SQL (`powerbi/README.md`)
- [x] Suite completa de pruebas unitarias automatizadas (`tests/`)

---

## 9. Equipo

Proyecto del Programa de Estudio de Ingeniería de Computación y Sistemas (UPAO). Docente: Ing. Armando Javier Caballero Alvarado.

| Integrante | Frente |
|---|---|
| Silva López, Wilmer | Coordinación, Google Cloud (Etapa 0) y orquestación |
| Caipo Trujillo, Sonia | Ingeniería de datos: Bronze y Silver |
| Principe Huamanchumo, Luis | Análisis y modelado: Gold y MLlib |
| Sandoval Vargas, Robert | Calidad, documentación, PostgreSQL y Power BI |

---
*Sistema Predictivo Big Data: Vigilancia Epidemiológica en La Libertad (Perú)*
