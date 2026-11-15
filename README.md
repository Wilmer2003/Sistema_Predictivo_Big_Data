# Sistema Predictivo Big Data

## Sistema predictivo basado en Big Data para el soporte de la vigilancia epidemiológica de infecciones respiratorias agudas y malaria en La Libertad

Proyecto académico (UPAO, Big Data y Analítica de Datos) que procesa de forma incremental datos epidemiológicos y meteorológicos con Apache Spark, entrena modelos con Spark MLlib y publica los resultados en dashboards de Power BI, como soporte a la vigilancia epidemiológica en el departamento de La Libertad, Perú.

## 1. Descripción del proyecto

El sistema integra información histórica de:

* Infecciones Respiratorias Agudas (IRA).
* Malaria.
* Variables meteorológicas (SENAMHI).

El periodo común de análisis es *2015 – 30/06/2024* y la unidad de integración es *Ubigeo + Año + Semana epidemiológica*. Cuando llega un lote nuevo de datos, este atraviesa las capas Bronze, Silver y Gold sin reprocesar lo anterior y se refleja en los dashboards.

## 2. Fuentes de datos

| Fuente | Entidad | Contenido | Periodo |
|---|---|---|---|
| IRA | MINSA | Episodios de IRA y neumonía por distrito y semana epidemiológica, grupos de edad y defunciones | 2000–2024 |
| Malaria | MINSA | Registros individuales de casos (diagnósticos B50 y B51): ubicación, año, semana, edad y sexo | 2009–2024 (La Libertad llega hasta 2022) |
| SENAMHI | SENAMHI | Temperatura (TEMP), humedad relativa (HR) y precipitación (PP) por estación, con fecha, hora, ubicación y Ubigeo | 01/01/2015 – 30/06/2024 |

Los datos no se suben al repositorio (ver `data/README.md`).

## 3. Arquitectura

Arquitectura Medallion sobre Spark. En local se usan las carpetas del repositorio y en AWS los prefijos de S3:

| Capa | Carpeta local | Prefijo en S3 | Contenido |
|---|---|---|---|
| Bronze | `data/raw/` | `bronze/` | Lotes CSV sin alterar, organizados por fecha de ingesta |
| Silver | `data/processed/` | `silver/` | Datos limpios y tipados en Parquet, sin duplicados, particionados por año |
| Gold | `data/curated/` | `gold/` | Tablas analíticas (modelo dimensional), features y predicciones |

Despliegue: laptop del equipo (orquestador `run_pipeline.py` y Power BI Desktop) y AWS (S3, clúster EMR con Spark en YARN y RDS PostgreSQL). Los diagramas están en `docs/arquitectura/`.

## 4. Estructura del repositorio

```text
Sistema_Predictivo_Big_Data/
├── README.md
├── CONTRIBUTING.md
├── requirements.txt
├── .env.example
├── .github/                 # CI, plantilla de PR e issues
├── data/                    # no se versiona (solo README y .gitkeep)
│   ├── raw/                 # Bronze: ira/ malaria/ senamhi/
│   ├── processed/           # Silver: ira/ malaria/ senamhi/
│   └── curated/             # Gold: epidemiologia_clima/
├── hadoop/                  # entorno local inicial (HDFS de un nodo)
├── src/
│   ├── common/              # configuración, sesión de Spark, semana epidemiológica
│   ├── ingestion/           # ingesta de lotes
│   ├── preprocessing/       # limpieza (Bronze a Silver)
│   ├── integration/         # integración y features (Silver a Gold)
│   ├── modeling/            # entrenamiento y predicción con MLlib
│   ├── serving/             # DDL y carga a PostgreSQL
│   ├── orchestration/       # run_pipeline.py y control de lotes
│   └── mapreduce/           # sin uso en el despliegue final
├── infra/                   # scripts de AWS (S3, EMR, RDS)
├── powerbi/                 # plantilla .pbit y guía de conexión
├── notebooks/               # exploración y gráficos
├── docs/                    # diccionario_datos, arquitectura, evidencias, informe
├── tests/                   # pruebas (fixtures con CSV pequeños)
└── results/                 # estadisticas, graficos, modelos
```

## 5. Flujo de procesamiento incremental

1. *Ingesta:* el orquestador calcula el hash del CSV y consulta la tabla de control de lotes. Si ya fue procesado, lo omite.
2. *Bronze:* el lote nuevo se guarda sin cambios por fecha de ingesta.
3. *Silver:* se limpia solo el lote pendiente, se une con lo existente, se eliminan duplicados por clave y se reescriben solo las particiones de año afectadas.
4. *Gold:* se integran las fuentes y se calculan las features de las semanas nuevas.
5. *Modelo:* Spark MLlib genera las predicciones con la versión del modelo vigente.
6. *Carga:* las tablas de Gold se copian a PostgreSQL y el lote se marca como completado.
7. *Consumo:* Power BI muestra la vigilancia y el estado del pipeline.

## 6. Tecnologías

* Python y PySpark 3.5.1 (Spark SQL y MLlib)
* AWS: S3, EMR (YARN) y RDS PostgreSQL
* Power BI Desktop
* pytest, Git y GitHub (con GitHub Actions)

## 7. Cómo empezar

```bash
git clone https://github.com/Wilmer2003/Sistema_Predictivo_Big_Data.git
cd Sistema_Predictivo_Big_Data
python -m venv .venv && source .venv/bin/activate   # en Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                 # completar los valores
pytest -q
```

Spark en local necesita Java 11 o 17. En Windows se recomienda WSL2.

## 8. Cómo trabajamos

Una rama por tarea, pull request a `main`, revisión de otro integrante y `pytest` en verde. El detalle está en `CONTRIBUTING.md`.

## 9. Estado del proyecto

- [x] Selección de datasets, periodo común y clave de integración
- [x] Limpieza de IRA, malaria y SENAMHI con PySpark
- [ ] Esqueleto común y configuración por entorno (`src/common`)
- [ ] Infraestructura en AWS (S3, EMR, RDS)
- [ ] Ingesta incremental con control de lotes
- [ ] Silver incremental
- [ ] Gold: integración y features
- [ ] Modelo con Spark MLlib y evaluación frente a la persistencia
- [ ] Carga a PostgreSQL
- [ ] Dashboards en Power BI
- [ ] Prueba de punta a punta con un lote nuevo

## 10. Equipo

Proyecto del Programa de Estudio de Ingeniería de Computación y Sistemas (UPAO). Docente: Ing. Armando Javier Caballero Alvarado.

| Integrante | Frente |
|---|---|
| Silva López, Wilmer | Coordinación, AWS (Etapa 0) y orquestación |
| Caipo Trujillo, Sonia | Ingeniería de datos: Bronze y Silver |
| Principe Huamanchumo, Luis | Análisis y modelado: Gold y MLlib |
| Sandoval Vargas, Robert | Calidad, documentación, PostgreSQL y Power BI |

---
*Sistema Predictivo Big Data: Vigilancia Epidemiológica en La Libertad*
