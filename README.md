# Sistema Predictivo Big Data

## Sistema predictivo basado en Big Data para el soporte de la vigilancia epidemiológica de infecciones respiratorias agudas y malaria en La Libertad

Proyecto académico orientado al procesamiento, integración y análisis de grandes volúmenes de datos epidemiológicos y meteorológicos mediante tecnologías Big Data, con el propósito de generar información que sirva como soporte para la vigilancia epidemiológica en el departamento de La Libertad, Perú.

## 1. Descripción del proyecto

El proyecto desarrolla un sistema predictivo basado en Big Data que integra información histórica de:

* Infecciones Respiratorias Agudas (IRA).
* Malaria.
* Variables meteorológicas.

La integración de los datos permitirá analizar el comportamiento epidemiológico en relación con variables temporales, geográficas y meteorológicas.

El procesamiento considera información correspondiente al periodo común *2015 – 30/06/2024*, utilizando como unidad de integración:

*Ubigeo + Año + Semana epidemiológica*

## 2. Fuentes de datos

### Infecciones Respiratorias Agudas (IRA)

Dataset epidemiológico que contiene información de casos de infecciones respiratorias agudas y neumonía, organizada por departamento, provincia, distrito, año y semana epidemiológica.

Variables principales:

* Departamento
* Provincia
* Distrito
* Año
* Semana epidemiológica
* Código Ubigeo
* Casos de IRA por grupos de edad
* Casos de neumonía
* Defunciones

### Malaria

Dataset epidemiológico que contiene registros de casos de malaria, incluyendo información geográfica, temporal, diagnóstica, demográfica y de sexo.

Variables principales:

* Departamento
* Provincia
* Distrito
* Localidad
* Año
* Semana epidemiológica
* Diagnóstico
* Ubigeo
* Edad
* Sexo

Los diagnósticos considerados incluyen principalmente:

* B50
* B51

### SENAMHI

Dataset de variables meteorológicas procedente del *Servicio Nacional de Meteorología e Hidrología del Perú (SENAMHI)*.

El conjunto contiene registros de estaciones meteorológicas automáticas correspondientes al periodo:

*01/01/2015 – 30/06/2024*

Variables meteorológicas principales:

* Temperatura (TEMP)
* Humedad relativa (HR)
* Precipitación (PP)
* Fecha
* Hora
* Ubicación geográfica
* Ubigeo
* Estación meteorológica

## 3. Arquitectura de datos

El proyecto utiliza una estructura de procesamiento organizada por zonas:

text
Datos originales
      │
      ▼
┌─────────────────┐
│      RAW        │
│ Datos originales│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PROCESSED     │
│ Limpieza y      │
│ transformación  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    CURATED      │
│ Datos integrados│
│ para análisis   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PREDICCIÓN    │
│ Modelos y       │
│ resultados      │
└─────────────────┘


## 4. Estructura del repositorio

text
Sistema_Predictivo_Big_Data/
│
├── README.md
├── .gitignore
│
├── data/
│   ├── raw/
│   │   ├── ira/
│   │   ├── malaria/
│   │   └── senamhi/
│   │
│   ├── processed/
│   │   ├── ira/
│   │   ├── malaria/
│   │   └── senamhi/
│   │
│   └── curated/
│       └── epidemiologia_clima/
│
├── hadoop/
│   ├── config/
│   │   ├── core-site.xml
│   │   ├── hdfs-site.xml
│   │   ├── mapred-site.xml
│   │   └── yarn-site.xml
│   │
│   └── scripts/
│       ├── crear_hdfs.sh
│       ├── cargar_datos.sh
│       └── ejecutar_pipeline.sh
│
├── src/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── integration/
│   └── mapreduce/
│
├── notebooks/
│   ├── 01_exploracion_ira.ipynb
│   ├── 02_exploracion_malaria.ipynb
│   ├── 03_exploracion_senamhi.ipynb
│   └── 04_datos_integrados.ipynb
│
├── docs/
│   ├── diccionario_datos/
│   ├── arquitectura/
│   └── informe/
│
├── tests/
│
└── results/
    ├── estadisticas/
    ├── graficos/
    └── modelos/


## 5. Flujo de procesamiento

El procesamiento general seguirá las siguientes etapas:

1. *Ingesta:* incorporación de los datasets epidemiológicos y meteorológicos.
2. *Almacenamiento:* organización de los datos originales en la zona raw.
3. *Preprocesamiento:* limpieza, normalización y transformación de los datos.
4. *Integración:* unión de los datasets mediante Ubigeo + Año + Semana epidemiológica.
5. *Procesamiento Big Data:* utilización de Hadoop/HDFS y procesamiento distribuido.
6. *Generación del dataset analítico:* construcción del conjunto de datos integrado.
7. *Modelamiento predictivo:* entrenamiento y evaluación de modelos.
8. *Resultados:* generación de estadísticas, gráficos y modelos.

## 6. Tecnologías

* Python
* Pandas
* Hadoop
* HDFS
* MapReduce
* Jupyter Notebook
* Git
* GitHub

## 7. Alcance geográfico

El proyecto se enfoca en el departamento de *La Libertad, Perú*, considerando la información disponible para sus provincias y distritos.

## 8. Gestión de datos

Los archivos originales de gran tamaño no se almacenarán directamente en el repositorio Git. Se utilizarán las carpetas locales de datos y el almacenamiento distribuido mediante HDFS.

El repositorio contiene principalmente:

* Código fuente.
* Scripts de procesamiento.
* Configuraciones.
* Documentación.
* Notebooks.
* Pruebas.
* Resultados seleccionados.

## 9. Estado del proyecto

El proyecto se encuentra en etapa de desarrollo.

### Avances

* [x] Selección de datasets epidemiológicos.
* [x] Selección del dataset meteorológico SENAMHI.
* [x] Definición del periodo común de análisis.
* [x] Definición de la clave de integración.
* [ ] Organización de datasets en el repositorio.
* [ ] Preprocesamiento de datos.
* [ ] Integración de datasets.
* [ ] Implementación del procesamiento Big Data.
* [ ] Construcción del dataset analítico.
* [ ] Desarrollo del modelo predictivo.
* [ ] Evaluación del modelo.
* [ ] Generación de resultados.

## 10. Equipo

Proyecto académico desarrollado para el Programa de Estudio de Ingeniería de Computación y Sistemas.

---

*Sistema Predictivo Big Data — Vigilancia Epidemiológica en La Libertad*