from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    regexp_replace,
    lpad,
    when,
    sha2,
    substring
)

# ============================================================
# 1. CREAR SESIÓN SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Limpieza_Malaria")
    .master("local[4]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.constraintPropagation.enabled", "false")
    .getOrCreate()
)

# ============================================================
# 2. RUTAS HDFS
# ============================================================

ruta_entrada = (
    "hdfs://localhost:9000/datalake/raw/malaria/"
    "dataset_Malaria.csv"
)

ruta_csv = (
    "hdfs://localhost:9000/datalake/processed/malaria/csv"
)

ruta_parquet = (
    "hdfs://localhost:9000/datalake/processed/malaria/parquet"
)

# ============================================================
# 3. LECTURA DEL DATASET
# ============================================================

print("\n========== LECTURA DEL DATASET ==========")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("encoding", "UTF-8")
    .option("sep", ",")
    .csv(ruta_entrada)
)

print("\n========== ESQUEMA ORIGINAL ==========")
df.printSchema()

print("\n========== COLUMNAS ORIGINALES ==========")
print(df.columns)

# ============================================================
# 4. NORMALIZAR NOMBRES DE COLUMNAS
# ============================================================

for nombre in df.columns:

    nuevo_nombre = (
        nombre.strip()
        .replace(" ", "_")
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
        .replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ñ", "N")
    )

    df = df.withColumnRenamed(
        nombre,
        nuevo_nombre
    )

print("\n========== COLUMNAS NORMALIZADAS ==========")
print(df.columns)

# ============================================================
# 5. LIMPIEZA DE CAMPOS DE TEXTO
# ============================================================

columnas_texto = [
    "departamento",
    "provincia",
    "distrito",
    "localidad",
    "enfermedad",
    "diagnostic",
    "diresa",
    "tipo_edad",
    "sexo"
]

for columna in columnas_texto:

    if columna in df.columns:

        df = df.withColumn(
            columna,
            trim(col(columna))
        )

# ============================================================
# 6. NORMALIZAR UBIGEO
# ============================================================

if "ubigeo" in df.columns:

    df = (
        df
        .withColumn(
            "ubigeo",
            regexp_replace(
                col("ubigeo").cast("string"),
                r"\.0$",
                ""
            )
        )
        .withColumn(
            "ubigeo",
            lpad(
                col("ubigeo"),
                6,
                "0"
            )
        )
    )

# ============================================================
# 7. NORMALIZAR LOCALCOD
# ============================================================

if "localcod" in df.columns:

    df = (
        df
        .withColumn(
            "localcod",
            regexp_replace(
                col("localcod").cast("string"),
                r"\.0$",
                ""
            )
        )
        .withColumn(
            "localcod",
            lpad(
                col("localcod"),
                10,
                "0"
            )
        )
    )

# ============================================================
# 8. CONVERSIÓN DE TIPOS
# ============================================================

if "ano" in df.columns:

    df = df.withColumn(
        "ano",
        col("ano").cast("integer")
    )

if "semana" in df.columns:

    df = df.withColumn(
        "semana",
        col("semana").cast("integer")
    )

if "edad" in df.columns:

    df = df.withColumn(
        "edad",
        col("edad").cast("integer")
    )

for columna in [
    "Malaria_total",
    "B50",
    "B51"
]:

    if columna in df.columns:

        df = df.withColumn(
            columna,
            col(columna).cast("integer")
        )

# ============================================================
# 9. ELIMINAR FILAS COMPLETAMENTE VACÍAS
# ============================================================

df = df.dropna(how="all")

# ============================================================
# 10. FILTRAR LA LIBERTAD
# ============================================================

print("\n========== FILTRANDO LA LIBERTAD ==========")

df = df.filter(
    upper(
        trim(
            col("departamento")
        )
    ) == "LA LIBERTAD"
)

print(
    "Registros después de filtrar LA LIBERTAD:",
    df.count()
)

# ============================================================
# 11. FILTRAR PERÍODO 2015 - 2024
# ============================================================

print("\n========== FILTRANDO PERÍODO ==========")

df = df.filter(
    (col("ano") >= 2015) &
    (col("ano") <= 2024)
)

# ============================================================
# 12. FILTRAR SEMANAS VÁLIDAS
# ============================================================

df = df.filter(
    (col("semana") >= 1) &
    (col("semana") <= 53)
)

# ============================================================
# 13. 2024 SOLO HASTA SEMANA 26
# ============================================================

df = df.filter(
    (col("ano") < 2024) |
    (
        (col("ano") == 2024) &
        (col("semana") <= 26)
    )
)

# ============================================================
# 13B. MATERIALIZAR EN CACHÉ
# ============================================================
# Sin esto, cada acción posterior (count/show) recalcula toda
# la cadena de filtros desde el CSV original: el optimizador de
# Spark vuelve a analizar un plan lógico cada vez más largo y el
# tiempo de planificación crece de forma no lineal. Al cachear
# aquí se corta esa cadena y las acciones siguientes reutilizan
# el resultado ya filtrado.

df = df.cache()
df.count()

# ============================================================
# 14. CONTROL DEL PERÍODO
# ============================================================

print("\n========== CONTROL DEL PERÍODO ==========")

print(
    "Total de registros:",
    df.count()
)

print(
    "Año mínimo:",
    df.selectExpr("min(ano)").first()[0]
)

print(
    "Año máximo:",
    df.selectExpr("max(ano)").first()[0]
)

print("\nDistribución por año:")

(
    df
    .groupBy("ano")
    .count()
    .orderBy("ano")
    .show(20)
)

print("\nProvincias encontradas:")

(
    df
    .select("provincia")
    .distinct()
    .orderBy("provincia")
    .show(100, truncate=False)
)

print("\nCantidad de provincias:")

print(
    df.select("provincia")
    .distinct()
    .count()
)

print("\nCantidad de distritos:")

print(
    df.select("distrito")
    .distinct()
    .count()
)

print("\nCantidad de localidades:")

print(
    df.select("localidad")
    .distinct()
    .count()
)

# ============================================================
# 15. CONTROL ESPECÍFICO DE 2024
# ============================================================

print("\n========== CONTROL 2024 ==========")

(
    df
    .filter(col("ano") == 2024)
    .selectExpr(
        "min(semana) as semana_minima",
        "max(semana) as semana_maxima"
    )
    .show()
)

# ============================================================
# 16. DUPLICADOS
# ============================================================

print("\n========== ELIMINANDO DUPLICADOS ==========")

registros_antes = df.count()

df = df.dropDuplicates()

registros_despues = df.count()

print(
    "Registros antes:",
    registros_antes
)

print(
    "Registros después:",
    registros_despues
)

print(
    "Duplicados eliminados:",
    registros_antes - registros_despues
)

# ============================================================
# 16B. PROTECCIÓN DE DATOS SENSIBLES (EDAD Y LOCALIDAD)
# ============================================================
# "edad" y "localidad" son cuasi-identificadores: combinados con
# diagnostico/semana epidemiologica permiten reidentificar a una
# persona en localidades pequeñas con pocos casos. Se aplican dos
# tecnicas sobre columnas derivadas (no se sobrescribe el dato
# original en "df", solo se excluye de "df_final" mas abajo):
#
#   - Generalizacion: "edad" exacta -> "edad_rango" (rango de 10-15
#     años). Reduce la granularidad del cuasi-identificador.
#   - Seudonimizacion: "localidad" -> "localidad_hash" (SHA-256
#     truncado a 12 caracteres). No reversible, pero es
#     deterministico: una misma localidad siempre produce el mismo
#     hash, lo que permite seguir agrupando/contando por localidad
#     sin exponer su nombre real en el dataset curado.
#
# Riesgo residual: si una localidad tiene muy pocos casos en una
# semana, la combinacion rango_edad + sexo + semana + diagnostico
# puede seguir siendo unica dentro de ese hash (bajo k-anonimato).
# El hash protege el nombre de la localidad, no la unicidad del
# registro.

if "edad" in df.columns:

    df = df.withColumn(
        "edad_rango",
        when(col("edad") < 5, "0-4")
        .when(col("edad") < 15, "5-14")
        .when(col("edad") < 45, "15-44")
        .when(col("edad") < 65, "45-64")
        .when(col("edad").isNotNull(), "65+")
        .otherwise("SIN_DATO")
    )

if "localidad" in df.columns:

    df = df.withColumn(
        "localidad_hash",
        substring(
            sha2(upper(trim(col("localidad"))), 256),
            1,
            12
        )
    )

print("\n========== EVIDENCIA: ANTES Y DESPUES DE LA PROTECCION ==========")

(
    df
    .select(
        "localidad",
        "localidad_hash",
        "edad",
        "edad_rango"
    )
    .show(10, truncate=False)
)

# ============================================================
# 17. COLUMNAS FINALES
# ============================================================

columnas_finales = [
    "departamento",
    "provincia",
    "distrito",
    "localidad_hash",
    "enfermedad",
    "ano",
    "semana",
    "diagnostic",
    "diresa",
    "ubigeo",
    "localcod",
    "edad_rango",
    "tipo_edad",
    "sexo",
    "Malaria_total",
    "B50",
    "B51"
]

columnas_existentes = [
    c for c in columnas_finales
    if c in df.columns
]

df_final = df.select(
    *columnas_existentes
)

# ============================================================
# 18. CONTROL FINAL
# ============================================================

print("\n========== ESQUEMA FINAL ==========")

df_final.printSchema()

print("\n========== DATOS FINALES ==========")

df_final.show(
    10,
    truncate=False
)

print("\n========== TOTAL FINAL ==========")

print(
    df_final.count()
)

# ============================================================
# 19. GUARDAR CSV
# ============================================================

print("\n========== GUARDANDO CSV ==========")

(
    df_final
    .write
    .mode("overwrite")
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("sep", ",")
    .csv(ruta_csv)
)

print("CSV guardado correctamente.")

# ============================================================
# 20. GUARDAR PARQUET
# ============================================================

print("\n========== GUARDANDO PARQUET ==========")

(
    df_final
    .write
    .mode("overwrite")
    .partitionBy("ano")
    .parquet(ruta_parquet)
)

print("Parquet guardado correctamente.")

# ============================================================
# 21. FINAL
# ============================================================

print("\n==========================================")
print("      LIMPIEZA MALARIA FINALIZADA")
print("==========================================")

print("CSV:")
print(ruta_csv)

print("\nParquet:")
print(ruta_parquet)

print("\nRegistros finales:")
print(df_final.count())

spark.stop()
