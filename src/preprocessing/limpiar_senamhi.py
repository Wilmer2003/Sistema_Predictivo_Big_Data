from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    regexp_extract,
    regexp_replace,
    to_date,
    weekofyear,
    concat_ws,
    lpad
)

# ============================================================
# 1. CREAR SESIÓN SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Limpieza_SENAMHI")
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
    "hdfs://localhost:9000/datalake/raw/senamhi/"
    "dataset_SENAMHI.csv"
)

ruta_csv = (
    "hdfs://localhost:9000/datalake/processed/senamhi/csv"
)

ruta_parquet = (
    "hdfs://localhost:9000/datalake/processed/senamhi/parquet"
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
# 5. LIMPIEZA DEL CAMPO LUGAR
# ============================================================
# El campo "Lugar" llega como "ESTACION (DEPARTAMENTO)".
# Se separa en Estacion y Departamento para poder integrarse
# más adelante con IRA y Malaria por Departamento.

df = df.withColumn(
    "Lugar",
    trim(col("Lugar"))
)

df = df.withColumn(
    "Departamento",
    trim(
        regexp_extract(
            col("Lugar"),
            r"\(([^()]+)\)\s*$",
            1
        )
    )
)

df = df.withColumn(
    "Estacion",
    trim(
        regexp_replace(
            col("Lugar"),
            r"\s*\([^()]+\)\s*$",
            ""
        )
    )
)

# ============================================================
# 6. CONVERSIÓN DE TIPOS
# ============================================================

for columna in ["Ano", "Mes", "Dia"]:

    if columna in df.columns:

        df = df.withColumn(
            columna,
            col(columna).cast("integer")
        )

for columna in [
    "Precipitacion_acumulada",
    "Temperatura_maxima",
    "Temperatura_minima",
    "tc"
]:

    if columna in df.columns:

        df = df.withColumn(
            columna,
            col(columna).cast("double")
        )

# ============================================================
# 7. CONSTRUIR FECHA Y SEMANA EPIDEMIOLÓGICA
# ============================================================

df = df.withColumn(
    "Fecha",
    to_date(
        concat_ws(
            "-",
            col("Ano"),
            lpad(col("Mes").cast("string"), 2, "0"),
            lpad(col("Dia").cast("string"), 2, "0")
        ),
        "yyyy-MM-dd"
    )
)

df = df.withColumn(
    "Semana_Epidemiologica",
    weekofyear(col("Fecha"))
)

# ============================================================
# 8. ELIMINAR FILAS COMPLETAMENTE VACÍAS O SIN FECHA VÁLIDA
# ============================================================

df = df.dropna(how="all")

df = df.filter(
    col("Fecha").isNotNull()
)

# ============================================================
# 9. FILTRAR LA LIBERTAD
# ============================================================

print("\n========== FILTRANDO LA LIBERTAD ==========")

df = df.filter(
    upper(
        trim(
            col("Departamento")
        )
    ) == "LA LIBERTAD"
)

print(
    "Registros después de filtrar LA LIBERTAD:",
    df.count()
)

# ============================================================
# 10. FILTRAR PERÍODO 2015 - 2024
# ============================================================

print("\n========== FILTRANDO PERÍODO ==========")

df = df.filter(
    (col("Ano") >= 2015) &
    (col("Ano") <= 2024)
)

# ============================================================
# 10B. MATERIALIZAR EN CACHÉ
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
# 11. CONTROL DE RANGOS FÍSICOS DE LAS VARIABLES
# ============================================================
# Reglas de calidad: la temperatura mínima no puede superar a la
# máxima y la precipitación no puede ser negativa.

print("\n========== CONTROL DE RANGOS ==========")

registros_antes_rango = df.count()

df = df.filter(
    (col("Precipitacion_acumulada") >= 0) &
    (col("Temperatura_minima") <= col("Temperatura_maxima"))
)

registros_despues_rango = df.count()

print("Registros antes del control de rango:", registros_antes_rango)
print("Registros después del control de rango:", registros_despues_rango)
print(
    "Registros descartados por rango inválido:",
    registros_antes_rango - registros_despues_rango
)

# ============================================================
# 12. CONTROL DEL PERÍODO
# ============================================================

print("\n========== CONTROL DEL PERÍODO ==========")

print(
    "Total de registros:",
    df.count()
)

print(
    "Año mínimo:",
    df.selectExpr("min(Ano)").first()[0]
)

print(
    "Año máximo:",
    df.selectExpr("max(Ano)").first()[0]
)

print("\nDistribución por año:")

(
    df
    .groupBy("Ano")
    .count()
    .orderBy("Ano")
    .show(20)
)

print("\nEstaciones encontradas:")

(
    df
    .select("Estacion")
    .distinct()
    .orderBy("Estacion")
    .show(20, truncate=False)
)

print("\nCantidad de estaciones:")

print(
    df.select("Estacion")
    .distinct()
    .count()
)

# ============================================================
# 13. ELIMINAR DUPLICADOS
# ============================================================

print("\n========== ELIMINANDO DUPLICADOS ==========")

registros_antes = df.count()

df = df.dropDuplicates(["Estacion", "Fecha"])

registros_despues = df.count()

print("Registros antes:", registros_antes)
print("Registros después:", registros_despues)
print(
    "Duplicados eliminados:",
    registros_antes - registros_despues
)

# ============================================================
# 14. COLUMNAS FINALES
# ============================================================

columnas_finales = [
    "Departamento",
    "Estacion",
    "Ano",
    "Mes",
    "Dia",
    "Fecha",
    "Semana_Epidemiologica",
    "Precipitacion_acumulada",
    "Temperatura_maxima",
    "Temperatura_minima",
    "tc"
]

columnas_existentes = [
    c for c in columnas_finales
    if c in df.columns
]

df_final = df.select(
    *columnas_existentes
)

# ============================================================
# 15. CONTROL FINAL
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
# 16. GUARDAR CSV
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
# 17. GUARDAR PARQUET
# ============================================================

print("\n========== GUARDANDO PARQUET ==========")

(
    df_final
    .write
    .mode("overwrite")
    .partitionBy("Ano")
    .parquet(ruta_parquet)
)

print("Parquet guardado correctamente.")

# ============================================================
# 18. FINAL
# ============================================================

print("\n==========================================")
print("      LIMPIEZA SENAMHI FINALIZADA")
print("==========================================")

print("CSV:")
print(ruta_csv)

print("\nParquet:")
print(ruta_parquet)

print("\nRegistros finales:")
print(df_final.count())

spark.stop()
