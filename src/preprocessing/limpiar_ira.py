
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    regexp_replace,
    lpad
)

# ============================================================
# 1. CREAR SESIÓN SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Limpieza_IRA")
    .getOrCreate()
)

# ============================================================
# 2. RUTAS HDFS
# ============================================================

ruta_entrada = (
    "hdfs://localhost:9000/datalake/raw/ira/"
    "dataset_IRA.csv"
)

ruta_csv = (
    "hdfs://localhost:9000/datalake/processed/ira/csv"
)

ruta_parquet = (
    "hdfs://localhost:9000/datalake/processed/ira/parquet"
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
    .option("sep", ";")
    .csv(ruta_entrada)
)

print("\n========== ESQUEMA ORIGINAL ==========")
df.printSchema()

print("\n========== COLUMNAS ORIGINALES ==========")
print(df.columns)

# ============================================================
# 4. LIMPIEZA DE NOMBRES DE COLUMNAS
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

# ============================================================
# 5. VERIFICAR COLUMNAS DESPUÉS DE NORMALIZAR
# ============================================================

print("\n========== COLUMNAS NORMALIZADAS ==========")
print(df.columns)

# ============================================================
# 6. LIMPIEZA DE CAMPOS DE TEXTO
# ============================================================

for columna in [
    "Departamento",
    "Provincia",
    "Distrito"
]:

    if columna in df.columns:

        df = df.withColumn(
            columna,
            trim(col(columna))
        )

# ============================================================
# 7. NORMALIZAR UBIGEO
# ============================================================

if "Codigo_Ubigeo" in df.columns:

    df = (
        df
        .withColumn(
            "Codigo_Ubigeo",
            regexp_replace(
                col("Codigo_Ubigeo").cast("string"),
                r"\.0$",
                ""
            )
        )
        .withColumn(
            "Codigo_Ubigeo",
            lpad(
                col("Codigo_Ubigeo"),
                6,
                "0"
            )
        )
    )

# ============================================================
# 8. CONVERSIÓN DE TIPOS
# ============================================================

if "Ano" in df.columns:

    df = df.withColumn(
        "Ano",
        col("Ano").cast("integer")
    )

if "Semana_Epidemiologica" in df.columns:

    df = df.withColumn(
        "Semana_Epidemiologica",
        col("Semana_Epidemiologica").cast("integer")
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
            col("Departamento")
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
    (col("Ano") >= 2015) &
    (col("Ano") <= 2024)
)

# ============================================================
# 12. FILTRAR SEMANAS VÁLIDAS
# ============================================================

df = df.filter(
    (col("Semana_Epidemiologica") >= 1) &
    (col("Semana_Epidemiologica") <= 53)
)

# ============================================================
# 13. 2024 SOLO HASTA SEMANA 26
# ============================================================

df = df.filter(
    (col("Ano") < 2024) |
    (
        (col("Ano") == 2024) &
        (col("Semana_Epidemiologica") <= 26)
    )
)

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

print("\nDepartamentos encontrados:")

(
    df
    .select("Departamento")
    .distinct()
    .show()
)

# ============================================================
# 15. CONTROL ESPECÍFICO DE 2024
# ============================================================

print("\n========== CONTROL 2024 ==========")

(
    df
    .filter(col("Ano") == 2024)
    .selectExpr(
        "min(Semana_Epidemiologica) as semana_minima",
        "max(Semana_Epidemiologica) as semana_maxima"
    )
    .show()
)

# ============================================================
# 16. ELIMINAR DUPLICADOS
# ============================================================

registros_antes = df.count()

df = df.dropDuplicates()

registros_despues = df.count()

print("\n========== DUPLICADOS ==========")

print("Antes:", registros_antes)
print("Después:", registros_despues)

print(
    "Eliminados:",
    registros_antes - registros_despues
)

# ============================================================
# 17. RECONSTRUIR DATAFRAME FINAL
# ============================================================

columnas_finales = [
    "Departamento",
    "Provincia",
    "Distrito",
    "Ano",
    "Semana_Epidemiologica",
    "Codigo_Departamento",
    "Codigo_Ubigeo",
    "IRA_menores_2_meses",
    "IRA_2_a_11_meses",
    "IRA_1_a_4_anos",
    "Neumonia_menores_2_meses",
    "Neumonia_2_a_11_meses",
    "Neumonia_1_a_4_anos",
    "Defunciones"
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
    .option("sep", ";")
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
    .partitionBy("Ano")
    .parquet(ruta_parquet)
)

print("Parquet guardado correctamente.")

# ============================================================
# 21. FINAL
# ============================================================

print("\n==========================================")
print("       LIMPIEZA IRA FINALIZADA")
print("==========================================")

print("CSV:")
print(ruta_csv)

print("\nParquet:")
print(ruta_parquet)

print("\nRegistros finales:")
print(df_final.count())

spark.stop()

