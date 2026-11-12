
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    regexp_replace,
    lpad,
    when
)

# ============================================================
# 1. CREAR SESIÓN SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Limpieza_IRA")
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
# 16B. PROTECCIÓN DE DATOS SENSIBLES (CELDAS PEQUEÑAS)
# ============================================================
# IRA viene agregado por distrito y semana epidemiologica (no es
# un registro por persona), pero sigue siendo un dato de salud
# sensible por categoria y, ademas, incluye casos en menores de
# edad (grupos "_menores_2_meses" y "_1_a_4_anos"), sujetos a
# proteccion reforzada. Al revisar el CSV original se detecto
# riesgo de "small cell disclosure": muchas celdas distrito-semana
# tienen conteos de 1 a 3 casos (ej. ~8% de las filas tienen
# exactamente 1 caso en "IRA_menores_2_meses"), lo que en un
# distrito pequeno puede permitir inferir el caso individual.
#
# Tecnica aplicada: bandeo/generalizacion de celda pequeña. Se
# agrega, junto a cada conteo exacto, una version "_rango" con el
# valor agrupado en bandas (0, 1-4, 5-9, 10-19, 20+). El conteo
# exacto se conserva porque es la variable objetivo del modelo
# predictivo (no se puede eliminar sin romper el proposito del
# proyecto); la version en rango es la que debe usarse en tablas,
# graficos o reportes que se compartan fuera del equipo del
# proyecto.
#
# Riesgo residual: el conteo exacto sigue existiendo en el dataset
# curado para fines de modelamiento. Si ese archivo se comparte
# sin control de acceso, el riesgo de celda pequeña persiste; la
# mitigacion solo es efectiva en la capa de reportes/salidas
# publicas (columnas "_rango"), no en la capa de datos interna.

columnas_conteo = [
    "IRA_menores_2_meses",
    "IRA_2_a_11_meses",
    "IRA_1_a_4_anos",
    "Neumonia_menores_2_meses",
    "Neumonia_2_a_11_meses",
    "Neumonia_1_a_4_anos",
    "Defunciones"
]

for columna in columnas_conteo:

    if columna in df.columns:

        df = df.withColumn(
            columna + "_rango",
            when(col(columna) == 0, "0")
            .when(col(columna) < 5, "1-4")
            .when(col(columna) < 10, "5-9")
            .when(col(columna) < 20, "10-19")
            .when(col(columna).isNotNull(), "20+")
            .otherwise("SIN_DATO")
        )

print("\n========== EVIDENCIA: ANTES Y DESPUES DE LA PROTECCION ==========")

(
    df
    .select(
        "Distrito",
        "Semana_Epidemiologica",
        "IRA_menores_2_meses",
        "IRA_menores_2_meses_rango",
        "Defunciones",
        "Defunciones_rango"
    )
    .show(10, truncate=False)
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
    "IRA_menores_2_meses_rango",
    "IRA_2_a_11_meses",
    "IRA_2_a_11_meses_rango",
    "IRA_1_a_4_anos",
    "IRA_1_a_4_anos_rango",
    "Neumonia_menores_2_meses",
    "Neumonia_menores_2_meses_rango",
    "Neumonia_2_a_11_meses",
    "Neumonia_2_a_11_meses_rango",
    "Neumonia_1_a_4_anos",
    "Neumonia_1_a_4_anos_rango",
    "Defunciones",
    "Defunciones_rango"
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

