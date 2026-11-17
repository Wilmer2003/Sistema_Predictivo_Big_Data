"""
Definición y Generación del Modelo Dimensional (Esquema Estrella).
Crea las tablas de dimensiones (dim_ubigeo, dim_tiempo) y la tabla de hechos
(fact_vigilancia_epidemiologica_clima) optimizadas para Serving y Power BI.
"""

from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Catálogo de mapeo de estaciones SENAMHI a Ubigeos distritales en La Libertad
ESTACION_UBIGEO_MAP = {
    "CACHICADAN": "131003",       # Cachicadán (Santiago de Chuco)
    "CALLANCAS": "130604",        # Charat (Otuzco)
    "HUACAMARCANGA": "130502",    # Calamarca (Julcán)
    "HUAMACHUCO": "130901",       # Huamachuco (Sánchez Carrión)
    "HUANGACOCHA": "130901",      # Huamachuco (Sánchez Carrión)
    "JULCAN": "130501",           # Julcán (Julcán)
    "MOLLEPATA": "131004",        # Mollepata (Santiago de Chuco)
    "QUIRUVILCA": "131006",       # Quiruvilca (Santiago de Chuco)
    "SINSICAP": "130613",         # Sinsicap (Otuzco)
    "TALLA (GUADALUPE)": "130702",# Guadalupe (Pacasmayo)
    "GUADALUPE": "130702",        # Guadalupe (Pacasmayo)
}


def construir_dim_ubigeo(df_ira: "DataFrame") -> "DataFrame":
    """
    Construye la dimensión de Ubigeos a partir de los datos consolidados de distritos.
    """
    from pyspark.sql.functions import col
    from pyspark.sql.types import BooleanType

    cols_base = ["Codigo_Ubigeo", "Departamento", "Provincia", "Distrito"]
    cols_presentes = [c for c in cols_base if c in df_ira.columns]

    dim = (
        df_ira.select(*cols_presentes)
        .withColumnRenamed("Codigo_Ubigeo", "ubigeo")
        .withColumnRenamed("Departamento", "departamento")
        .withColumnRenamed("Provincia", "provincia")
        .withColumnRenamed("Distrito", "distrito")
        .dropna(subset=["ubigeo"])
        .dropDuplicates(["ubigeo"])
    )

    ubigeos_con_estacion = list(set(ESTACION_UBIGEO_MAP.values()))
    dim = dim.withColumn(
        "tiene_estacion_senamhi",
        col("ubigeo").isin(ubigeos_con_estacion).cast(BooleanType()),
    )

    return dim.orderBy("ubigeo")


def construir_dim_tiempo(df_base: "DataFrame") -> "DataFrame":
    """
    Construye la dimensión temporal a nivel de semana epidemiológica.
    """
    from pyspark.sql.functions import col, ceil
    from pyspark.sql.types import IntegerType

    cols = ["ano", "semana_epidemiologica"]
    dim = (
        df_base.select(*[col(c).cast(IntegerType()) for c in cols])
        .dropna()
        .dropDuplicates()
    )

    dim = dim.withColumn("tiempo_id", col("ano") * 100 + col("semana_epidemiologica"))

    dim = (
        dim
        .withColumn("trimestre", ceil(col("semana_epidemiologica") / 13.0).cast(IntegerType()))
        .withColumn("mes_aproximado", ceil(col("semana_epidemiologica") / 4.42).cast(IntegerType()))
    )

    return dim.select("tiempo_id", "ano", "semana_epidemiologica", "trimestre", "mes_aproximado").orderBy("tiempo_id")


def construir_fact_vigilancia(
    df_integrado: "DataFrame",
) -> "DataFrame":
    """
    Construye la tabla de hechos 'fact_vigilancia_epidemiologica_clima'.
    """
    from pyspark.sql.functions import col
    from pyspark.sql.types import IntegerType

    fact = df_integrado.withColumn(
        "tiempo_id",
        (col("ano") * 100 + col("semana_epidemiologica")).cast(IntegerType()),
    )

    columnas_fact = [
        "ubigeo",
        "tiempo_id",
        "ano",
        "semana_epidemiologica",
        "ira_total_menores_5",
        "neumonia_total_menores_5",
        "defunciones",
        "ira_menores_2m",
        "ira_2_a_11m",
        "ira_1_a_4a",
        "neumonia_menores_2m",
        "neumonia_2_a_11m",
        "neumonia_1_a_4a",
        "malaria_casos_total",
        "malaria_b50",
        "malaria_b51",
        "temp_media_promedio",
        "temp_max_promedio",
        "temp_min_promedio",
        "precipitacion_total",
        "dias_con_lluvia",
    ]

    cols_existentes = [c for c in columnas_fact if c in fact.columns]
    return fact.select(*cols_existentes)
