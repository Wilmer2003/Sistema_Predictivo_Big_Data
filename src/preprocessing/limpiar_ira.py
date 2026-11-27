"""
Pipeline de Limpieza y Estandarización de Datos de IRA (Capa Silver).
Aplica tipado estricto, normalización de Ubigeo, filtrado para La Libertad (13),
rango 2015-2024, protección de celdas pequeñas y escritura en Parquet particionado.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

from src.common.config import get_config, AppConfig, ANIO_INICIAL, ANIO_FINAL
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.common.geo_utils import with_standardized_ubigeo, filter_la_libertad
from src.ingestion.batch_manager import BatchControlManager
from src.preprocessing.base_preprocessor import (
    normalize_column_names,
    trim_text_columns,
    apply_small_cell_protection,
    write_partitioned_parquet_incremental,
)

logger = get_logger("Limpieza_IRA")

COLUMNAS_CONTEO_IRA = [
    "IRA_menores_2_meses",
    "IRA_2_a_11_meses",
    "IRA_1_a_4_anos",
    "Neumonia_menores_2_meses",
    "Neumonia_2_a_11_meses",
    "Neumonia_1_a_4_anos",
    "Defunciones",
]


def transformar_datos_ira(df: DataFrame) -> DataFrame:
    """
    Aplica todas las transformaciones de limpieza y validación sobre el DataFrame de IRA.

    Args:
        df: DataFrame de PySpark con datos crudos de IRA.

    Returns:
        DataFrame: Datos limpios, tipados y protegidos para la capa Silver.
    """
    # 1. Normalizar nombres de columnas
    df = normalize_column_names(df)

    # 2. Trim de campos de texto descriptivos
    df = trim_text_columns(df, ["Departamento", "Provincia", "Distrito"])

    # 3. Normalizar Ubigeo a String(6) con ceros a la izquierda
    if "Codigo_Ubigeo" in df.columns:
        df = with_standardized_ubigeo(df, ubigeo_col="Codigo_Ubigeo", output_col="Codigo_Ubigeo")

    # 4. Tipado estricto
    if "Ano" in df.columns:
        df = df.withColumn("Ano", col("Ano").cast(IntegerType()))

    if "Semana_Epidemiologica" in df.columns:
        df = df.withColumn("Semana_Epidemiologica", col("Semana_Epidemiologica").cast(IntegerType()))

    for c in COLUMNAS_CONTEO_IRA:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(IntegerType()))

    # 5. Descartar filas completamente vacías
    df = df.dropna(how="all")

    # 6. Filtrar estrictamente La Libertad (código 13 o nombre)
    df = filter_la_libertad(df, ubigeo_col="Codigo_Ubigeo", dpto_col="Departamento")

    # 7. Filtrar período común 2015 - 2024 y semanas válidas (1 a 53, 2024 hasta semana 26)
    df = df.filter(
        (col("Ano") >= ANIO_INICIAL) &
        (col("Ano") <= ANIO_FINAL) &
        (col("Semana_Epidemiologica") >= 1) &
        (col("Semana_Epidemiologica") <= 53) &
        ((col("Ano") < 2024) | ((col("Ano") == 2024) & (col("Semana_Epidemiologica") <= 26)))
    )

    # 8. Deduplicación por clave de negocio
    clave_dedup = ["Codigo_Ubigeo", "Ano", "Semana_Epidemiologica"]
    if all(k in df.columns for k in clave_dedup):
        df = df.dropDuplicates(clave_dedup)
    else:
        df = df.dropDuplicates()

    # 9. Protección de datos sensibles / celdas pequeñas
    df = apply_small_cell_protection(df, COLUMNAS_CONTEO_IRA)

    # 10. Selección y ordenamiento de columnas finales
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
        "Defunciones_rango",
    ]
    cols_presentes = [c for c in columnas_finales if c in df.columns]
    return df.select(*cols_presentes)


def procesar_silver_ira(
    spark: Optional[SparkSession] = None,
    input_path: Optional[str] = None,
    output_parquet_path: Optional[str] = None,
    config: Optional[AppConfig] = None,
) -> DataFrame:
    """
    Ejecuta el pipeline completo de Bronze a Silver para IRA.

    Lee los datos crudos, ejecuta transformaciones y escribe el dataset
    Parquet particionado por 'Ano' en la capa Silver.
    """
    cfg = config or get_config()
    active_spark = spark or get_spark_session(app_name="Limpieza_IRA", config=cfg)

    # Ruta de entrada Bronze
    ruta_in = input_path or f"{cfg.get_bronze_path('ira')}/*/*.csv"
    ruta_out = output_parquet_path or f"{cfg.get_silver_path('ira')}/parquet"

    logger.info("Leyendo datos crudos de IRA desde: %s", ruta_in)
    df_raw = (
        active_spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .csv(ruta_in)
    )

    df_silver = transformar_datos_ira(df_raw)

    # Materializar y escribir a Parquet particionado
    write_partitioned_parquet_incremental(df_silver, ruta_out, partition_col="Ano")

    # Actualizar estado de lotes en el gestor de control
    control_mgr = BatchControlManager(config=cfg)
    control_mgr.mark_batches_as_silver("ira", df_raw.inputFiles() if cfg.is_aws else None)

    logger.info("Pipeline Silver de IRA concluido con éxito.")
    return df_silver


if __name__ == "__main__":
    procesar_silver_ira()
