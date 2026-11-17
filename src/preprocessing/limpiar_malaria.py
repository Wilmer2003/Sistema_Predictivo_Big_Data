"""
Pipeline de Limpieza y Estandarización de Datos de Malaria (Capa Silver).
Aplica tipado estricto, normalización de Ubigeo (6 dígitos) y localcod (10 dígitos),
filtrado para La Libertad (13), diagnósticos B50/B51 y escritura en Parquet particionado.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lpad, regexp_replace, trim
from pyspark.sql.types import IntegerType

from src.common.config import (
    get_config,
    AppConfig,
    ANIO_INICIAL,
    ANIO_FINAL,
    DIAGNOSTICOS_MALARIA,
)
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.common.geo_utils import with_standardized_ubigeo, filter_la_libertad
from src.ingestion.batch_manager import BatchControlManager
from src.preprocessing.base_preprocessor import (
    normalize_column_names,
    trim_text_columns,
    write_partitioned_parquet_incremental,
)

logger = get_logger("Limpieza_Malaria")

COLUMNAS_TEXTO_MALARIA = [
    "departamento",
    "provincia",
    "distrito",
    "localidad",
    "enfermedad",
    "diagnostic",
    "diresa",
    "tipo_edad",
    "sexo",
]


def transformar_datos_malaria(df: DataFrame) -> DataFrame:
    """
    Aplica las transformaciones de limpieza y validación sobre datos de Malaria.

    Args:
        df: DataFrame de PySpark con datos crudos de Malaria.

    Returns:
        DataFrame: Datos limpios y estructurados para la capa Silver.
    """
    # 1. Normalizar nombres de columnas
    df = normalize_column_names(df)

    # 2. Trim en campos de texto
    df = trim_text_columns(df, COLUMNAS_TEXTO_MALARIA)

    # 3. Normalizar Ubigeo a 6 dígitos
    if "ubigeo" in df.columns:
        df = with_standardized_ubigeo(df, ubigeo_col="ubigeo", output_col="ubigeo")

    # 4. Normalizar localcod a 10 dígitos (código de centro poblado)
    if "localcod" in df.columns:
        df = df.withColumn(
            "localcod",
            lpad(regexp_replace(trim(col("localcod").cast("string")), r"\.0+$", ""), 10, "0"),
        )

    # 5. Conversión de tipos de datos
    for c in ["ano", "semana", "edad", "Malaria_total", "B50", "B51"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(IntegerType()))

    # 6. Descartar filas completamente vacías
    df = df.dropna(how="all")

    # 7. Filtrar La Libertad
    df = filter_la_libertad(df, ubigeo_col="ubigeo", dpto_col="departamento")

    # 8. Filtrar rango temporal 2015-2024 y semanas válidas
    df = df.filter(
        (col("ano") >= ANIO_INICIAL) &
        (col("ano") <= ANIO_FINAL) &
        (col("semana") >= 1) &
        (col("semana") <= 53)
    )

    # 9. Filtrar diagnósticos oficiales de Malaria (B50: P. falciparum, B51: P. vivax)
    if "diagnostic" in df.columns:
        df = df.filter(col("diagnostic").isin(list(DIAGNOSTICOS_MALARIA)))

    # 10. Deduplicación por clave de negocio
    clave_dedup = ["ubigeo", "ano", "semana", "localidad", "diagnostic", "sexo", "edad"]
    cols_dedup = [c for c in clave_dedup if c in df.columns]
    if cols_dedup:
        df = df.dropDuplicates(cols_dedup)
    else:
        df = df.dropDuplicates()

    # 11. Columnas finales
    columnas_finales = [
        "departamento",
        "provincia",
        "distrito",
        "localidad",
        "enfermedad",
        "ano",
        "semana",
        "diagnostic",
        "diresa",
        "ubigeo",
        "localcod",
        "edad",
        "tipo_edad",
        "sexo",
        "Malaria_total",
        "B50",
        "B51",
    ]
    cols_presentes = [c for c in columnas_finales if c in df.columns]
    return df.select(*cols_presentes)


def procesar_silver_malaria(
    spark: Optional[SparkSession] = None,
    input_path: Optional[str] = None,
    output_parquet_path: Optional[str] = None,
    config: Optional[AppConfig] = None,
) -> DataFrame:
    """
    Ejecuta el pipeline de Bronze a Silver para Malaria.
    """
    cfg = config or get_config()
    active_spark = spark or get_spark_session(app_name="Limpieza_Malaria", config=cfg)

    ruta_in = input_path or f"{cfg.get_bronze_path('malaria')}/*/*.csv"
    ruta_out = output_parquet_path or f"{cfg.get_silver_path('malaria')}/parquet"

    logger.info("Leyendo datos crudos de Malaria desde: %s", ruta_in)
    df_raw = (
        active_spark.read
        .option("header", True)
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .csv(ruta_in)
    )

    df_silver = transformar_datos_malaria(df_raw)

    # Escribir Parquet particionado por 'ano'
    write_partitioned_parquet_incremental(df_silver, ruta_out, partition_col="ano")

    # Actualizar estado de lotes en el gestor de control
    control_mgr = BatchControlManager(config=cfg)
    for b in control_mgr.get_pending_batches(source="malaria"):
        control_mgr.update_batch_status(b.file_hash, "PROCESSED_SILVER")

    logger.info("Pipeline Silver de Malaria concluido con éxito.")
    return df_silver


if __name__ == "__main__":
    procesar_silver_malaria()
