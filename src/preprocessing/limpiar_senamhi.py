"""
Pipeline de Limpieza y Estandarización de Datos Meteorológicos de SENAMHI (Capa Silver).
Construye Fecha y Semana Epidemiológica ISO, aplica tipado estricto, valida rangos físicos
(PP >= 0, Tmin <= Tmax), deduplica por Estación + Fecha y escribe Parquet particionado.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    regexp_extract,
    regexp_replace,
)
from pyspark.sql.types import IntegerType, DoubleType

from src.common.config import get_config, AppConfig, ANIO_INICIAL, ANIO_FINAL, NOMBRE_DEPARTAMENTO_LA_LIBERTAD
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.common.epi_utils import with_fecha_from_ymd, with_semana_epidemiologica
from src.ingestion.batch_manager import BatchControlManager
from src.preprocessing.base_preprocessor import (
    normalize_column_names,
    write_partitioned_parquet_incremental,
    mark_pending_batches_failed,
)

logger = get_logger("Limpieza_SENAMHI")

VARIABLES_METEOROLOGICAS = [
    "Precipitacion_acumulada",
    "Temperatura_maxima",
    "Temperatura_minima",
    "tc",
]


def transformar_datos_senamhi(df: DataFrame) -> DataFrame:
    """
    Aplica las transformaciones de limpieza y validación sobre datos de SENAMHI.

    Args:
        df: DataFrame de PySpark con datos meteorológicos crudos.

    Returns:
        DataFrame: Datos procesados y validados para la capa Silver.
    """
    # 1. Normalizar nombres de columnas
    df = normalize_column_names(df)

    # 2. Descomponer Lugar en Estacion y Departamento si viene en formato "ESTACION (DEPARTAMENTO)"
    if "Lugar" in df.columns:
        df = df.withColumn("Lugar", trim(col("Lugar")))
        if "Departamento" not in df.columns:
            df = df.withColumn(
                "Departamento",
                trim(regexp_extract(col("Lugar"), r"\(([^()]+)\)\s*$", 1)),
            )
        if "Estacion" not in df.columns:
            df = df.withColumn(
                "Estacion",
                trim(regexp_replace(col("Lugar"), r"\s*\([^()]+\)\s*$", "")),
            )

    if "Estacion" in df.columns:
        df = df.withColumn("Estacion", trim(col("Estacion")))
    if "Departamento" in df.columns:
        df = df.withColumn("Departamento", trim(col("Departamento")))

    # 3. Conversión de tipos
    for c in ["Ano", "Mes", "Dia"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(IntegerType()))

    for c in VARIABLES_METEOROLOGICAS:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))

    # 4. Construir Fecha y Semana Epidemiológica si no existen
    if "Fecha" not in df.columns and all(c in df.columns for c in ["Ano", "Mes", "Dia"]):
        df = with_fecha_from_ymd(df, anio_col="Ano", mes_col="Mes", dia_col="Dia", output_col="Fecha")

    if "Semana_Epidemiologica" not in df.columns and "Fecha" in df.columns:
        df = with_semana_epidemiologica(df, fecha_col="Fecha", output_col="Semana_Epidemiologica")
    elif "Semana_Epidemiologica" in df.columns:
        df = df.withColumn("Semana_Epidemiologica", col("Semana_Epidemiologica").cast(IntegerType()))

    # 5. Descartar filas completamente vacías o sin fecha válida
    df = df.dropna(how="all")
    if "Fecha" in df.columns:
        df = df.filter(col("Fecha").isNotNull())

    # 6. Filtrar La Libertad
    if "Departamento" in df.columns:
        df = df.filter(upper(trim(col("Departamento"))) == NOMBRE_DEPARTAMENTO_LA_LIBERTAD)

    # 7. Filtrar período 2015 - 2024
    if "Ano" in df.columns:
        df = df.filter((col("Ano") >= ANIO_INICIAL) & (col("Ano") <= ANIO_FINAL))

    # 8. Control de consistencia física
    # Precipitacion no negativa, Temperatura mínima <= Temperatura máxima
    if "Precipitacion_acumulada" in df.columns:
        df = df.filter(col("Precipitacion_acumulada") >= 0.0)

    if "Temperatura_minima" in df.columns and "Temperatura_maxima" in df.columns:
        df = df.filter(col("Temperatura_minima") <= col("Temperatura_maxima"))

    # 9. Deduplicación por clave primaria (Estación + Fecha)
    if "Estacion" in df.columns and "Fecha" in df.columns:
        df = df.dropDuplicates(["Estacion", "Fecha"])
    else:
        df = df.dropDuplicates()

    # 10. Columnas finales estandarizadas
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
        "tc",
    ]
    cols_presentes = [c for c in columnas_finales if c in df.columns]
    return df.select(*cols_presentes)


def procesar_silver_senamhi(
    spark: Optional[SparkSession] = None,
    input_path: Optional[str] = None,
    output_parquet_path: Optional[str] = None,
    config: Optional[AppConfig] = None,
) -> DataFrame:
    """
    Ejecuta el pipeline de Bronze a Silver para SENAMHI.
    """
    cfg = config or get_config()
    active_spark = spark or get_spark_session(app_name="Limpieza_SENAMHI", config=cfg)
    control_mgr = BatchControlManager(config=cfg)

    ruta_in = input_path or f"{cfg.get_bronze_path('senamhi')}/*/*.csv"
    ruta_out = output_parquet_path or f"{cfg.get_silver_path('senamhi')}/parquet"

    try:
        logger.info("Leyendo datos crudos de SENAMHI desde: %s", ruta_in)
        df_raw = (
            active_spark.read
            .option("header", True)
            .option("sep", ",")
            .option("encoding", "UTF-8")
            .csv(ruta_in)
        )

        df_silver = transformar_datos_senamhi(df_raw)

        # Escribir Parquet particionado por 'Ano'
        write_partitioned_parquet_incremental(df_silver, ruta_out, partition_col="Ano")
    except Exception as e:
        mark_pending_batches_failed(control_mgr, "senamhi", e)
        raise

    # Actualizar estado de lotes en el gestor de control
    for b in control_mgr.get_pending_batches(source="senamhi"):
        control_mgr.update_batch_status(b.file_hash, "PROCESSED_SILVER")

    logger.info("Pipeline Silver de SENAMHI concluido con éxito.")
    return df_silver


if __name__ == "__main__":
    procesar_silver_senamhi()
