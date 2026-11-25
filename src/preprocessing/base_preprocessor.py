"""
Utilidades y Funciones Base para el Preprocesamiento (Capa Silver).
Manejo de nombres de columnas, escritura dinámica en Parquet particionado
y protección de celdas pequeñas para privacidad de datos sensibles.
"""

from __future__ import annotations
from typing import List, Optional, Any

try:
    from pyspark.sql import DataFrame
except ImportError:
    DataFrame = Any  # type: ignore

from src.common.logger import get_logger

logger = get_logger(__name__)


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normaliza los nombres de columnas de un DataFrame eliminando tildes,
    espacios y caracteres especiales.

    Args:
        df: DataFrame de PySpark.

    Returns:
        DataFrame: Con nombres de columnas normalizados.
    """
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
        if nuevo_nombre != nombre:
            df = df.withColumnRenamed(nombre, nuevo_nombre)
    return df


def trim_text_columns(df: DataFrame, columnas: List[str]) -> DataFrame:
    """Aplica trim a una lista de columnas de texto existentes."""
    from pyspark.sql.functions import col, trim

    for c in columnas:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))
    return df


def apply_small_cell_protection(
    df: DataFrame,
    columnas_conteo: List[str],
) -> DataFrame:
    """
    Aplica bandeo/generalización sobre celdas pequeñas (conteos sensibles < 5)
    para mitigar el riesgo de inferencia individual en salud pública.
    Genera columnas complementarias con sufijo '_rango'.

    Bandas: 0, 1-4, 5-9, 10-19, 20+, SIN_DATO.
    """
    from pyspark.sql.functions import col, when

    for columna in columnas_conteo:
        if columna in df.columns:
            df = df.withColumn(
                f"{columna}_rango",
                when(col(columna) == 0, "0")
                .when(col(columna) < 5, "1-4")
                .when(col(columna) < 10, "5-9")
                .when(col(columna) < 20, "10-19")
                .when(col(columna).isNotNull(), "20+")
                .otherwise("SIN_DATO"),
            )
    return df


def write_partitioned_parquet_incremental(
    df: DataFrame,
    output_path: str,
    partition_col: str = "Ano",
) -> None:
    """
    Escribe el DataFrame en formato Parquet particionado por la columna indicada,
    empleando 'partitionOverwriteMode=dynamic' para sobrescribir únicamente
    las particiones presentes en el lote sin alterar las particiones de años no afectados.

    Args:
        df: DataFrame procesado.
        output_path: Ruta base destino para los archivos Parquet de la capa Silver.
        partition_col: Columna de particionamiento (por defecto 'Ano').
    """
    spark = df.sparkSession
    # Habilitar sobreescritura dinámica de particiones en Spark
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info(
        "Escribiendo Parquet incremental en '%s' (particionado por %s, modo dinámico)...",
        output_path,
        partition_col,
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy(partition_col)
        .parquet(output_path)
    )

    logger.info("Escritura en Parquet completada exitosamente en: %s", output_path)
