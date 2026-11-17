"""
Módulo de Ingeniería de Características (Feature Engineering) para la Capa Gold.
Genera variables rezagadas (lags de 1 a 3 semanas), deltas intersemanales
y medias móviles de variables climáticas y epidemiológicas sobre ventanas temporales.
"""

from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from src.common.logger import get_logger

logger = get_logger(__name__)


def calcular_lags_temporales(
    df: "DataFrame",
    columnas_a_rezagar: List[str],
    partition_col: str = "ubigeo",
    order_cols: Optional[List[str]] = None,
    max_lag: int = 3,
) -> "DataFrame":
    """
    Calcula variables rezagadas (lags de 1 a max_lag semanas) para una serie temporal
    por partición (ubigeo).
    """
    from pyspark.sql.functions import col, lag
    from pyspark.sql.window import Window

    orden = order_cols or ["ano", "semana_epidemiologica"]
    window_spec = Window.partitionBy(partition_col).orderBy(*orden)

    logger.info(
        "Calculando lags 1 a %d para %d variables agrupadas por '%s'...",
        max_lag,
        len(columnas_a_rezagar),
        partition_col,
    )

    for columna in columnas_a_rezagar:
        if columna in df.columns:
            for l in range(1, max_lag + 1):
                lag_col_name = f"{columna}_lag{l}"
                df = df.withColumn(
                    lag_col_name,
                    lag(col(columna), l).over(window_spec),
                )

    return df


def calcular_medias_moviles(
    df: "DataFrame",
    columnas: List[str],
    partition_col: str = "ubigeo",
    order_cols: Optional[List[str]] = None,
    window_size: int = 3,
) -> "DataFrame":
    """
    Calcula medias móviles (rolling averages) para capturar tendencias climáticas y epidemiológicas.
    """
    from pyspark.sql.functions import col, avg, round as spark_round
    from pyspark.sql.window import Window

    orden = order_cols or ["ano", "semana_epidemiologica"]
    rolling_window = Window.partitionBy(partition_col).orderBy(*orden).rowsBetween(-(window_size - 1), 0)

    for columna in columnas:
        if columna in df.columns:
            rolling_col_name = f"{columna}_media_movil_{window_size}s"
            df = df.withColumn(
                rolling_col_name,
                spark_round(avg(col(columna)).over(rolling_window), 2),
            )

    return df


def calcular_deltas_intersemanales(
    df: "DataFrame",
    columnas_delta: List[str],
) -> "DataFrame":
    """
    Calcula la variación intersemanal (delta_t = val_t - val_{t-1}).
    """
    from pyspark.sql.functions import col, round as spark_round

    for c in columnas_delta:
        lag1 = f"{c}_lag1"
        if c in df.columns and lag1 in df.columns:
            delta_name = f"delta_{c}_1s"
            df = df.withColumn(
                delta_name,
                spark_round(col(c) - col(lag1), 2),
            )
    return df
