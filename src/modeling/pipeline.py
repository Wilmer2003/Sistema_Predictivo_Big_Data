"""
Definición del Pipeline de Características de PySpark MLlib.
Construye el ensamble de vectores y escalado estandarizado para features
climáticas y rezagos epidemiológicos.
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.ml import Pipeline

from src.common.logger import get_logger

logger = get_logger(__name__)

# Lista de características climáticas y epidemiológicas rezagadas por defecto
DEFAULT_FEATURE_COLS = [
    # Lags climáticos
    "temp_media_promedio_lag1",
    "temp_media_promedio_lag2",
    "temp_media_promedio_lag3",
    "precipitacion_total_lag1",
    "precipitacion_total_lag2",
    "precipitacion_total_lag3",
    "dias_con_lluvia_lag1",
    # Tendencias y deltas climáticos
    "temp_media_promedio_media_movil_3s",
    "delta_temp_media_promedio_1s",
    # Lags autorregresivos de morbilidad
    "ira_total_menores_5_lag1",
    "ira_total_menores_5_lag2",
    "ira_total_menores_5_lag3",
    "ira_total_menores_5_media_movil_3s",
    "delta_ira_total_menores_5_1s",
]

TARGET_COL_IRA = "ira_total_menores_5"
TARGET_COL_MALARIA = "malaria_casos_total"


def preparar_features_df(
    df: "DataFrame",
    target_col: str = TARGET_COL_IRA,
    feature_cols: Optional[List[str]] = None,
) -> "DataFrame":
    """
    Filtra y prepara el DataFrame eliminando registros con nulos en las variables
    predictoras y la variable objetivo para el entrenamiento.

    Args:
        df: DataFrame de la capa Gold (features_ml).
        target_col: Nombre de la variable dependiente a predecir.
        feature_cols: Lista de nombres de columnas de entrada.

    Returns:
        DataFrame: Limpio, sin nulos en las columnas seleccionadas.
    """
    from pyspark.sql.functions import col

    cols = feature_cols or DEFAULT_FEATURE_COLS
    cols_existentes = [c for c in cols if c in df.columns]

    if target_col not in df.columns:
        raise ValueError(f"Variable objetivo '{target_col}' no encontrada en el DataFrame.")

    # Asegurar que no haya nulos en las columnas clave del modelo
    cols_a_validar = cols_existentes + [target_col]
    df_clean = df.dropna(subset=cols_a_validar)

    logger.info(
        "Preparado dataset para modelado. Registros válidos: %d con %d features.",
        df_clean.count(),
        len(cols_existentes),
    )
    return df_clean


def construir_pipeline_mllib(
    feature_cols: Optional[List[str]] = None,
    output_features_col: str = "features",
) -> "Pipeline":
    """
    Construye las etapas del pipeline de PySpark MLlib:
    1. VectorAssembler: Consolida las columnas numéricas en un vector denso.
    2. StandardScaler: Estandariza con media 0 y varianza unitaria.

    Args:
        feature_cols: Lista de nombres de columnas a transformar en vector.
        output_features_col: Nombre de la columna de vector escalado final.

    Returns:
        Pipeline: Objeto Pipeline de PySpark listo para ajustar y transformar.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler, StandardScaler

    cols = feature_cols or DEFAULT_FEATURE_COLS

    assembler = VectorAssembler(
        inputCols=cols,
        outputCol="features_raw",
        handleInvalid="skip",
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol=output_features_col,
        withStd=True,
        withMean=False,
    )

    return Pipeline(stages=[assembler, scaler])
