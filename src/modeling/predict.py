"""
Módulo de Inferencia y Generación de Predicciones en PySpark MLlib.
Carga el modelo serializado entrenado y aplica predicciones de morbilidad
sobre lotes de datos entrantes o el dataset curado.
"""

from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.ml import PipelineModel

from src.common.config import get_config, AppConfig
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session

logger = get_logger("Prediccion_ML")


def cargar_modelo(
    model_name: str = "rf_ira_model",
    config: Optional[AppConfig] = None,
) -> "PipelineModel":
    """
    Carga un PipelineModel serializado de Spark MLlib desde el almacenamiento.

    Args:
        model_name: Nombre de la carpeta del modelo en results/modelos/
        config: Configuración de la aplicación.

    Returns:
        PipelineModel: Modelo cargado listo para inferencia.
    """
    from pyspark.ml import PipelineModel

    cfg = config or get_config()
    model_path = cfg.get_model_path(model_name)

    logger.info("Cargando modelo serializado de MLlib desde: %s", model_path)
    return PipelineModel.load(model_path)


def generar_predicciones(
    model: "PipelineModel",
    df_data: "DataFrame",
    target_col: Optional[str] = "ira_total_menores_5",
) -> "DataFrame":
    """
    Genera predicciones con el modelo cargado sobre el DataFrame de entrada.

    Args:
        model: PipelineModel ajustado.
        df_data: DataFrame que contiene las columnas de features requeridas.
        target_col: Nombre de la variable objetivo real (opcional para cálculo de error).

    Returns:
        DataFrame: Con columnas 'prediccion_casos' y opcionalmente 'error_residual'.
    """
    from pyspark.sql.functions import col, round as spark_round, when

    logger.info("Generando inferencias con el modelo...")
    df_preds = model.transform(df_data)

    # Redondear predicción (los conteos de personas no pueden ser negativos)
    df_preds = df_preds.withColumn(
        "prediccion_casos",
        when(col("prediction") < 0, 0.0).otherwise(spark_round(col("prediction"), 2)),
    )

    columnas_salida = [
        "ubigeo",
        "ano",
        "semana_epidemiologica",
        "prediccion_casos",
    ]

    if target_col and target_col in df_preds.columns:
        df_preds = df_preds.withColumnRenamed(target_col, "casos_reales")
        df_preds = df_preds.withColumn(
            "error_residual",
            spark_round(col("casos_reales") - col("prediccion_casos"), 2),
        )
        columnas_salida.extend(["casos_reales", "error_residual"])

    cols_existentes = [c for c in columnas_salida if c in df_preds.columns]
    return df_preds.select(*cols_existentes)


def guardar_predicciones(
    df_predicciones: "DataFrame",
    output_name: str = "predicciones_vigilancia",
    config: Optional[AppConfig] = None,
) -> str:
    """
    Guarda las predicciones generadas en formato Parquet en la capa Gold / Resultados.
    """
    cfg = config or get_config()
    dest_path = cfg.get_predictions_path(output_name)

    logger.info("Guardando predicciones generadas en: %s", dest_path)
    df_predicciones.write.mode("overwrite").parquet(dest_path)
    return dest_path


if __name__ == "__main__":
    cfg = get_config()
    spark = get_spark_session(app_name="Prediccion_Inferencia", config=cfg)
    gold_features_path = f"{cfg.get_gold_path('epidemiologia_clima')}/features_ml"

    logger.info("Cargando features para inferencia desde Gold...")
    df_features = spark.read.parquet(gold_features_path)
    modelo = cargar_modelo(config=cfg)
    preds = generar_predicciones(modelo, df_features)
    guardar_predicciones(preds, config=cfg)
