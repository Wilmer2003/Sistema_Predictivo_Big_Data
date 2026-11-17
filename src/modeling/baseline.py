"""
Modelo Baseline de Persistencia (y_t = y_{t-1}).
Proporciona el umbral de referencia obligatoria para evaluar si los modelos de Machine Learning
generan una ganancia predictiva real frente a la inercia temporal.
"""

import math
from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from src.common.logger import get_logger

logger = get_logger(__name__)


def calcular_metricas_pure_python(y_true: List[float], y_pred: List[float]) -> Dict[str, float]:
    """
    Calcula RMSE, MAE y R2 usando Python estándar (útil para validación unitaria sin Spark).
    """
    n = len(y_true)
    if n == 0 or len(y_pred) != n:
        raise ValueError("Las listas de valores reales y predicciones deben tener la misma longitud > 0.")

    mae = sum(abs(t - p) for t, p in zip(y_true, y_pred)) / n
    mse = sum((t - p) ** 2 for t, p in zip(y_true, y_pred)) / n
    rmse = math.sqrt(mse)

    mean_y = sum(y_true) / n
    ss_tot = sum((t - mean_y) ** 2 for t in y_true)
    ss_res = sum((t - p) ** 2 for t, p in zip(y_true, y_pred))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    return {
        "rmse": round(rmse, 4),
        "mae": round(mae, 4),
        "r2": round(r2, 4),
    }


def evaluar_baseline_persistencia(
    df_eval: "DataFrame",
    target_col: str = "ira_total_menores_5",
    lag1_col: str = "ira_total_menores_5_lag1",
) -> Dict[str, float]:
    """
    Evalúa el baseline de persistencia en PySpark MLlib.
    Predice la morbilidad de la semana actual utilizando directamente el valor
    observado la semana anterior (y_pred = lag1).

    Args:
        df_eval: DataFrame de prueba con columnas 'target_col' y 'lag1_col'.
        target_col: Nombre de la columna real observada.
        lag1_col: Nombre de la columna del período anterior.

    Returns:
        Dict[str, float]: Diccionario con las métricas {'rmse', 'mae', 'r2'}.
    """
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.sql.functions import col

    logger.info("Evaluando Baseline de Persistencia (y_hat = %s)...", lag1_col)

    df_base = df_eval.dropna(subset=[target_col, lag1_col])

    eval_rmse = RegressionEvaluator(labelCol=target_col, predictionCol=lag1_col, metricName="rmse")
    eval_mae = RegressionEvaluator(labelCol=target_col, predictionCol=lag1_col, metricName="mae")
    eval_r2 = RegressionEvaluator(labelCol=target_col, predictionCol=lag1_col, metricName="r2")

    rmse = eval_rmse.evaluate(df_base)
    mae = eval_mae.evaluate(df_base)
    r2 = eval_r2.evaluate(df_base)

    metricas = {
        "rmse": round(rmse, 4),
        "mae": round(mae, 4),
        "r2": round(r2, 4),
    }

    logger.info("Métricas Baseline Persistencia: RMSE=%.4f | MAE=%.4f | R2=%.4f", rmse, mae, r2)
    return metricas
