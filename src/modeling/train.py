"""
Módulo de Entrenamiento y Evaluación de Modelos Predictivos en PySpark MLlib.
Aplica división temporal (2015-2022 train / 2023-2024 test), entrena modelos
ensamblados (Random Forest / GBT), evalúa frente al baseline y guarda resultados.
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.ml import PipelineModel

from src.common.config import get_config, AppConfig
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.modeling.pipeline import (
    DEFAULT_FEATURE_COLS,
    TARGET_COL_IRA,
    construir_pipeline_mllib,
    preparar_features_df,
)
from src.modeling.baseline import evaluar_baseline_persistencia

logger = get_logger("Entrenamiento_ML")


def division_temporal_train_test(
    df: "DataFrame",
    split_year: int = 2022,
) -> Tuple["DataFrame", "DataFrame"]:
    """
    Divide el dataset cronológicamente para evitar fugas de información temporal.

    Train: Años <= split_year (2015 - 2022)
    Test: Años > split_year (2023 - 2024)
    """
    from pyspark.sql.functions import col

    train_df = df.filter(col("ano") <= split_year)
    test_df = df.filter(col("ano") > split_year)

    logger.info(
        "División temporal completada: Train (<= %d): %d filas | Test (> %d): %d filas",
        split_year,
        train_df.count(),
        split_year,
        test_df.count(),
    )
    return train_df, test_df


def entrenar_modelo_predictivo(
    df_features: "DataFrame",
    target_col: str = TARGET_COL_IRA,
    tipo_modelo: str = "random_forest",
    split_year: int = 2022,
    model_name: str = "rf_ira_model",
    config: Optional[AppConfig] = None,
) -> Tuple["PipelineModel", Dict[str, Any]]:
    """
    Entrena un pipeline completo de PySpark MLlib y lo evalúa contra el baseline de persistencia.

    Args:
        df_features: DataFrame curado con lags de la capa Gold.
        target_col: Variable objetivo (por defecto 'ira_total_menores_5').
        tipo_modelo: 'random_forest' o 'gbt' o 'linear'.
        split_year: Año de corte temporal para la división de entrenamiento y prueba.
        model_name: Nombre de carpeta para guardar el modelo serializado.
        config: Configuración de la aplicación.

    Returns:
        Tuple[PipelineModel, Dict[str, Any]]: Modelo ajustado y reporte completo de métricas.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.regression import (
        RandomForestRegressor,
        GBTRegressor,
        LinearRegression,
    )
    from pyspark.ml.evaluation import RegressionEvaluator

    cfg = config or get_config()
    df_clean = preparar_features_df(df_features, target_col=target_col)
    train_df, test_df = division_temporal_train_test(df_clean, split_year=split_year)

    # 1. Pipeline de preparación de características
    pipeline_prep = construir_pipeline_mllib(target_col=target_col)

    # 2. Instanciar estimador según tipo solicitado
    if tipo_modelo.lower() == "gbt":
        logger.info("Configurando GBTRegressor (Gradient Boosted Trees)...")
        regressor = GBTRegressor(
            featuresCol="features",
            labelCol=target_col,
            predictionCol="prediction",
            maxIter=30,
            maxDepth=6,
            seed=42,
        )
    elif tipo_modelo.lower() == "linear":
        logger.info("Configurando LinearRegression...")
        regressor = LinearRegression(
            featuresCol="features",
            labelCol=target_col,
            predictionCol="prediction",
            regParam=0.1,
            elasticNetParam=0.5,
        )
    else:
        logger.info("Configurando RandomForestRegressor...")
        regressor = RandomForestRegressor(
            featuresCol="features",
            labelCol=target_col,
            predictionCol="prediction",
            numTrees=50,
            maxDepth=8,
            seed=42,
        )

    # 3. Ensamblar Pipeline completo (Preprocesamiento + Modelo)
    full_pipeline = Pipeline(stages=pipeline_prep.getStages() + [regressor])

    logger.info("Ajustando modelo sobre conjunto de entrenamiento...")
    model = full_pipeline.fit(train_df)

    # 4. Generar predicciones en conjunto de prueba
    logger.info("Evaluando predicciones sobre conjunto de prueba...")
    predictions = model.transform(test_df)

    # 5. Métricas del Modelo
    eval_rmse = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
    eval_mae = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="mae")
    eval_r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2")

    rmse = round(eval_rmse.evaluate(predictions), 4)
    mae = round(eval_mae.evaluate(predictions), 4)
    r2 = round(eval_r2.evaluate(predictions), 4)

    # 6. Métricas del Baseline de Persistencia (y_t = y_{t-1})
    lag1_col = f"{target_col}_lag1"
    baseline_metrics = evaluar_baseline_persistencia(test_df, target_col=target_col, lag1_col=lag1_col)

    # 7. Ganancia / Comparación frente a persistencia
    base_rmse = baseline_metrics["rmse"]
    base_mae = baseline_metrics["mae"]
    mejora_rmse_pct = round(((base_rmse - rmse) / base_rmse) * 100.0, 2) if base_rmse > 0 else 0.0
    mejora_mae_pct = round(((base_mae - mae) / base_mae) * 100.0, 2) if base_mae > 0 else 0.0

    reporte = {
        "timestamp_evaluacion": datetime.now(timezone.utc).isoformat(),
        "modelo": tipo_modelo,
        "target": target_col,
        "split_temporal": {"train_max_year": split_year, "test_min_year": split_year + 1},
        "metricas_modelo": {"rmse": rmse, "mae": mae, "r2": r2},
        "metricas_baseline_persistencia": baseline_metrics,
        "ganancia_vs_baseline": {
            "reduccion_rmse_porcentaje": mejora_rmse_pct,
            "reduccion_mae_porcentaje": mejora_mae_pct,
            "supera_al_baseline": rmse < base_rmse,
        },
    }

    logger.info("==================================================")
    logger.info("  RESULTADOS DE EVALUACIÓN DE MACHINE LEARNING")
    logger.info("==================================================")
    logger.info("Modelo ML   -> RMSE: %.4f | MAE: %.4f | R2: %.4f", rmse, mae, r2)
    logger.info("Baseline    -> RMSE: %.4f | MAE: %.4f | R2: %.4f", base_rmse, base_mae, baseline_metrics["r2"])
    logger.info("Mejora RMSE -> %.2f%% (Supera al baseline: %s)", mejora_rmse_pct, rmse < base_rmse)
    logger.info("==================================================")

    # 8. Persistir modelo y métricas en disco
    dir_modelos = cfg.root_dir / "results" / "modelos" / model_name
    dir_estadisticas = cfg.root_dir / "results" / "estadisticas"
    dir_modelos.parent.mkdir(parents=True, exist_ok=True)
    dir_estadisticas.mkdir(parents=True, exist_ok=True)

    # Guardar modelo de Spark
    try:
        model.write().overwrite().save(str(dir_modelos).replace("\\", "/"))
        logger.info("Modelo serializado guardado en: %s", dir_modelos)
    except Exception as e:
        logger.warning("No se pudo serializar el modelo en disco local: %s", e)

    # Guardar reporte JSON
    reporte_path = dir_estadisticas / "metricas_evaluacion.json"
    with open(reporte_path, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2)
    logger.info("Reporte de métricas guardado en: %s", reporte_path)

    return model, reporte


def ejecutar_entrenamiento_completo() -> Tuple[Any, Dict[str, Any]]:
    """Carga los datos curados de Gold y ejecuta el entrenamiento."""
    cfg = get_config()
    spark = get_spark_session(app_name="Entrenamiento_ML", config=cfg)
    gold_features_path = f"{cfg.get_gold_path('epidemiologia_clima')}/features_ml"

    logger.info("Cargando features desde la capa Gold: %s", gold_features_path)
    df_features = spark.read.parquet(gold_features_path)
    return entrenar_modelo_predictivo(df_features, config=cfg)


if __name__ == "__main__":
    ejecutar_entrenamiento_completo()
