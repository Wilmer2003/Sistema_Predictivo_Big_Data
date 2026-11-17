"""
Módulo de Modelado Predictivo con PySpark MLlib (src.modeling).
Contiene ensamblado de características, baseline de persistencia, entrenamiento temporal,
evaluación de métricas (RMSE, MAE, R2) e inferencia.
"""

from src.modeling.pipeline import (
    DEFAULT_FEATURE_COLS,
    TARGET_COL_IRA,
    TARGET_COL_MALARIA,
    preparar_features_df,
    construir_pipeline_mllib,
)
from src.modeling.baseline import (
    calcular_metricas_pure_python,
    evaluar_baseline_persistencia,
)
from src.modeling.train import (
    division_temporal_train_test,
    entrenar_modelo_predictivo,
    ejecutar_entrenamiento_completo,
)
from src.modeling.predict import (
    cargar_modelo,
    generar_predicciones,
    guardar_predicciones,
)

__all__ = [
    "DEFAULT_FEATURE_COLS",
    "TARGET_COL_IRA",
    "TARGET_COL_MALARIA",
    "preparar_features_df",
    "construir_pipeline_mllib",
    "calcular_metricas_pure_python",
    "evaluar_baseline_persistencia",
    "division_temporal_train_test",
    "entrenar_modelo_predictivo",
    "ejecutar_entrenamiento_completo",
    "cargar_modelo",
    "generar_predicciones",
    "guardar_predicciones",
]
