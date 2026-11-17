"""
Módulo de Integración y Capa Gold (src.integration).
Consolida fuentes Silver (IRA, Malaria, SENAMHI), genera variables rezagadas (lags),
construye el modelo dimensional analítico y exporta datasets curados.
"""

from src.integration.feature_engineering import (
    calcular_lags_temporales,
    calcular_medias_moviles,
    calcular_deltas_intersemanales,
)
from src.integration.dimensional_model import (
    ESTACION_UBIGEO_MAP,
    construir_dim_ubigeo,
    construir_dim_tiempo,
    construir_fact_vigilancia,
)
from src.integration.integrar_datasets import (
    agregar_senamhi_semanal,
    agregar_ira_semanal,
    agregar_malaria_semanal,
    integrar_fuentes_silver,
    aplicar_feature_engineering_completo,
    procesar_capa_gold,
)

__all__ = [
    "ESTACION_UBIGEO_MAP",
    "calcular_lags_temporales",
    "calcular_medias_moviles",
    "calcular_deltas_intersemanales",
    "construir_dim_ubigeo",
    "construir_dim_tiempo",
    "construir_fact_vigilancia",
    "agregar_senamhi_semanal",
    "agregar_ira_semanal",
    "agregar_malaria_semanal",
    "integrar_fuentes_silver",
    "aplicar_feature_engineering_completo",
    "procesar_capa_gold",
]
