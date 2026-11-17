"""
Módulo de Preprocesamiento y Capa Silver (src.preprocessing).
Responsable de la limpieza, tipado estricto, deduplicación y almacenamiento
en Parquet particionado para IRA, Malaria y SENAMHI.
"""

from src.preprocessing.base_preprocessor import (
    normalize_column_names,
    trim_text_columns,
    apply_small_cell_protection,
    write_partitioned_parquet_incremental,
)
from src.preprocessing.limpiar_ira import transformar_datos_ira, procesar_silver_ira
from src.preprocessing.limpiar_malaria import transformar_datos_malaria, procesar_silver_malaria
from src.preprocessing.limpiar_senamhi import transformar_datos_senamhi, procesar_silver_senamhi

__all__ = [
    "normalize_column_names",
    "trim_text_columns",
    "apply_small_cell_protection",
    "write_partitioned_parquet_incremental",
    "transformar_datos_ira",
    "procesar_silver_ira",
    "transformar_datos_malaria",
    "procesar_silver_malaria",
    "transformar_datos_senamhi",
    "procesar_silver_senamhi",
]
