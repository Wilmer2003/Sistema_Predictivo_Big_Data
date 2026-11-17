"""
Módulo Común (src.common) para el Sistema Predictivo Big Data.
Proporciona configuración centralizada, utilidades geo-epidemiológicas, logging
y el ciclo de vida de la sesión Apache Spark.
"""

from src.common.config import (
    AppConfig,
    PostgresConfig,
    SparkSettings,
    get_config,
    CODIGO_DEPARTAMENTO_LA_LIBERTAD,
    NOMBRE_DEPARTAMENTO_LA_LIBERTAD,
    ANIO_INICIAL,
    ANIO_FINAL,
    LONGITUD_UBIGEO,
    DIAGNOSTICOS_MALARIA,
    VARIABLES_CLIMA,
)
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session, stop_spark_session
from src.common.epi_utils import (
    calculate_epi_week,
    calculate_epi_year_and_week,
    is_valid_semana_epidemiologica,
    with_fecha_from_ymd,
    with_semana_epidemiologica,
    with_anio_epidemiologico,
)
from src.common.geo_utils import (
    clean_ubigeo_str,
    is_ubigeo_la_libertad,
    with_standardized_ubigeo,
    filter_la_libertad,
)

__all__ = [
    # Configuración
    "AppConfig",
    "PostgresConfig",
    "SparkSettings",
    "get_config",
    "CODIGO_DEPARTAMENTO_LA_LIBERTAD",
    "NOMBRE_DEPARTAMENTO_LA_LIBERTAD",
    "ANIO_INICIAL",
    "ANIO_FINAL",
    "LONGITUD_UBIGEO",
    "DIAGNOSTICOS_MALARIA",
    "VARIABLES_CLIMA",
    # Logging
    "get_logger",
    # Spark Session
    "get_spark_session",
    "stop_spark_session",
    # Epidemiología
    "calculate_epi_week",
    "calculate_epi_year_and_week",
    "is_valid_semana_epidemiologica",
    "with_fecha_from_ymd",
    "with_semana_epidemiologica",
    "with_anio_epidemiologico",
    # Geo / Ubigeo
    "clean_ubigeo_str",
    "is_ubigeo_la_libertad",
    "with_standardized_ubigeo",
    "filter_la_libertad",
]
