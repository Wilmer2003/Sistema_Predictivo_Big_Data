"""
Pipeline de Integración de Datos y Construcción de la Capa Gold.
Integra IRA + Malaria + SENAMHI al grano (ubigeo + ano + semana_epidemiologica),
ejecuta Feature Engineering (lags 1 a 3 semanas) y genera el modelo dimensional.
"""

from typing import Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from src.common.config import get_config, AppConfig
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.ingestion.batch_manager import BatchControlManager
from src.integration.feature_engineering import (
    calcular_lags_temporales,
    calcular_medias_moviles,
    calcular_deltas_intersemanales,
)
from src.integration.dimensional_model import (
    construir_dim_ubigeo,
    construir_dim_tiempo,
    construir_fact_vigilancia,
)

logger = get_logger("Integracion_Gold")


def agregar_senamhi_semanal(df_senamhi: "DataFrame") -> "DataFrame":
    """
    Agrega las mediciones meteorológicas diarias a nivel departamental semanal.
    """
    from pyspark.sql.functions import col, avg, sum as spark_sum, when, round as spark_round

    logger.info("Agregando variables meteorológicas de SENAMHI a nivel semanal...")
    return (
        df_senamhi
        .groupBy("Ano", "Semana_Epidemiologica")
        .agg(
            spark_round(avg("Temperatura_maxima"), 2).alias("temp_max_promedio"),
            spark_round(avg("Temperatura_minima"), 2).alias("temp_min_promedio"),
            spark_round(avg("tc"), 2).alias("temp_media_promedio"),
            spark_round(spark_sum("Precipitacion_acumulada"), 2).alias("precipitacion_total"),
            spark_sum(when(col("Precipitacion_acumulada") > 0.1, 1).otherwise(0)).alias("dias_con_lluvia"),
        )
        .withColumnRenamed("Ano", "ano")
        .withColumnRenamed("Semana_Epidemiologica", "semana_epidemiologica")
    )


def agregar_ira_semanal(df_ira: "DataFrame") -> "DataFrame":
    """
    Agrega métricas de IRA al grano (ubigeo, ano, semana_epidemiologica).
    """
    from pyspark.sql.functions import col, sum as spark_sum, coalesce, lit

    logger.info("Agregando métricas de IRA a nivel de ubigeo y semana...")
    return (
        df_ira
        .groupBy("Codigo_Ubigeo", "Ano", "Semana_Epidemiologica")
        .agg(
            spark_sum(coalesce(col("IRA_menores_2_meses"), lit(0))).alias("ira_menores_2m"),
            spark_sum(coalesce(col("IRA_2_a_11_meses"), lit(0))).alias("ira_2_a_11m"),
            spark_sum(coalesce(col("IRA_1_a_4_anos"), lit(0))).alias("ira_1_a_4a"),
            spark_sum(coalesce(col("Neumonia_menores_2_meses"), lit(0))).alias("neumonia_menores_2m"),
            spark_sum(coalesce(col("Neumonia_2_a_11_meses"), lit(0))).alias("neumonia_2_a_11m"),
            spark_sum(coalesce(col("Neumonia_1_a_4_anos"), lit(0))).alias("neumonia_1_a_4a"),
            spark_sum(coalesce(col("Defunciones"), lit(0))).alias("defunciones"),
        )
        .withColumn("ira_total_menores_5", col("ira_menores_2m") + col("ira_2_a_11m") + col("ira_1_a_4a"))
        .withColumn("neumonia_total_menores_5", col("neumonia_menores_2m") + col("neumonia_2_a_11m") + col("neumonia_1_a_4a"))
        .withColumnRenamed("Codigo_Ubigeo", "ubigeo")
        .withColumnRenamed("Ano", "ano")
        .withColumnRenamed("Semana_Epidemiologica", "semana_epidemiologica")
    )


def agregar_malaria_semanal(df_malaria: "DataFrame") -> "DataFrame":
    """
    Agrega casos individuales de Malaria al grano (ubigeo, ano, semana_epidemiologica).
    """
    from pyspark.sql.functions import col, sum as spark_sum, coalesce, lit

    logger.info("Agregando casos de Malaria a nivel de ubigeo y semana...")
    semana_col = "semana" if "semana" in df_malaria.columns else "semana_epidemiologica"

    return (
        df_malaria
        .groupBy("ubigeo", "ano", semana_col)
        .agg(
            spark_sum(coalesce(col("Malaria_total"), lit(0))).alias("malaria_casos_total"),
            spark_sum(coalesce(col("B50"), lit(0))).alias("malaria_b50"),
            spark_sum(coalesce(col("B51"), lit(0))).alias("malaria_b51"),
        )
        .withColumnRenamed(semana_col, "semana_epidemiologica")
    )


def integrar_fuentes_silver(
    df_ira: "DataFrame",
    df_malaria: "DataFrame",
    df_senamhi: "DataFrame",
) -> "DataFrame":
    """
    Integra las tres fuentes procesadas de la capa Silver en un DataFrame unificado.
    """
    ira_agg = agregar_ira_semanal(df_ira)
    malaria_agg = agregar_malaria_semanal(df_malaria)
    clima_agg = agregar_senamhi_semanal(df_senamhi)

    df_epi = ira_agg.join(
        malaria_agg,
        on=["ubigeo", "ano", "semana_epidemiologica"],
        how="left",
    ).fillna({
        "malaria_casos_total": 0,
        "malaria_b50": 0,
        "malaria_b51": 0,
    })

    df_integrado = df_epi.join(
        clima_agg,
        on=["ano", "semana_epidemiologica"],
        how="left",
    )

    return df_integrado


def resumir_cobertura_climatica(
    df_integrado: "DataFrame",
    columna_referencia: str = "temp_media_promedio",
) -> dict:
    """Resume qué proporción de filas distrito-semana tiene clima semanal.

    El clima se agrega a nivel departamental por año y semana, y luego se
    comparte entre los distritos de esa semana. Por ello, el porcentaje mide
    cobertura temporal del clima regional en las filas distrito-semana; no
    cobertura de estaciones meteorológicas dentro de cada distrito.
    """
    from pyspark.sql.functions import col, count, sum as spark_sum, when

    if columna_referencia not in df_integrado.columns:
        raise ValueError(f"Columna de clima '{columna_referencia}' no encontrada.")

    resumen = (
        df_integrado
        .agg(
            count("*").alias("total_distrito_semana"),
            spark_sum(when(col(columna_referencia).isNotNull(), 1).otherwise(0)).alias(
                "distrito_semana_con_clima"
            ),
        )
        .first()
    )
    total = int(resumen["total_distrito_semana"] or 0)
    con_clima = int(resumen["distrito_semana_con_clima"] or 0)
    porcentaje = round(100 * con_clima / total, 2) if total else 0.0
    return {
        "total_distrito_semana": total,
        "distrito_semana_con_clima": con_clima,
        "porcentaje_con_clima": porcentaje,
    }


def aplicar_feature_engineering_completo(df_integrado: "DataFrame") -> "DataFrame":
    """
    Aplica cálculo de lags de 1 a 3 semanas, deltas y medias móviles sobre el dataset integrado.
    """
    columnas_clima_lags = [
        "temp_media_promedio",
        "temp_max_promedio",
        "temp_min_promedio",
        "precipitacion_total",
        "dias_con_lluvia",
    ]
    columnas_epi_lags = [
        "ira_total_menores_5",
        "neumonia_total_menores_5",
        "malaria_casos_total",
    ]

    df_features = calcular_lags_temporales(
        df_integrado,
        columnas_a_rezagar=columnas_clima_lags + columnas_epi_lags,
        partition_col="ubigeo",
        max_lag=3,
    )

    df_features = calcular_medias_moviles(
        df_features,
        columnas=["temp_media_promedio", "precipitacion_total", "ira_total_menores_5"],
        partition_col="ubigeo",
        window_size=3,
    )

    df_features = calcular_deltas_intersemanales(
        df_features,
        columnas_delta=["temp_media_promedio", "ira_total_menores_5"],
    )

    return df_features


def procesar_capa_gold(
    spark: Optional["SparkSession"] = None,
    config: Optional[AppConfig] = None,
) -> Tuple["DataFrame", "DataFrame", "DataFrame", "DataFrame"]:
    """
    Ejecuta el pipeline completo de integración hacia la capa Gold.
    """
    cfg = config or get_config()
    active_spark = spark or get_spark_session(app_name="Integracion_Gold", config=cfg)
    gold_dir = cfg.get_gold_path("epidemiologia_clima")

    silver_ira_path = f"{cfg.get_silver_path('ira')}/parquet"
    silver_malaria_path = f"{cfg.get_silver_path('malaria')}/parquet"
    silver_senamhi_path = f"{cfg.get_silver_path('senamhi')}/parquet"

    logger.info("Cargando datasets Silver para integración...")
    try:
        df_ira = active_spark.read.parquet(silver_ira_path)
    except Exception:
        df_ira = active_spark.read.option("header", True).option("sep", ";").csv(
            str(cfg.root_dir / "data" / "processed" / "ira" / "IRA_La_Libertad_2015_2024.csv")
        )

    try:
        df_malaria = active_spark.read.parquet(silver_malaria_path)
    except Exception:
        df_malaria = active_spark.read.option("header", True).option("sep", ",").csv(
            str(cfg.root_dir / "data" / "processed" / "malaria" / "Malaria_La_Libertad_2015_2024.csv")
        )

    try:
        df_senamhi = active_spark.read.parquet(silver_senamhi_path)
    except Exception:
        df_senamhi = active_spark.read.option("header", True).option("sep", ",").csv(
            str(cfg.root_dir / "data" / "processed" / "senamhi" / "Clima_La_Libertad_2015_2024.csv")
        )

    df_integrado = integrar_fuentes_silver(df_ira, df_malaria, df_senamhi)
    cobertura_clima = resumir_cobertura_climatica(df_integrado)
    logger.info(
        "Cobertura de clima regional: %.2f%% (%d de %d filas distrito-semana).",
        cobertura_clima["porcentaje_con_clima"],
        cobertura_clima["distrito_semana_con_clima"],
        cobertura_clima["total_distrito_semana"],
    )
    df_features = aplicar_feature_engineering_completo(df_integrado)

    dim_ubigeo = construir_dim_ubigeo(df_ira)
    dim_tiempo = construir_dim_tiempo(df_features)
    fact_vigilancia = construir_fact_vigilancia(df_features)

    logger.info("Persistiendo tablas dimensionales y dataset curado en Gold: %s", gold_dir)
    dim_ubigeo.write.mode("overwrite").parquet(f"{gold_dir}/dim_ubigeo")
    dim_tiempo.write.mode("overwrite").parquet(f"{gold_dir}/dim_tiempo")
    fact_vigilancia.write.mode("overwrite").parquet(f"{gold_dir}/fact_vigilancia")
    df_features.write.mode("overwrite").parquet(f"{gold_dir}/features_ml")

    control_mgr = BatchControlManager(config=cfg)
    for b in control_mgr._load_registry().get("batches", []):
        if b.get("status") == "PROCESSED_SILVER":
            control_mgr.update_batch_status(b.get("file_hash"), "PROCESSED_GOLD")

    logger.info("Integración Gold finalizada exitosamente.")
    return dim_ubigeo, dim_tiempo, fact_vigilancia, df_features
