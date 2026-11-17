"""
Orquestador Principal End-to-End del Pipeline Medallion.
Coordina el flujo completo: Bronze (Ingesta) -> Silver (Limpieza) ->
Gold (Integración & Features) -> Modelo (MLlib) -> Serving (PostgreSQL).
"""

import argparse
import sys
from typing import Optional

from src.common.config import get_config, AppConfig
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session, stop_spark_session
from src.ingestion.cargar_ira import cargar_ira
from src.ingestion.cargar_malaria import cargar_malaria
from src.ingestion.cargar_senamhi import cargar_senamhi
from src.preprocessing.limpiar_ira import procesar_silver_ira
from src.preprocessing.limpiar_malaria import procesar_silver_malaria
from src.preprocessing.limpiar_senamhi import procesar_silver_senamhi
from src.integration.integrar_datasets import procesar_capa_gold
from src.modeling.train import entrenar_modelo_predictivo
from src.modeling.predict import generar_predicciones, guardar_predicciones
from src.serving.cargar_postgres import crear_tablas_postgres, cargar_capa_gold_a_postgres

logger = get_logger("Orquestador_Pipeline")


def ejecutar_pipeline_completo(
    run_ingestion: bool = True,
    run_silver: bool = True,
    run_gold: bool = True,
    run_model: bool = True,
    run_serving: bool = True,
    force_ingest: bool = False,
    config: Optional[AppConfig] = None,
) -> bool:
    """
    Ejecuta el pipeline de extremo a extremo respetando la arquitectura Medallion.
    """
    cfg = config or get_config()
    logger.info("==================================================")
    logger.info("  INICIANDO EJECUCIÓN DEL PIPELINE PREDICTIVO")
    logger.info("  Entorno: %s | Modo AWS: %s", cfg.env, cfg.is_aws)
    logger.info("==================================================")

    # --------------------------------------------------------------------------
    # FASE 1: BRONZE (Ingesta cruda con control SHA-256)
    # --------------------------------------------------------------------------
    if run_ingestion:
        logger.info("\n--- [FASE 1: BRONZE - INGESTA DE LOTES] ---")
        cargar_ira(force=force_ingest)
        cargar_malaria(force=force_ingest)
        cargar_senamhi(force=force_ingest)

    # Iniciar SparkSession centralizada para las fases analíticas
    spark = get_spark_session(app_name="Pipeline_End_to_End", config=cfg)

    try:
        # ----------------------------------------------------------------------
        # FASE 2: SILVER (Limpieza, tipado estricto y Parquet por año)
        # ----------------------------------------------------------------------
        if run_silver:
            logger.info("\n--- [FASE 2: SILVER - PREPROCESAMIENTO] ---")
            procesar_silver_ira(spark=spark, config=cfg)
            procesar_silver_malaria(spark=spark, config=cfg)
            procesar_silver_senamhi(spark=spark, config=cfg)

        # ----------------------------------------------------------------------
        # FASE 3: GOLD (Integración y Feature Engineering con Lags 1-3)
        # ----------------------------------------------------------------------
        df_features = None
        if run_gold or run_model:
            logger.info("\n--- [FASE 3: GOLD - INTEGRACIÓN Y FEATURES] ---")
            dim_u, dim_t, fact_v, df_features = procesar_capa_gold(spark=spark, config=cfg)

        # ----------------------------------------------------------------------
        # FASE 4: MODELADO PREDICTIVO (PySpark MLlib vs Baseline)
        # ----------------------------------------------------------------------
        if run_model and df_features is not None:
            logger.info("\n--- [FASE 4: MODELADO PREDICTIVO MLLIB] ---")
            modelo, reporte = entrenar_modelo_predictivo(df_features, config=cfg)
            preds = generar_predicciones(modelo, df_features)
            guardar_predicciones(preds, config=cfg)

        # ----------------------------------------------------------------------
        # FASE 5: SERVING (Carga a PostgreSQL / RDS)
        # ----------------------------------------------------------------------
        if run_serving:
            logger.info("\n--- [FASE 5: SERVING - POSTGRESQL / RDS] ---")
            crear_tablas_postgres(config=cfg)
            cargar_capa_gold_a_postgres(spark=spark, config=cfg)

        logger.info("\n==================================================")
        logger.info("  PIPELINE EJECUTADO EXITOSAMENTE AL 100%")
        logger.info("==================================================")
        return True

    except Exception as e:
        logger.error("Error crítico durante la ejecución del pipeline: %s", e, exc_info=True)
        return False
    finally:
        stop_spark_session(spark)


def main():
    parser = argparse.ArgumentParser(description="Orquestador del Sistema Predictivo Big Data.")
    parser.add_argument("--all", action="store_true", help="Ejecutar todas las etapas de inicio a fin.")
    parser.add_argument("--ingest", action="store_true", help="Ejecutar solo ingesta Bronze.")
    parser.add_argument("--silver", action="store_true", help="Ejecutar solo limpieza Silver.")
    parser.add_argument("--gold", action="store_true", help="Ejecutar solo integración Gold.")
    parser.add_argument("--model", action="store_true", help="Ejecutar solo entrenamiento e inferencia ML.")
    parser.add_argument("--serving", action="store_true", help="Ejecutar solo carga a PostgreSQL.")
    parser.add_argument("--force", action="store_true", help="Forzar reingesta de archivos omitiendo SHA-256.")
    args = parser.parse_args()

    # Si no se indica ningún argumento específico, por defecto ejecuta todo
    if not (args.ingest or args.silver or args.gold or args.model or args.serving):
        exito = ejecutar_pipeline_completo(force_ingest=args.force)
    else:
        exito = ejecutar_pipeline_completo(
            run_ingestion=args.ingest or args.all,
            run_silver=args.silver or args.all,
            run_gold=args.gold or args.all,
            run_model=args.model or args.all,
            run_serving=args.serving or args.all,
            force_ingest=args.force,
        )

    sys.exit(0 if exito else 1)


if __name__ == "__main__":
    main()
