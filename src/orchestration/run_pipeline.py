"""
Orquestador Principal End-to-End del Pipeline Medallion.
Coordina el flujo completo: Bronze (Ingesta) -> Silver (Limpieza) ->
Gold (Integración & Features) -> Modelo (MLlib) -> Serving (Cloud SQL / PostgreSQL).
Soporta ejecución local y lanzamiento de jobs sobre Google Cloud Dataproc.
"""

import argparse
import sys
from typing import Optional, List, Dict, Any

from src.common.config import get_config, AppConfig
from src.common.logger import get_logger

logger = get_logger("Orquestador_Pipeline")


def lanzar_job_dataproc(
    main_python_file_uri: str,
    job_args: Optional[List[str]] = None,
    config: Optional[AppConfig] = None,
) -> bool:
    """
    Envía un trabajo PySpark al clúster de Google Cloud Dataproc (imagen 2.2 / Spark 3.5.3)
    mediante la API de Dataproc (JobControllerClient).

    Args:
        main_python_file_uri: URI en GCS (gs://...) o ruta del script ejecutable.
        job_args: Argumentos de línea de comandos para el script.
        config: Configuración de la aplicación (AppConfig).

    Returns:
        bool: True si el job finalizó exitosamente en Dataproc, False en caso de error.
    """
    cfg = config or get_config()
    logger.info("Preparando envío de trabajo PySpark a Dataproc...")
    logger.info("Proyecto: %s | Región: %s | Clúster: %s", cfg.gcp_project_id, cfg.gcp_region, cfg.dataproc_cluster_name)

    try:
        from google.cloud import dataproc_v1 as dataproc

        endpoint = f"{cfg.gcp_region}-dataproc.googleapis.com:443"
        job_client = dataproc.JobControllerClient(
            client_options={"api_endpoint": endpoint}
        )

        job_payload = {
            "placement": {"cluster_name": cfg.dataproc_cluster_name},
            "pyspark_job": {
                "main_python_file_uri": main_python_file_uri,
                "args": job_args or [],
                "properties": {
                    "spark.jars.packages": "org.postgresql:postgresql:42.7.3",
                    "spark.sql.sources.partitionOverwriteMode": "dynamic",
                },
            },
        }

        logger.info("Enviando PySpark job a Dataproc...")
        operation = job_client.submit_job_as_operation(
            request={
                "project_id": cfg.gcp_project_id,
                "region": cfg.gcp_region,
                "job": job_payload,
            }
        )
        response = operation.result()
        state = response.status.state
        state_name = getattr(state, "name", str(state))
        logger.info("Trabajo Dataproc finalizado con estado: %s", state_name)
        return state == dataproc.JobStatus.State.DONE

    except Exception as e:
        logger.error("Error al enviar o ejecutar el trabajo en Dataproc: %s", e)
        return False


def ejecutar_pipeline_completo(
    run_ingestion: bool = True,
    run_silver: bool = True,
    run_gold: bool = True,
    run_model: bool = True,
    run_serving: bool = True,
    force_ingest: bool = False,
    submit_to_dataproc: bool = False,
    config: Optional[AppConfig] = None,
) -> bool:
    """
    Ejecuta el pipeline de extremo a extremo respetando la arquitectura Medallion.
    En modo local ejecuta PySpark localmente.
    En modo GCP desde la laptop, ingesta a GCS y puede despachar el cómputo hacia Dataproc.
    """
    cfg = config or get_config()
    logger.info("==================================================")
    logger.info("  INICIANDO EJECUCIÓN DEL PIPELINE PREDICTIVO")
    logger.info("  Entorno: %s | Modo Google Cloud: %s", cfg.env, cfg.is_gcp)
    logger.info("==================================================")

    # --------------------------------------------------------------------------
    # FASE 1: BRONZE (Ingesta cruda con control SHA-256)
    # En GCP sube a Cloud Storage (gs://) y registra en tabla 'lotes' de Cloud SQL
    # --------------------------------------------------------------------------
    if run_ingestion:
        from src.ingestion.cargar_ira import cargar_ira
        from src.ingestion.cargar_malaria import cargar_malaria
        from src.ingestion.cargar_senamhi import cargar_senamhi

        logger.info("\n--- [FASE 1: BRONZE - INGESTA DE LOTES] ---")
        cargar_ira(force=force_ingest)
        cargar_malaria(force=force_ingest)
        cargar_senamhi(force=force_ingest)

    # Si se solicita despachar las fases de procesamiento Spark a Dataproc:
    if cfg.is_gcp and submit_to_dataproc:
        logger.info("\n--- [DESPACHANDO COMPUTO A DATAPROC CLUSTER] ---")
        script_gcs = f"gs://{cfg.gcs_bucket}/scripts/run_pipeline.py"
        args_dataproc = ["--silver", "--gold", "--model", "--serving"]
        return lanzar_job_dataproc(script_gcs, job_args=args_dataproc, config=cfg)

    from src.common.spark_session import get_spark_session, stop_spark_session

    # Ejecución de PySpark (en clúster o en entorno local)
    spark = get_spark_session(app_name="Pipeline_End_to_End", config=cfg)

    try:
        # ----------------------------------------------------------------------
        # FASE 2: SILVER (Limpieza, tipado estricto y Parquet por año)
        # ----------------------------------------------------------------------
        if run_silver:
            from src.preprocessing.limpiar_ira import procesar_silver_ira
            from src.preprocessing.limpiar_malaria import procesar_silver_malaria
            from src.preprocessing.limpiar_senamhi import procesar_silver_senamhi

            logger.info("\n--- [FASE 2: SILVER - PREPROCESAMIENTO] ---")
            procesar_silver_ira(spark=spark, config=cfg)
            procesar_silver_malaria(spark=spark, config=cfg)
            procesar_silver_senamhi(spark=spark, config=cfg)

        # ----------------------------------------------------------------------
        # FASE 3: GOLD (Integración y Feature Engineering con Lags 1-3)
        # ----------------------------------------------------------------------
        df_features = None
        if run_gold or run_model:
            from src.integration.integrar_datasets import procesar_capa_gold

            logger.info("\n--- [FASE 3: GOLD - INTEGRACIÓN Y FEATURES] ---")
            dim_u, dim_t, fact_v, df_features = procesar_capa_gold(spark=spark, config=cfg)

        # ----------------------------------------------------------------------
        # FASE 4: MODELADO PREDICTIVO (PySpark MLlib vs Baseline)
        # ----------------------------------------------------------------------
        if run_model and df_features is not None:
            from src.modeling.train import entrenar_modelo_predictivo
            from src.modeling.predict import generar_predicciones, guardar_predicciones

            logger.info("\n--- [FASE 4: MODELADO PREDICTIVO MLLIB] ---")
            modelo, reporte = entrenar_modelo_predictivo(df_features, config=cfg)
            preds = generar_predicciones(modelo, df_features)
            guardar_predicciones(preds, config=cfg)

        # ----------------------------------------------------------------------
        # FASE 5: SERVING (Carga a Cloud SQL / PostgreSQL)
        # ----------------------------------------------------------------------
        if run_serving:
            from src.serving.cargar_postgres import crear_tablas_postgres, cargar_capa_gold_a_postgres

            logger.info("\n--- [FASE 5: SERVING - CLOUD SQL / POSTGRESQL] ---")
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
    parser = argparse.ArgumentParser(description="Orquestador del Sistema Predictivo Big Data (Google Cloud / Local).")
    parser.add_argument("--all", action="store_true", help="Ejecutar todas las etapas de inicio a fin.")
    parser.add_argument("--ingest", action="store_true", help="Ejecutar solo ingesta Bronze.")
    parser.add_argument("--silver", action="store_true", help="Ejecutar solo limpieza Silver.")
    parser.add_argument("--gold", action="store_true", help="Ejecutar solo integración Gold.")
    parser.add_argument("--model", action="store_true", help="Ejecutar solo entrenamiento e inferencia ML.")
    parser.add_argument("--serving", action="store_true", help="Ejecutar solo carga a Cloud SQL / PostgreSQL.")
    parser.add_argument("--force", action="store_true", help="Forzar reingesta de archivos omitiendo SHA-256.")
    parser.add_argument("--dataproc", action="store_true", help="Lanzar las fases de procesamiento Spark al clúster de Dataproc.")
    args = parser.parse_args()

    if not (args.ingest or args.silver or args.gold or args.model or args.serving):
        exito = ejecutar_pipeline_completo(force_ingest=args.force, submit_to_dataproc=args.dataproc)
    else:
        exito = ejecutar_pipeline_completo(
            run_ingestion=args.ingest or args.all,
            run_silver=args.silver or args.all,
            run_gold=args.gold or args.all,
            run_model=args.model or args.all,
            run_serving=args.serving or args.all,
            force_ingest=args.force,
            submit_to_dataproc=args.dataproc,
        )

    sys.exit(0 if exito else 1)


if __name__ == "__main__":
    main()
