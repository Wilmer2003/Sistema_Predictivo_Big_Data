"""
Orquestador Principal End-to-End del Pipeline Medallion.
Coordina el flujo completo: Bronze (Ingesta) -> Silver (Limpieza) ->
Gold (Integración & Features) -> Modelo (MLlib) -> Serving (Amazon RDS / PostgreSQL).
Soporta ejecución local y lanzamiento de pasos sobre Amazon EMR (Spark en YARN).
"""

import argparse
import sys
from typing import Optional, List, Dict, Any

from src.common.config import get_config, AppConfig
from src.common.logger import get_logger

logger = get_logger("Orquestador_Pipeline")


def lanzar_step_emr(
    script_s3_uri: str,
    step_args: Optional[List[str]] = None,
    step_name: str = "Pipeline_Predictivo_Step",
    config: Optional[AppConfig] = None,
) -> bool:
    """
    Envía un paso PySpark al clúster de Amazon EMR (1 nodo primario + 2 core en YARN)
    mediante la API de EMR (Boto3 add_job_flow_steps).

    Args:
        script_s3_uri: URI en Amazon S3 (s3://...) o ruta del script ejecutable.
        step_args: Argumentos de línea de comandos para el script.
        step_name: Nombre descriptivo del paso en EMR.
        config: Configuración de la aplicación (AppConfig).

    Returns:
        bool: True si el paso se ejecutó o registró exitosamente en EMR, False en caso de error.
    """
    cfg = config or get_config()
    logger.info("Preparando envío de paso PySpark a Amazon EMR...")
    logger.info("Región: %s | Clúster EMR ID: %s", cfg.aws_region, cfg.emr_cluster_id)

    try:
        import boto3

        emr_client = boto3.client("emr", region_name=cfg.aws_region)

        args = ["spark-submit", "--deploy-mode", "cluster"]
        args.extend([
            "--packages", "org.postgresql:postgresql:42.7.3",
            "--conf", "spark.sql.sources.partitionOverwriteMode=dynamic",
        ])
        args.append(script_s3_uri)
        if step_args:
            args.extend(step_args)

        step_config = {
            "Name": step_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": args,
            },
        }

        logger.info("Enviando paso PySpark al clúster EMR %s...", cfg.emr_cluster_id)
        response = emr_client.add_job_flow_steps(
            JobFlowId=cfg.emr_cluster_id,
            Steps=[step_config],
        )
        step_ids = response.get("StepIds", [])
        if not step_ids:
            logger.error("No se obtuvo StepId de la respuesta de Amazon EMR.")
            return False

        step_id = step_ids[0]
        logger.info("Paso registrado en EMR con StepId: %s. Verificando estado...", step_id)

        desc = emr_client.describe_step(ClusterId=cfg.emr_cluster_id, StepId=step_id)
        state = desc.get("Step", {}).get("Status", {}).get("State", "UNKNOWN")
        logger.info("Estado del paso en EMR: %s", state)
        return state in ("COMPLETED", "PENDING", "RUNNING")

    except Exception as e:
        logger.error("Error al enviar o ejecutar el paso en Amazon EMR: %s", e)
        return False


def ejecutar_pipeline_completo(
    run_ingestion: bool = True,
    run_silver: bool = True,
    run_gold: bool = True,
    run_model: bool = True,
    run_serving: bool = True,
    force_ingest: bool = False,
    submit_to_emr: bool = False,
    config: Optional[AppConfig] = None,
) -> bool:
    """
    Ejecuta el pipeline de extremo a extremo respetando la arquitectura Medallion.
    En modo local ejecuta PySpark localmente.
    En modo AWS desde la laptop, ingesta a Amazon S3 y puede despachar el cómputo hacia Amazon EMR.
    """
    cfg = config or get_config()
    logger.info("==================================================")
    logger.info("  INICIANDO EJECUCIÓN DEL PIPELINE PREDICTIVO")
    logger.info("  Entorno: %s | Modo AWS: %s", cfg.env, cfg.is_aws)
    logger.info("==================================================")

    # --------------------------------------------------------------------------
    # FASE 1: BRONZE (Ingesta cruda con control SHA-256)
    # En AWS sube a Amazon S3 (s3://) y registra en tabla 'lotes' de Amazon RDS
    # --------------------------------------------------------------------------
    if run_ingestion:
        from src.ingestion.cargar_ira import cargar_ira
        from src.ingestion.cargar_malaria import cargar_malaria
        from src.ingestion.cargar_senamhi import cargar_senamhi

        logger.info("\n--- [FASE 1: BRONZE - INGESTA DE LOTES] ---")
        cargar_ira(force=force_ingest)
        cargar_malaria(force=force_ingest)
        cargar_senamhi(force=force_ingest)

    # Si se solicita despachar las fases de procesamiento Spark a Amazon EMR:
    if cfg.is_aws and submit_to_emr:
        logger.info("\n--- [DESPACHANDO COMPUTO A AMAZON EMR CLUSTER] ---")
        script_s3 = f"s3://{cfg.s3_bucket}/scripts/run_pipeline.py"
        args_emr = ["--silver", "--gold", "--model", "--serving"]
        return lanzar_step_emr(script_s3, step_args=args_emr, config=cfg)

    # Si solo se requería la fase de ingesta Bronze, finalizamos exitosamente sin levantar Spark
    if not (run_silver or run_gold or run_model or run_serving):
        logger.info("\n==================================================")
        logger.info("  FASE DE INGESTA BRONZE COMPLETADA EXITOSAMENTE")
        logger.info("==================================================")
        return True

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
        # FASE 5: SERVING (Carga a Amazon RDS / PostgreSQL)
        # ----------------------------------------------------------------------
        if run_serving:
            from src.serving.cargar_postgres import crear_tablas_postgres, cargar_capa_gold_a_postgres

            logger.info("\n--- [FASE 5: SERVING - AMAZON RDS / POSTGRESQL] ---")
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
    parser = argparse.ArgumentParser(description="Orquestador del Sistema Predictivo Big Data (Amazon Web Services / Local).")
    parser.add_argument("--all", action="store_true", help="Ejecutar todas las etapas de inicio a fin.")
    parser.add_argument("--ingest", action="store_true", help="Ejecutar solo ingesta Bronze.")
    parser.add_argument("--silver", action="store_true", help="Ejecutar solo limpieza Silver.")
    parser.add_argument("--gold", action="store_true", help="Ejecutar solo integración Gold.")
    parser.add_argument("--model", action="store_true", help="Ejecutar solo entrenamiento e inferencia ML.")
    parser.add_argument("--serving", action="store_true", help="Ejecutar solo carga a Amazon RDS / PostgreSQL.")
    parser.add_argument("--force", action="store_true", help="Forzar reingesta de archivos omitiendo SHA-256.")
    parser.add_argument("--emr", action="store_true", help="Lanzar las fases de procesamiento Spark al clúster de Amazon EMR.")
    args = parser.parse_args()

    if not (args.ingest or args.silver or args.gold or args.model or args.serving):
        exito = ejecutar_pipeline_completo(force_ingest=args.force, submit_to_emr=args.emr)
    else:
        exito = ejecutar_pipeline_completo(
            run_ingestion=args.ingest or args.all,
            run_silver=args.silver or args.all,
            run_gold=args.gold or args.all,
            run_model=args.model or args.all,
            run_serving=args.serving or args.all,
            force_ingest=args.force,
            submit_to_emr=args.emr,
        )

    sys.exit(0 if exito else 1)


if __name__ == "__main__":
    main()
