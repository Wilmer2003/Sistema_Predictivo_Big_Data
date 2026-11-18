"""
Fábrica y Gestor de SparkSession para PySpark 3.5.3 (Dataproc 2.2 / Local).
Configura parámetros de rendimiento, soporte GCS para Google Cloud y PostgreSQL JDBC para Cloud SQL.
"""

from typing import Optional, Dict, Any, TYPE_CHECKING
from src.common.config import get_config, AppConfig
from src.common.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


def get_spark_session(
    app_name: Optional[str] = None,
    config: Optional[AppConfig] = None,
    extra_configs: Optional[Dict[str, Any]] = None,
) -> "SparkSession":
    """
    Crea o recupera una SparkSession optimizada para el pipeline.

    Aplica configuraciones para:
    - Timezone 'America/Lima' (consistencia epidemiológica en Perú).
    - Particionamiento de shuffle según entorno.
    - Conector Hadoop GCS (gs://) si el entorno es 'gcp' (Dataproc / Cloud Storage).
    - Paquete JDBC de PostgreSQL para la capa de Serving (Cloud SQL).

    Args:
        app_name: Nombre de la aplicación Spark (opcional).
        config: Objeto AppConfig (si no se proporciona se invoca get_config()).
        extra_configs: Diccionario de configuraciones adicionales clave-valor.

    Returns:
        SparkSession: Sesión activa de Apache Spark.
    """
    from pyspark.sql import SparkSession

    cfg = config or get_config()
    final_app_name = app_name or cfg.spark.app_name

    logger.info("Inicializando SparkSession: '%s' (Entorno: %s)", final_app_name, cfg.env)

    builder = (
        SparkSession.builder
        .appName(final_app_name)
        .master(cfg.spark.master)
        .config("spark.driver.memory", cfg.spark.driver_memory)
        .config("spark.executor.memory", cfg.spark.executor_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.spark.shuffle_partitions))
        .config("spark.sql.session.timeZone", "America/Lima")
        .config("spark.sql.constraintPropagation.enabled", "false")
    )

    # Configuración de paquete PostgreSQL JDBC (Cloud SQL / Local)
    builder = builder.config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )

    # Configuración de almacenamiento en Google Cloud Storage (GCS vía gs://)
    if cfg.is_gcp:
        logger.info("Configurando soporte Google Cloud Storage (gs://) para el proyecto: %s", cfg.gcp_project_id)
        builder = (
            builder
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        )

        if cfg.gcp_project_id:
            builder = builder.config("spark.hadoop.fs.gs.project.id", cfg.gcp_project_id)

        if cfg.google_application_credentials:
            builder = (
                builder
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", cfg.google_application_credentials)
            )

    # Aplicar configuraciones adicionales pasadas explícitamente
    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, str(v))

    spark = builder.getOrCreate()

    # Reducir verbosidad de logs de Spark según la configuración
    spark.sparkContext.setLogLevel(cfg.spark.log_level)

    logger.info("SparkSession inicializada exitosamente. Versión Spark: %s", spark.version)
    return spark


def stop_spark_session(spark: Optional["SparkSession"] = None) -> None:
    """Detiene limpiamente la sesión activa de Spark."""
    try:
        from pyspark.sql import SparkSession
        active_spark = spark or SparkSession.getActiveSession()
        if active_spark:
            logger.info("Deteniendo SparkSession activa...")
            active_spark.stop()
            logger.info("SparkSession detenida correctamente.")
    except ImportError:
        pass
