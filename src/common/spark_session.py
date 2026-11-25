"""
Fábrica y Gestor de SparkSession para PySpark 3.5 (Amazon EMR / Local).
Configura parámetros de rendimiento, soporte S3 para Amazon Web Services y PostgreSQL JDBC para Amazon RDS.
"""

import os
from pathlib import Path
import sys
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
    - Soporte S3 (s3:// y s3a://) si el entorno es 'aws' (Amazon EMR / S3).
    - Paquete JDBC de PostgreSQL para la capa de Serving (Amazon RDS).

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
    master = str((extra_configs or {}).get("spark.master", cfg.spark.master))
    local_windows = sys.platform == "win32" and (
        master == "local" or master.startswith("local[")
    )

    if local_windows:
        # SparkContext lee este valor al crearse, antes de lanzar los workers.
        if not os.environ.get("PYSPARK_PYTHON"):
            os.environ["PYSPARK_PYTHON"] = sys.executable

        hadoop_home = os.environ.get("HADOOP_HOME")
        if hadoop_home:
            os.environ["HADOOP_HOME"] = str(
                (cfg.root_dir / Path(hadoop_home)).resolve()
            )
        elif (cfg.root_dir / "hadoop" / "bin" / "winutils.exe").is_file():
            os.environ["HADOOP_HOME"] = str(cfg.root_dir / "hadoop")

    logger.info("Inicializando SparkSession: '%s' (Entorno: %s)", final_app_name, cfg.env)

    builder = (
        SparkSession.builder
        .appName(final_app_name)
        .master(master)
        .config("spark.driver.memory", cfg.spark.driver_memory)
        .config("spark.executor.memory", cfg.spark.executor_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.spark.shuffle_partitions))
        .config("spark.sql.session.timeZone", "America/Lima")
        .config("spark.sql.constraintPropagation.enabled", "false")
    )

    if local_windows:
        builder = (
            builder
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )

    # Configuración de paquete PostgreSQL JDBC (Amazon RDS / Local)
    builder = builder.config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )

    # Configuración de almacenamiento en Amazon S3 (s3:// y s3a://)
    if cfg.is_aws:
        logger.info("Configurando soporte Amazon S3 (s3://) para región: %s", cfg.aws_region)
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{cfg.aws_region}.amazonaws.com")
        )

        if cfg.aws_access_key_id and cfg.aws_secret_access_key:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.access.key", cfg.aws_access_key_id)
                .config("spark.hadoop.fs.s3a.secret.key", cfg.aws_secret_access_key)
            )
            if cfg.aws_session_token:
                builder = (
                    builder
                    .config("spark.hadoop.fs.s3a.session.token", cfg.aws_session_token)
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
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
