"""
Módulo de Configuración Centralizada (Local y Amazon Web Services - AWS).
Lee variables desde entorno o archivo .env y expone rutas, credenciales
y constantes de dominio para todas las capas del pipeline Medallion.
"""

from dataclasses import dataclass, field
import os
from uuid import uuid4
from pathlib import Path
from typing import Optional

# Cargar automáticamente variables de entorno desde .env
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_ENV_FILE = Path(os.getenv("PIPELINE_CONFIG_FILE") or _REPO_ROOT / ".env")

try:
    from dotenv import load_dotenv
    load_dotenv(_ENV_FILE)
except ImportError:
    # Fallback seguro en entornos mínimos donde python-dotenv aún no fue instalado
    _env_file = _ENV_FILE
    if _env_file.exists():
        try:
            with open(_env_file, "r", encoding="utf-8") as _f:
                for _line in _f:
                    _line = _line.strip()
                    if _line and not _line.startswith("#") and "=" in _line:
                        _k, _v = _line.split("=", 1)
                        _v = _v.split("#")[0].strip()
                        os.environ.setdefault(_k.strip(), _v)
        except Exception:
            pass


# ==============================================================================
# CONSTANTES DE DOMINIO EPIDEMIOLÓGICO Y GEOGRÁFICO
# ==============================================================================
CODIGO_DEPARTAMENTO_LA_LIBERTAD = "13"
NOMBRE_DEPARTAMENTO_LA_LIBERTAD = "LA LIBERTAD"
ANIO_INICIAL = 2015
ANIO_FINAL = 2024
LONGITUD_UBIGEO = 6

# Diagnósticos CIE-10 para Malaria
DIAGNOSTICOS_MALARIA = ("B50", "B51")

# Variables meteorológicas clave de SENAMHI
VARIABLES_CLIMA = (
    "Precipitacion_acumulada",
    "Temperatura_maxima",
    "Temperatura_minima",
    "tc",
)


@dataclass(frozen=True)
class PostgresConfig:
    """Configuración de conexión para PostgreSQL / Amazon RDS PostgreSQL."""
    host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    db: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "epidemiologia_db"))
    user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", ""))
    schema: str = field(default_factory=lambda: os.getenv("POSTGRES_SCHEMA", "public"))

    sslmode: Optional[str] = field(default_factory=lambda: os.getenv("POSTGRES_SSLMODE"))
    sslrootcert: Optional[str] = field(default_factory=lambda: os.getenv("POSTGRES_SSLROOTCERT"))

    @property
    def tls_options(self) -> dict:
        return {
            key: value
            for key, value in (("sslmode", self.sslmode), ("sslrootcert", self.sslrootcert))
            if value
        }

    @property
    def jdbc_url(self) -> str:
        """Retorna la URL JDBC compatible con Spark."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    @property
    def connection_properties(self) -> dict:
        """Propiedades de conexión para spark.read.jdbc y df.write.jdbc."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
            **self.tls_options,
        }


@dataclass(frozen=True)
class SparkSettings:
    """Parámetros de configuración de PySpark (compatible con Amazon EMR / Spark 3.5.3 / Local)."""
    app_name: str = field(default_factory=lambda: os.getenv("SPARK_APP_NAME", "Sistema_Predictivo_La_Libertad"))
    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    driver_memory: str = field(default_factory=lambda: os.getenv("SPARK_DRIVER_MEMORY", "4g"))
    executor_memory: str = field(default_factory=lambda: os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
    shuffle_partitions: int = field(default_factory=lambda: int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")))
    log_level: str = field(default_factory=lambda: os.getenv("SPARK_LOG_LEVEL", "WARN"))


@dataclass
class AppConfig:
    """
    Gestor central de rutas y configuraciones del pipeline.
    Soporta ejecución local (filesystem) y Amazon Web Services (S3 s3:// + EMR + Amazon RDS).
    """
    env: str = field(default_factory=lambda: os.getenv("PIPELINE_ENV", "local").lower())
    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    aws_access_key_id: Optional[str] = field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key: Optional[str] = field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"))
    aws_session_token: Optional[str] = field(default_factory=lambda: os.getenv("AWS_SESSION_TOKEN"))
    s3_bucket: str = field(default_factory=lambda: os.getenv("S3_BUCKET", ""))
    emr_cluster_id: str = field(default_factory=lambda: os.getenv("EMR_CLUSTER_ID", ""))
    data_dir_override: Optional[str] = field(default_factory=lambda: os.getenv("DATA_DIR"))

    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    spark: SparkSettings = field(default_factory=SparkSettings)
    run_id: str = field(default_factory=lambda: os.getenv("PIPELINE_RUN_ID") or uuid4().hex)

    @property
    def is_aws(self) -> bool:
        """Indica si el pipeline se ejecuta contra Amazon Web Services (S3 + EMR + RDS)."""
        return self.env == "aws"

    @property
    def root_dir(self) -> Path:
        """Ruta raíz del repositorio."""
        return _REPO_ROOT

    # --------------------------------------------------------------------------
    # Rutas Base Medallion
    # --------------------------------------------------------------------------
    @property
    def raw_base_path(self) -> str:
        """Ruta base para la capa Bronze."""
        if self.is_aws:
            if not self.s3_bucket:
                raise ValueError("S3_BUCKET debe estar configurado cuando PIPELINE_ENV='aws'")
            return f"s3://{self.s3_bucket}/bronze"
        base = Path(self.data_dir_override) if self.data_dir_override else self.root_dir / "data"
        return str((base / "raw").resolve()).replace("\\", "/")

    @property
    def processed_base_path(self) -> str:
        """Ruta base para la capa Silver."""
        if self.is_aws:
            if not self.s3_bucket:
                raise ValueError("S3_BUCKET debe estar configurado cuando PIPELINE_ENV='aws'")
            return f"s3://{self.s3_bucket}/silver"
        base = Path(self.data_dir_override) if self.data_dir_override else self.root_dir / "data"
        return str((base / "processed").resolve()).replace("\\", "/")

    @property
    def curated_base_path(self) -> str:
        """Ruta base para la capa Gold."""
        if self.is_aws:
            if not self.s3_bucket:
                raise ValueError("S3_BUCKET debe estar configurado cuando PIPELINE_ENV='aws'")
            return f"s3://{self.s3_bucket}/gold"
        base = Path(self.data_dir_override) if self.data_dir_override else self.root_dir / "data"
        return str((base / "curated").resolve()).replace("\\", "/")

    @property
    def models_base_path(self) -> str:
        """Ruta base para modelos MLlib."""
        if self.is_aws:
            if not self.s3_bucket:
                raise ValueError("S3_BUCKET debe estar configurado cuando PIPELINE_ENV='aws'")
            return f"s3://{self.s3_bucket}/modelos"
        return str((self.root_dir / "results" / "modelos").resolve()).replace("\\", "/")

    @property
    def batch_control_path(self) -> str:
        """Ruta del registro de control de lotes local (en AWS el estado reside en la tabla 'lotes' de Amazon RDS)."""
        base = Path(self.data_dir_override) if self.data_dir_override else self.root_dir / "data"
        control_dir = base / "control"
        control_dir.mkdir(parents=True, exist_ok=True)
        return str((control_dir / "batch_control.json").resolve()).replace("\\", "/")

    # --------------------------------------------------------------------------
    # Rutas Específicas por Fuente y Capa
    # --------------------------------------------------------------------------
    def get_bronze_path(self, source: str) -> str:
        """Retorna la ruta Bronze para 'ira', 'malaria' o 'senamhi'."""
        source = source.lower()
        return f"{self.raw_base_path}/{source}"

    def get_silver_path(self, source: str) -> str:
        """Retorna la ruta Silver para 'ira', 'malaria' o 'senamhi'."""
        source = source.lower()
        return f"{self.processed_base_path}/{source}"

    def get_gold_path(self, dataset_name: str = "epidemiologia_clima") -> str:
        """Retorna la ruta Gold para el dataset integrado de analítica/features."""
        return f"{self.curated_base_path}/{dataset_name}"

    def get_model_path(self, model_name: str = "rf_ira_model") -> str:
        """Retorna la ruta del modelo MLlib (en Amazon S3 s3:// o local results/modelos/)."""
        return f"{self.models_base_path}/{model_name}"


# Instancia singleton predeterminada
_config_instance: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Retorna la instancia global de configuración."""
    global _config_instance
    if _config_instance is None:
        _config_instance = AppConfig()
    return _config_instance
