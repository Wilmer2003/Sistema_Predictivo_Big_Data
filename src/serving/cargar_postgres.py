"""
Módulo de Serving y Carga a Cloud SQL para PostgreSQL / Local.
Ejecuta el script DDL (schema.sql) y copia las tablas Gold (dim_ubigeo, dim_tiempo,
fact_vigilancia_clima y predicciones) hacia la base de datos relacional para Power BI.
"""

import argparse
from pathlib import Path
from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from src.common.config import get_config, AppConfig, PostgresConfig
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session

logger = get_logger("Serving_Postgres")


def probar_conexion_postgres(config: Optional[AppConfig] = None) -> bool:
    """
    Verifica la conectividad hacia el servidor PostgreSQL configurado en .env.

    Returns:
        bool: True si la conexión fue exitosa, False en caso de error.
    """
    cfg = config or get_config()
    pg = cfg.postgres
    logger.info("Probando conectividad hacia PostgreSQL en %s:%d/%s...", pg.host, pg.port, pg.db)

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=pg.host,
            port=pg.port,
            dbname=pg.db,
            user=pg.user,
            password=pg.password,
            connect_timeout=3,
        )
        conn.close()
        logger.info("Conexión a PostgreSQL establecida exitosamente.")
        return True
    except ImportError:
        logger.warning("psycopg2 no está instalado en el entorno actual para prueba directa.")
        return False
    except Exception as e:
        logger.warning("No se pudo conectar a PostgreSQL (%s:%d): %s", pg.host, pg.port, e)
        return False


def crear_tablas_postgres(config: Optional[AppConfig] = None) -> bool:
    """
    Ejecuta el archivo schema.sql en PostgreSQL para crear el modelo dimensional.

    Returns:
        bool: True si las tablas se crearon con éxito, False en caso contrario.
    """
    cfg = config or get_config()
    pg = cfg.postgres
    schema_path = Path(__file__).parent / "schema.sql"

    if not schema_path.is_file():
        logger.error("Archivo DDL no encontrado: %s", schema_path)
        return False

    with open(schema_path, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=pg.host,
            port=pg.port,
            dbname=pg.db,
            user=pg.user,
            password=pg.password,
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(ddl_sql)
        conn.commit()
        conn.close()
        logger.info("Tablas del esquema dimensional creadas exitosamente en PostgreSQL.")
        return True
    except ImportError:
        logger.warning("psycopg2 no disponible para ejecutar schema.sql directamente.")
        return False
    except Exception as e:
        logger.error("Error al crear tablas en PostgreSQL: %s", e)
        return False


def cargar_dataframe_a_postgres(
    df: "DataFrame",
    nombre_tabla: str,
    modo: str = "append",
    config: Optional[AppConfig] = None,
) -> bool:
    """
    Carga un DataFrame de PySpark hacia una tabla de PostgreSQL mediante JDBC.

    Args:
        df: DataFrame de PySpark a escribir.
        nombre_tabla: Nombre de la tabla destino en PostgreSQL.
        modo: 'append' o 'overwrite'.
        config: Configuración de la aplicación.

    Returns:
        bool: True si la escritura fue exitosa, False en caso contrario.
    """
    cfg = config or get_config()
    pg = cfg.postgres

    logger.info("Escribiendo datos en PostgreSQL (Tabla: %s, Modo: %s)...", nombre_tabla, modo)
    try:
        (
            df.write
            .format("jdbc")
            .option("url", pg.jdbc_url)
            .option("dbtable", nombre_tabla)
            .option("user", pg.user)
            .option("password", pg.password)
            .option("driver", "org.postgresql.Driver")
            .mode(modo)
            .save()
        )
        logger.info("Tabla '%s' cargada correctamente en PostgreSQL.", nombre_tabla)
        return True
    except Exception as e:
        logger.error("Error al escribir tabla '%s' vía JDBC: %s", nombre_tabla, e)
        return False


def cargar_capa_gold_a_postgres(
    spark: Optional["SparkSession"] = None,
    config: Optional[AppConfig] = None,
    modo: str = "append",
) -> Dict[str, bool]:
    """
    Lee las tablas generadas en Gold (Parquet) y las transfiere a PostgreSQL.

    Tablas:
    1. dim_ubigeo
    2. dim_tiempo
    3. fact_vigilancia_clima
    4. predicciones_vigilancia (si existen en results/estadisticas)
    """
    cfg = config or get_config()
    active_spark = spark or get_spark_session(app_name="Serving_Carga_Postgres", config=cfg)
    gold_dir = cfg.get_gold_path("epidemiologia_clima")

    resultados = {}

    tablas = [
        ("dim_ubigeo", f"{gold_dir}/dim_ubigeo"),
        ("dim_tiempo", f"{gold_dir}/dim_tiempo"),
        ("fact_vigilancia_clima", f"{gold_dir}/fact_vigilancia"),
    ]

    for tabla_pg, ruta_parquet in tablas:
        try:
            logger.info("Cargando %s desde %s...", tabla_pg, ruta_parquet)
            df = active_spark.read.parquet(ruta_parquet)
            exito = cargar_dataframe_a_postgres(df, tabla_pg, modo=modo, config=cfg)
            resultados[tabla_pg] = exito
        except Exception as e:
            logger.warning("No se pudo cargar tabla '%s': %s", tabla_pg, e)
            resultados[tabla_pg] = False

    # Cargar predicciones si existen
    preds_path = str(cfg.root_dir / "results" / "estadisticas" / "predicciones_vigilancia").replace("\\", "/")
    if Path(preds_path).exists():
        try:
            df_preds = active_spark.read.parquet(preds_path)
            exito = cargar_dataframe_a_postgres(df_preds, "predicciones_vigilancia", modo=modo, config=cfg)
            resultados["predicciones_vigilancia"] = exito
        except Exception as e:
            logger.warning("No se pudo cargar tabla de predicciones: %s", e)
            resultados["predicciones_vigilancia"] = False

    return resultados


def main():
    parser = argparse.ArgumentParser(description="Módulo de Serving hacia Cloud SQL para PostgreSQL / Local.")
    parser.add_argument("--test", action="store_true", help="Probar conectividad con PostgreSQL.")
    parser.add_argument("--crear-tablas", action="store_true", help="Ejecutar schema.sql para crear tablas.")
    parser.add_argument("--cargar", action="store_true", help="Cargar tablas Gold a PostgreSQL.")
    args = parser.parse_args()

    cfg = get_config()

    if args.test:
        probar_conexion_postgres(cfg)
    elif args.crear-tablas:
        crear_tablas_postgres(cfg)
    elif args.cargar:
        cargar_capa_gold_a_postgres(config=cfg)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
