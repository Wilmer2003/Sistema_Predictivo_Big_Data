"""
Módulo de Serving y Carga a Amazon RDS PostgreSQL / Local.
Ejecuta el script DDL (schema.sql) y copia las tablas Gold (dim_ubigeo, dim_tiempo,
fact_vigilancia_clima y predicciones) hacia la base de datos relacional para Power BI.
"""

import argparse
from importlib.resources import files
from pathlib import Path
from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from src.common.config import get_config, AppConfig
from src.common.postgres import connect_postgres as _conectar
from src.common.postgres import postgres_cursor
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session

logger = get_logger("Serving_Postgres")

CLAVES_SERVING = {
    "dim_ubigeo": ("ubigeo",),
    "dim_tiempo": ("tiempo_id",),
    "fact_vigilancia_clima": ("ubigeo", "tiempo_id"),
    "predicciones_vigilancia": ("ubigeo", "tiempo_id"),
}


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
        _conectar(pg).close()
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
    try:
        from psycopg2 import sql

        ddl_sql = files("src.serving").joinpath("schema.sql").read_text(encoding="utf-8")
        with postgres_cursor(_conectar(pg)) as cur:
            cur.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(pg.schema)))
            cur.execute(ddl_sql)
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
    """Inserta filas por lotes en una transacción y conserva las claves existentes."""
    cfg = config or get_config()
    pg = cfg.postgres

    try:
        from psycopg2 import sql
        from psycopg2.extras import execute_values

        if modo != "append":
            raise ValueError("Serving admite únicamente el modo append idempotente.")
        claves = CLAVES_SERVING[nombre_tabla]
        consulta = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING").format(
            sql.Identifier(pg.schema, nombre_tabla),
            sql.SQL(", ").join(map(sql.Identifier, df.columns)),
            sql.SQL(", ").join(map(sql.Identifier, claves)),
        )
        with postgres_cursor(_conectar(pg)) as cur:
            execute_values(cur, consulta, df.toLocalIterator(), page_size=1000)
        logger.info("Tabla '%s' cargada correctamente en PostgreSQL.", nombre_tabla)
        return True
    except Exception:
        logger.exception("Error al escribir tabla '%s' en PostgreSQL.", nombre_tabla)
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
    4. predicciones_vigilancia
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

    preds_path = cfg.get_predictions_path()
    if cfg.is_aws or Path(preds_path).exists():
        try:
            if not resultados["dim_tiempo"] or not resultados["dim_ubigeo"]:
                raise ValueError("La carga de predicciones requiere ambas dimensiones.")
            df_preds = active_spark.read.parquet(preds_path)

            # Obtener la dimensión temporal desde Gold
            df_tiempo = active_spark.read.parquet(
                f"{gold_dir}/dim_tiempo"
            ).select(
                "tiempo_id",
                "ano",
                "semana_epidemiologica"
            )

            # Verificar cantidad original de predicciones
            total_original = df_preds.count()

            # Incorporar tiempo_id mediante la clave temporal
            df_preds = df_preds.join(
                df_tiempo,
                on=["ano", "semana_epidemiologica"],
                how="left"
            )

            # Validar que todas las predicciones tengan tiempo_id
            if df_preds.filter("tiempo_id IS NULL").count() > 0:
                raise ValueError(
                    "Existen predicciones sin correspondencia temporal."
                )

            # Evitar multiplicación de registros durante el JOIN
            if df_preds.count() != total_original:
                raise ValueError(
                    "El JOIN temporal modificó la cantidad de predicciones."
                )

            logger.info(
                "Predicciones validadas para PostgreSQL: %d",
                total_original
            )

            exito = cargar_dataframe_a_postgres(
                df_preds,
                "predicciones_vigilancia",
                modo=modo,
                config=cfg
            )

            resultados["predicciones_vigilancia"] = exito

        except Exception as e:
            logger.warning(
                "No se pudo cargar tabla de predicciones: %s", e
            )
            resultados["predicciones_vigilancia"] = False
    else:
        logger.info("No se encontraron predicciones locales en: %s", preds_path)

    return resultados


def main():
    parser = argparse.ArgumentParser(description="Módulo de Serving hacia Amazon RDS PostgreSQL / Local.")
    parser.add_argument("--test", action="store_true", help="Probar conectividad con PostgreSQL.")
    parser.add_argument("--crear-tablas", action="store_true", help="Ejecutar schema.sql para crear tablas.")
    parser.add_argument("--cargar", action="store_true", help="Cargar tablas Gold a PostgreSQL.")
    args = parser.parse_args()

    cfg = get_config()

    if args.test:
        return 0 if probar_conexion_postgres(cfg) else 1
    elif args.crear_tablas:
        return 0 if crear_tablas_postgres(cfg) else 1
    elif args.cargar:
        try:
            resultados = cargar_capa_gold_a_postgres(config=cfg)
            return 0 if all(resultados.values()) else 1
        except Exception:
            logger.exception("No se pudo iniciar la carga de Serving.")
            return 1
    else:
        parser.print_help()


if __name__ == "__main__":
    raise SystemExit(main())
