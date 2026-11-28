"""Prepara o publica una revisión de malaria sin ejecutar el pipeline IRA."""
import argparse
from collections import Counter
import hashlib
import json
import logging
from pathlib import Path
import re

from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.s3_artifacts import validate_run_id
from src.common.spark_session import get_spark_session
from src.integration.vigilancia_malaria import CLAVE_MALARIA, COLUMNAS_GOLD_MALARIA, agregar_vigilancia_malaria
from src.preprocessing.limpiar_malaria import procesar_silver_malaria

logger = logging.getLogger(__name__)


def filas_gold(df):
    return [tuple(fila) for fila in df.select(*COLUMNAS_GOLD_MALARIA).orderBy(*CLAVE_MALARIA).toLocalIterator()]


def huella_gold(filas):
    digest = hashlib.sha256()
    for fila in filas:
        digest.update(json.dumps(fila, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def perfiles(filas):
    totales = Counter()
    for fila in filas:
        totales[fila[3]] += fila[-1]
    return dict(totales)


def preparar(spark, cfg, args, ruta):
    ficheros = (
        spark.read.format("binaryFile").load(args.bronze)
        .select(F.sha2("content", 256).alias("sha256")).take(2)
    )
    if len(ficheros) != 1 or ficheros[0].sha256 != args.sha256:
        raise ValueError("Bronze debe ser un solo archivo con la huella SHA-256 declarada.")
    silver = f"{ruta}/silver"
    procesar_silver_malaria(
        spark, args.bronze, silver, cfg, snapshot_versionado=True
    )
    gold = agregar_vigilancia_malaria(spark.read.parquet(silver))
    gold.write.mode("errorifexists").parquet(f"{ruta}/gold")
    filas = filas_gold(spark.read.parquet(f"{ruta}/gold"))
    esperado = {"B50": args.b50, "B51": args.b51}
    if perfiles(filas) != {k: v for k, v in esperado.items() if v}:
        raise ValueError("Gold no coincide con los conteos autorizados por especie.")
    manifiesto = {
        "run_id": args.run_id, "bronze": args.bronze, "sha256_bronze": args.sha256,
        "uri_gold": f"{ruta}/gold", "sha256_gold": huella_gold(filas),
        "filas_gold": len(filas), "b50": args.b50, "b51": args.b51,
        "unidad": "registros_fuente",
    }
    spark.createDataFrame([manifiesto]).coalesce(1).write.mode("errorifexists").json(f"{ruta}/manifest")
    logger.info("Revisión %s preparada. Registros: %s", args.run_id, esperado)


def servir(spark, cfg, args, ruta):
    from src.serving.publicar_malaria import publicar_malaria

    lectura = Path(args.artefactos_locales).resolve().as_posix() if args.artefactos_locales else ruta
    manifiestos = spark.read.json(f"{lectura}/manifest").take(2)
    if len(manifiestos) != 1:
        raise ValueError("Se requiere un manifiesto Gold único.")
    m = manifiestos[0]
    if (m.run_id, m.unidad, m.uri_gold, m.sha256_bronze, m.b50, m.b51) != (
        args.run_id, "registros_fuente", f"{ruta}/gold", args.sha256, args.b50, args.b51
    ):
        raise ValueError("El manifiesto no coincide con la versión, unidad o controles autorizados.")
    filas = filas_gold(spark.read.parquet(f"{lectura}/gold"))
    esperado = {"B50": args.b50, "B51": args.b51}
    if (len(filas) != m.filas_gold or huella_gold(filas) != m.sha256_gold
            or perfiles(filas) != {k: v for k, v in esperado.items() if v}):
        raise ValueError("Gold difiere del manifiesto o del conteo autorizado por especie.")
    cambio = publicar_malaria(
        cfg.postgres, run_id=args.run_id, sha256=args.sha256, uri_gold=m.uri_gold,
        filas=filas, activa_esperada=None if args.activa_esperada == "ninguna" else args.activa_esperada,
        registros_esperados=args.b50 + args.b51,
    )
    logger.info("Revisión %s. Publicación nueva: %s", args.run_id, cambio)


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("etapa", choices=("preparar", "servir"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--b50", required=True, type=int)
    parser.add_argument("--b51", required=True, type=int)
    parser.add_argument("--bronze")
    parser.add_argument("--activa-esperada", help="Run actualmente activo o 'ninguna'.")
    parser.add_argument("--artefactos-locales", help="Copia local de gold/ y manifest/.")
    args = parser.parse_args()
    validate_run_id(args.run_id)
    if (not re.fullmatch(r"[a-f0-9]{64}", args.sha256)
            or min(args.b50, args.b51) < 0 or args.b50 + args.b51 <= 0):
        parser.error("Se requiere SHA-256 y conteos por especie no negativos, con total positivo.")
    if args.etapa == "preparar" and not args.bronze:
        parser.error("preparar requiere --bronze")
    if args.etapa == "servir" and not args.activa_esperada:
        parser.error("servir requiere --activa-esperada")
    if args.activa_esperada and args.activa_esperada != "ninguna":
        validate_run_id(args.activa_esperada)
    cfg = get_config()
    ruta = f"{cfg.get_gold_path('vigilancia_malaria')}/runs/{args.run_id}"
    extra = {"spark.jars.packages": ""}
    spark = get_spark_session("Vigilancia_Malaria", config=cfg, extra_configs=extra)
    try:
        (preparar if args.etapa == "preparar" else servir)(spark, cfg, args, ruta)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
