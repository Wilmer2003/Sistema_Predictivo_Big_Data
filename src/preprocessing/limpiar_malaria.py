"""Silver de malaria para extractos de detalle, con linaje y controles de calidad."""
import argparse
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.config import (
    ANIO_FINAL,
    ANIO_INICIAL,
    CODIGO_DEPARTAMENTO_LA_LIBERTAD,
    DIAGNOSTICOS_MALARIA,
    LONGITUD_UBIGEO,
    NOMBRE_DEPARTAMENTO_LA_LIBERTAD,
    AppConfig,
    get_config,
)
from src.common.geo_utils import with_standardized_ubigeo
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.ingestion.batch_manager import BatchControlManager
from src.preprocessing.base_preprocessor import normalize_column_names, trim_text_columns

logger = get_logger("Limpieza_Malaria")

FORMATO_DETALLE = "detalle_v1"
ESPECIES_POR_ENFERMEDAD = {
    "MALARIA POR P. VIVAX": "B51",
    "MALARIA P. VIVAX": "B51",
    "MALARIA P. FALCIPARUM": "B50",
    "MALARIA POR P. FALCIPARUM": "B50",
}
CAMPOS_TEXTO = (
    "departamento", "provincia", "distrito", "localidad", "enfermedad",
    "diagnostic", "diresa", "tipo_edad", "sexo", "edad_anios",
)
CAMPOS_REQUERIDOS = {"ubigeo", "ano", "semana", "enfermedad"}
CAMPOS_ENTRADA = CAMPOS_REQUERIDOS | set(CAMPOS_TEXTO) | {"localcod", "edad"}
CAMPOS_AGREGADOS = {"malaria_total", "b50", "b51"}
CLAVE_COINCIDENCIA = ("ubigeo", "ano", "semana", "localidad", "sexo", "edad")


def transformar_datos_malaria(
    df: DataFrame, *, archivo_origen: Optional[str] = None
) -> DataFrame:
    """Valida un extracto de detalle y conserva todas sus ocurrencias elegibles."""
    if len({c.lower() for c in df.columns}) != len(df.columns):
        raise ValueError("Malaria Silver: encabezados duplicados.")

    df = normalize_column_names(df)
    nombres = [c.lower() for c in df.columns]
    if len(set(nombres)) != len(nombres):
        raise ValueError("Malaria Silver: encabezados en conflicto tras normalizar.")
    df = df.toDF(*nombres)
    columnas = set(nombres)
    if columnas & CAMPOS_AGREGADOS:
        raise ValueError("Malaria Silver: formato agregado no acreditado; lote rechazado.")
    if CAMPOS_REQUERIDOS - columnas or columnas - CAMPOS_ENTRADA:
        raise ValueError(
            "Malaria Silver: esquema incompatible. "
            f"Faltantes={sorted(CAMPOS_REQUERIDOS - columnas)}; "
            f"no admitidas={sorted(columnas - CAMPOS_ENTRADA)}."
        )

    for campo in sorted(CAMPOS_ENTRADA - columnas):
        df = df.withColumn(campo, F.lit(None).cast("string"))
    df = trim_text_columns(df, list(CAMPOS_TEXTO))
    for campo in ("ano", "semana", "edad"):
        df = df.withColumn(f"_{campo}_entero", F.expr(f"try_cast({campo} AS INT)"))

    ubigeo = F.regexp_replace(F.trim(F.col("ubigeo").cast("string")), r"\.0+$", "")
    localcod = F.regexp_replace(F.trim(F.col("localcod").cast("string")), r"\.0+$", "")
    ubigeo_valido = F.coalesce(ubigeo.rlike(rf"^[0-9]{{1,{LONGITUD_UBIGEO}}}$"), F.lit(False))
    departamento = F.upper(F.col("departamento"))
    interes_geografico = (
        ubigeo.startswith(CODIGO_DEPARTAMENTO_LA_LIBERTAD)
        | (departamento == NOMBRE_DEPARTAMENTO_LA_LIBERTAD)
        | ~ubigeo_valido
    )

    catalogo = F.create_map(*(F.lit(v) for par in ESPECIES_POR_ENFERMEDAD.items() for v in par))
    df = df.withColumn("especie_codigo", catalogo[F.upper(F.col("enfermedad"))])
    diagnostico = F.upper(F.col("diagnostic"))
    edad_invalida = (F.trim(F.col("edad").cast("string")) != "") & (
        F.col("_edad_entero").isNull() | (F.col("_edad_entero") < 0)
    )
    fuera_de_periodo = F.col("_ano_entero").isNotNull() & ~F.col("_ano_entero").between(
        ANIO_INICIAL, ANIO_FINAL
    )
    df = df.filter(interes_geografico & ~fuera_de_periodo)
    motivo = (
        F.when(~ubigeo_valido, "ubigeo_invalido")
        .when(
            ~ubigeo.startswith(CODIGO_DEPARTAMENTO_LA_LIBERTAD)
            | (
                (F.coalesce(departamento, F.lit("")) != "")
                & (departamento != NOMBRE_DEPARTAMENTO_LA_LIBERTAD)
            ),
            "departamento_ubigeo_en_conflicto",
        )
        .when(F.col("_ano_entero").isNull(), "ano_invalido")
        .when(
            F.col("_semana_entero").isNull() | ~F.col("_semana_entero").between(1, 53),
            "semana_invalida",
        )
        .when(edad_invalida, "edad_invalida")
        .when((localcod != "") & ~localcod.rlike(r"^[A-Za-z0-9]{1,10}$"), "localcod_invalido")
        .when(F.col("especie_codigo").isNull(), "especie_desconocida")
        .when(
            (diagnostico != "")
            & (~diagnostico.isin(*DIAGNOSTICOS_MALARIA) | (diagnostico != F.col("especie_codigo"))),
            "diagnostico_en_conflicto",
        )
    )
    errores = (
        df.select(motivo.alias("motivo")).where("motivo IS NOT NULL")
        .groupBy("motivo").count().orderBy("motivo").collect()
    )
    if errores:
        resumen = ", ".join(f"{r.motivo}={r['count']}" for r in errores)
        raise ValueError(f"Malaria Silver: lote rechazado por calidad ({resumen}).")

    df = with_standardized_ubigeo(df, "ubigeo", "ubigeo")
    df = df.withColumn("localcod", F.when(localcod != "", F.lpad(localcod, 10, "0")))
    for campo in ("ano", "semana", "edad"):
        df = df.withColumn(campo, F.col(f"_{campo}_entero")).drop(f"_{campo}_entero")
    ventana = Window.partitionBy(*CLAVE_COINCIDENCIA)
    return (
        df.withColumn("archivo_origen", F.lit(archivo_origen).cast("string"))
        .withColumn("formato_origen", F.lit(FORMATO_DETALLE))
        .withColumn("registros_fuente", F.lit(1).cast("long"))
        .withColumn("ocurrencias_clave_coincidente", F.count(F.lit(1)).over(ventana))
        .select(
            "ubigeo", "ano", "semana", "localcod", "edad", *CAMPOS_TEXTO,
            "especie_codigo", "archivo_origen", "formato_origen",
            "registros_fuente", "ocurrencias_clave_coincidente",
        )
    )


def procesar_silver_malaria(
    spark: Optional[SparkSession] = None,
    input_path: Optional[str] = None,
    output_parquet_path: Optional[str] = None,
    config: Optional[AppConfig] = None,
    *,
    snapshot_versionado: bool = False,
) -> DataFrame:
    """Publica un único extracto completo en la ruta versionada de Silver."""
    cfg = config or get_config()
    active_spark = spark or get_spark_session(app_name="Limpieza_Malaria", config=cfg)
    ruta_in = input_path or f"{cfg.get_bronze_path('malaria')}/*/*.csv"
    ruta_out = output_parquet_path or f"{cfg.get_silver_path('malaria')}/parquet_{FORMATO_DETALLE}"
    logger.info("Leyendo extracto de detalle de malaria: %s", ruta_in)
    df_raw = (
        active_spark.read.option("header", True).option("sep", ",")
        .option("encoding", "UTF-8").option("mode", "FAILFAST")
        .csv(ruta_in)
    )
    archivos = df_raw.inputFiles()
    if len(archivos) != 1:
        raise ValueError("Malaria Silver: seleccione un único CSV Bronze completo mediante input_path.")
    origen_local = None
    if cfg.is_aws:
        archivo = unquote(archivos[0]).replace("s3a://", "s3://", 1)
    else:
        origen_local = Path(url2pathname(urlsplit(archivos[0]).path)).resolve()
        archivo = origen_local.as_uri()
    try:
        df_silver = transformar_datos_malaria(df_raw, archivo_origen=archivo)
    except ValueError as exc:
        logger.error("%s", exc)
        raise
    if not df_silver.take(1):
        raise ValueError("Malaria Silver: el extracto no tiene filas elegibles; no se publicó.")

    # El extracto es un snapshot completo. No conservar años de una versión anterior.
    modo = "errorifexists" if snapshot_versionado else "overwrite"
    (df_silver.write.mode(modo).option("partitionOverwriteMode", "static")
     .partitionBy("ano").parquet(ruta_out))

    if snapshot_versionado:
        # La publicación independiente mantiene su propio manifiesto y auditoría.
        return df_silver
    control = BatchControlManager(config=cfg)
    if cfg.is_aws:
        control.mark_batches_as_silver("malaria", archivos)
    else:
        for lote in control.get_pending_batches("malaria"):
            if Path(lote.destination_path).resolve() == origen_local:
                control.update_batch_status(lote.file_hash, "PROCESSED_SILVER")
    logger.info("Silver de malaria publicado en %s", ruta_out)
    return df_silver


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publica Silver de malaria desde un extracto completo.")
    parser.add_argument("--archivo", help="Ruta del CSV Bronze seleccionado.")
    procesar_silver_malaria(input_path=parser.parse_args().archivo)
