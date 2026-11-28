"""Gold semanal de registros de origen de malaria."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.preprocessing.limpiar_malaria import FORMATO_DETALLE

CLAVE_MALARIA = ("ubigeo", "ano", "semana_epidemiologica", "especie_codigo")
COLUMNAS_GOLD_MALARIA = (
    *CLAVE_MALARIA, "tiempo_id", "departamento", "provincia", "distrito",
    "registros_fuente",
)


def agregar_vigilancia_malaria(detalle: DataFrame) -> DataFrame:
    """Agrega únicamente el contrato Silver de detalle."""
    nombres_geo = ("departamento", "provincia", "distrito")
    requeridas = {"ubigeo", "ano", "semana", "especie_codigo", *nombres_geo,
                  "registros_fuente", "formato_origen"}
    if requeridas - set(detalle.columns):
        raise ValueError("Gold malaria requiere el contrato Silver de detalle.")
    valido = (
        (F.col("formato_origen") == FORMATO_DETALLE)
        & (F.col("registros_fuente") == 1)
        & F.col("especie_codigo").isin("B50", "B51")
        & F.col("ubigeo").rlike("^13[0-9]{4}$")
        & F.col("ano").between(2015, 2024)
        & F.col("semana").between(1, 53)
    )
    if detalle.where(~F.coalesce(valido, F.lit(False))).limit(1).count():
        raise ValueError("Gold malaria: filas incompatibles con el contrato de detalle.")
    geografia = detalle.groupBy("ubigeo").agg(
        *(F.max(c).alias(c) for c in nombres_geo),
        *(F.countDistinct(c).alias(f"variantes_{c}") for c in nombres_geo),
    )
    if geografia.where("variantes_departamento > 1 OR variantes_provincia > 1 OR variantes_distrito > 1").limit(1).count():
        raise ValueError("Gold malaria: etiquetas geográficas incompatibles para un ubigeo.")
    return (
        detalle.withColumnRenamed("semana", "semana_epidemiologica")
        .groupBy(*CLAVE_MALARIA).agg(F.sum("registros_fuente").alias("registros_fuente"))
        .join(geografia.select("ubigeo", *nombres_geo), "ubigeo")
        .withColumn("tiempo_id", F.col("ano") * 100 + F.col("semana_epidemiologica"))
        .select(*COLUMNAS_GOLD_MALARIA)
    )
