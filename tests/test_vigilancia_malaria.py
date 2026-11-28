"""Contratos de vigilancia y de compatibilidad del modelo IRA."""
import pytest

pytest.importorskip("pyspark")

from src.integration.integrar_datasets import agregar_malaria_semanal, aplicar_feature_engineering_completo, integrar_fuentes_silver
from src.integration.vigilancia_malaria import agregar_vigilancia_malaria
from src.modeling.pipeline import DEFAULT_FEATURE_COLS, TARGET_COL_IRA
from src.preprocessing.limpiar_malaria import transformar_datos_malaria
from tests.spark_support import create_test_spark_session


@pytest.fixture(scope="module")
def spark():
    session = create_test_spark_session("Test_Vigilancia_Malaria")
    yield session
    session.stop()


def test_vigilancia_incluye_claves_sin_ira_y_no_usa_agregados(spark):
    entrada = spark.createDataFrame([
        ("130301", 2018, 19, "MALARIA POR P. VIVAX"),
        ("130301", 2018, 19, "MALARIA P. FALCIPARUM"),
        ("130301", 2018, 19, "MALARIA P. FALCIPARUM"),
        ("130306", 2018, 24, "MALARIA POR P. VIVAX"),
    ], ["ubigeo", "ano", "semana", "enfermedad"])
    silver = transformar_datos_malaria(entrada)
    filas = agregar_vigilancia_malaria(silver).collect()
    assert {(r.ubigeo, r.tiempo_id, r.especie_codigo): r.registros_fuente for r in filas} == {
        ("130301", 201819, "B51"): 1, ("130301", 201819, "B50"): 2,
        ("130306", 201824, "B51"): 1,
    }
    with pytest.raises(ValueError, match="contrato Silver"):
        agregar_vigilancia_malaria(entrada.withColumnRenamed("enfermedad", "Malaria_total"))
    with pytest.raises(ValueError, match="Gold semanal"):
        agregar_malaria_semanal(entrada)


def test_referencia_malaria_no_modifica_ira(spark):
    ira = spark.createDataFrame(
        [("130301", y, w, w, 2, 3, 4, 5, 6, 0) for y in (2022, 2023) for w in range(1, 5)],
        ["Codigo_Ubigeo", "Ano", "Semana_Epidemiologica", "IRA_menores_2_meses",
         "IRA_2_a_11_meses", "IRA_1_a_4_anos", "Neumonia_menores_2_meses",
         "Neumonia_2_a_11_meses", "Neumonia_1_a_4_anos", "Defunciones"],
    )
    clima = spark.createDataFrame(
        [(y, w, 26.0, 17.0, 21.0, 2.0) for y in (2022, 2023) for w in range(1, 5)],
        ["Ano", "Semana_Epidemiologica", "Temperatura_maxima", "Temperatura_minima", "tc", "Precipitacion_acumulada"],
    )
    legacy = spark.createDataFrame(
        [("130301", 2022, 1, 3, 1, 2), ("130306", 2022, 1, 7, 0, 7)],
        ["ubigeo", "ano", "semana_epidemiologica", "malaria_casos_total", "malaria_b50", "malaria_b51"],
    )
    claves = ("ubigeo", "ano", "semana_epidemiologica")
    columnas = (*claves, TARGET_COL_IRA, *DEFAULT_FEATURE_COLS)
    con = aplicar_feature_engineering_completo(integrar_fuentes_silver(ira, legacy, clima))
    sin = aplicar_feature_engineering_completo(integrar_fuentes_silver(ira, None, clima))
    assert con.select(*columnas).orderBy(*claves).collect() == sin.select(*columnas).orderBy(*claves).collect()
    assert con.count() == 8
    assert con.where("malaria_casos_total IS NOT NULL").count() == 1
    assert sin.where("malaria_casos_total IS NOT NULL").count() == 0
