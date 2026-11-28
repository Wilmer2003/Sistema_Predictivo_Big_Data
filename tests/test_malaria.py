"""Pruebas Silver de malaria con extractos locales sintéticos."""
import csv
from collections import Counter
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from src.common.config import AppConfig
from src.ingestion.batch_manager import BatchControlManager
from src.ingestion.cargar_malaria import cargar_malaria
from src.preprocessing.limpiar_malaria import procesar_silver_malaria, transformar_datos_malaria
from tests.spark_support import create_test_spark_session

CAMPOS = ("ubigeo", "ano", "semana", "enfermedad", "localidad", "sexo", "edad")
VIVAX = "MALARIA POR P. VIVAX"
FALCIPARUM = "MALARIA P. FALCIPARUM"


@pytest.fixture(scope="module")
def spark():
    session = create_test_spark_session("Test_Limpieza_Malaria")
    yield session
    session.stop()


def crear_extracto(path, filas):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(CAMPOS)
        writer.writerows(filas)


def test_seis_colisiones_historicas_y_ocurrencias_identicas(spark):
    filas = [
        (" 130301.0 ", "2018", "19", especie, f"SINTETICA_{indice}", "F", "20")
        for indice in range(6)
        for especie in ("  " + VIVAX.lower() + " ", FALCIPARUM)
    ]
    filas.append(filas[0])
    obtenido = transformar_datos_malaria(spark.createDataFrame(filas, CAMPOS)).collect()

    assert Counter(r.especie_codigo for r in obtenido) == {"B51": 7, "B50": 6}
    assert Counter(r.ocurrencias_clave_coincidente for r in obtenido) == {2: 10, 3: 3}
    assert {r.ubigeo for r in obtenido} == {"130301"}
    assert {r.registros_fuente for r in obtenido} == {1}


def test_filtros_geograficos_temporales_y_diagnostico_concordante(spark):
    filas = [
        ("130301", "2015", "1", VIVAX, "S", "F", "20", "b51"),
        ("130301", "2024", "53", FALCIPARUM, "S", "F", "20", "B50"),
        ("130301", "2014", "1", VIVAX, "S", "F", "20", "B51"),
        ("160101", "2018", "19", VIVAX, "S", "F", "20", "B51"),
    ]
    df = spark.createDataFrame(filas, (*CAMPOS, "diagnostic"))
    assert {(r.ano, r.semana, r.especie_codigo) for r in transformar_datos_malaria(df).collect()} == {
        (2015, 1, "B51"), (2024, 53, "B50")
    }


def test_rechaza_errores_con_conteos_agregados(spark):
    filas = [
        ("130301", "2018", "19", "DESCONOCIDA", "SINTETICA", "F", "20", "B51", "LA LIBERTAD"),
        ("130301", "2018", "19", VIVAX, "SINTETICA", "F", "20", "B50", "LA LIBERTAD"),
        ("130301", "2018", "54", VIVAX, "SINTETICA", "F", "20", "B51", "LA LIBERTAD"),
        ("1303010", "2018", "19", VIVAX, "SINTETICA", "F", "20", "B51", "LA LIBERTAD"),
        ("130301", "INVALIDO", "19", VIVAX, "SINTETICA", "F", "20", "B51", "LA LIBERTAD"),
        ("130301", "2018", "19", VIVAX, "SINTETICA", "F", "INVALIDA", "B51", "LA LIBERTAD"),
        ("160101", "2018", "19", VIVAX, "SINTETICA", "F", "20", "B51", "LA LIBERTAD"),
    ]
    df = spark.createDataFrame(filas, (*CAMPOS, "diagnostic", "departamento"))
    with pytest.raises(ValueError) as error:
        transformar_datos_malaria(df)
    for motivo in (
        "especie_desconocida", "diagnostico_en_conflicto", "semana_invalida",
        "ubigeo_invalido", "ano_invalido", "edad_invalida", "departamento_ubigeo_en_conflicto",
    ):
        assert f"{motivo}=1" in str(error.value)
    assert "SINTETICA" not in str(error.value)


def test_rechaza_esquemas_incompatibles_y_agregados_repetidos(spark):
    filas = [("130301", "2018", "19", VIVAX, "S", "F", "20")] * 2
    df = spark.createDataFrame(filas, CAMPOS)
    with pytest.raises(ValueError, match="Faltantes=.*enfermedad"):
        transformar_datos_malaria(df.drop("enfermedad"))
    with pytest.raises(ValueError, match="encabezados"):
        transformar_datos_malaria(df.selectExpr("*", "ano AS ANO"))
    agregado = spark.createDataFrame(
        [(*fila, 14, 8, 6) for fila in filas], (*CAMPOS, "Malaria_total", "B50", "B51")
    )
    with pytest.raises(ValueError, match="agregado no acreditado"):
        transformar_datos_malaria(agregado)


def test_reintento_ingesta_y_snapshot_silver(spark, tmp_path):
    cfg = AppConfig(env="local", data_dir_override=str(tmp_path / "datos"))
    origen = tmp_path / "notificaciones.csv"
    fila = ("130301", "2018", "19", VIVAX, "S", "F", "20")
    crear_extracto(origen, [fila, fila])
    lote = cargar_malaria(str(origen), config=cfg)
    assert lote is not None
    assert cargar_malaria(str(origen), config=cfg) is None

    otro = tmp_path / "otra_version.csv"
    crear_extracto(otro, [("130301", "2019", "19", FALCIPARUM, "S", "F", "20")])
    otro_lote = cargar_malaria(str(otro), config=cfg)
    assert otro_lote is not None
    with pytest.raises(ValueError, match="único CSV"):
        procesar_silver_malaria(spark=spark, config=cfg)

    ruta = f"{cfg.get_silver_path('malaria')}/parquet_detalle_v1"
    for _ in range(2):
        procesar_silver_malaria(spark=spark, input_path=lote.destination_path, config=cfg)
        obtenido = spark.read.parquet(ruta).collect()
        assert len(obtenido) == 2
        assert {r.registros_fuente for r in obtenido} == {1}
        assert {r.archivo_origen for r in obtenido} == {Path(lote.destination_path).as_uri()}

    gestor = BatchControlManager(cfg)
    assert {r.file_hash for r in gestor.get_pending_batches("malaria")} == {otro_lote.file_hash}
    procesar_silver_malaria(spark=spark, input_path=otro_lote.destination_path, config=cfg)
    obtenido = spark.read.parquet(ruta).collect()
    assert [(r.ano, r.especie_codigo) for r in obtenido] == [(2019, "B50")]
    assert not gestor.get_pending_batches("malaria")
    assert not Path(cfg.get_silver_path("malaria"), "parquet").exists()
