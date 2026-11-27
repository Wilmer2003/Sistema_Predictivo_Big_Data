import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.common.config import AppConfig, PostgresConfig
from src.common.postgres import connect_postgres, postgres_cursor
from src.common.s3_artifacts import put_json
from src.ingestion.batch_manager import BatchControlManager
from src.ingestion.raw_ingestor import ingest_raw_file
from src.orchestration.run_pipeline import ejecutar_pipeline_completo
from src.serving.metricas import persistir_metricas_aws


@pytest.fixture
def config(tmp_path):
    return AppConfig(env="aws", s3_bucket="pruebas", run_id="run-01", data_dir_override=str(tmp_path))


@pytest.fixture
def reporte():
    return {
        "timestamp_evaluacion": "2025-03-01T12:00:00+00:00",
        "modelo": "random_forest",
        "target": "ira_total_menores_5",
        "split_temporal": {"train_max_year": 2022, "test_min_year": 2023},
        "metricas_modelo": {"rmse": 2, "mae": 1, "r2": 0.9},
        "metricas_baseline_persistencia": {"rmse": 4, "mae": 2, "r2": 0.8},
        "ganancia_vs_baseline": {
            "reduccion_rmse_porcentaje": 50, "reduccion_mae_porcentaje": 50,
            "supera_al_baseline": True,
        },
    }


def test_aws_no_crea_respaldo_local_ni_silencia_fallo_de_rds(config, tmp_path):
    mgr = BatchControlManager(config)
    with patch.object(mgr, "_get_db_connection", side_effect=RuntimeError("RDS inaccesible")):
        with pytest.raises(RuntimeError, match="RDS inaccesible"):
            mgr.is_batch_processed("hash")
    assert not (tmp_path / "control").exists()


def test_subida_fallida_no_registra_lote(config, tmp_path):
    entrada = tmp_path / "entrada.csv"
    entrada.write_text("x\n1\n", encoding="utf-8")
    with (
        patch.object(BatchControlManager, "is_batch_processed", return_value=False),
        patch.object(BatchControlManager, "register_batch") as register,
        patch("boto3.client") as boto,
    ):
        boto.return_value.upload_file.side_effect = RuntimeError("S3 inaccesible")
        with pytest.raises(RuntimeError, match="S3 inaccesible"):
            ingest_raw_file(str(entrada), "malaria", config=config)
    register.assert_not_called()


def test_auditoria_s3_usa_solo_lotes_de_rds(config):
    lote = {"batch_id": "real", "status": "PROCESSED_GOLD"}
    with (
        patch.object(BatchControlManager, "_get_db_connection") as connect,
        patch("boto3.client") as boto,
    ):
        cursor = connect.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(lote,)]
        uri = BatchControlManager(config).publish_snapshot("gold")
    assert uri == "s3://pruebas/control/runs/run-01/gold/batch_control.json"
    body = boto.return_value.put_object.call_args.kwargs["Body"]
    assert json.loads(body)["batches"] == [lote]
    connect.return_value.close.assert_called_once()


def test_silver_actualiza_solamente_archivos_leidos(config):
    with patch.object(BatchControlManager, "_get_db_connection") as connect:
        BatchControlManager(config).mark_batches_as_silver("ira", ["s3a://pruebas/a%20b.csv"])
    cursor = connect.return_value.cursor.return_value.__enter__.return_value
    assert cursor.execute.call_args.args[1] == ("ira", ["s3://pruebas/a b.csv"])


def test_despacho_silver_no_incluye_modelo_ni_serving(config):
    with patch("src.orchestration.run_pipeline.lanzar_step_emr", return_value=True) as lanzar:
        assert ejecutar_pipeline_completo(
            run_ingestion=False, run_gold=False, run_model=False, run_serving=False,
            submit_to_emr=True, config=config,
        )
    assert lanzar.call_args.kwargs["step_args"] == ["--silver"]


def test_postgres_tls_y_transaccion():
    config = PostgresConfig(sslmode="verify-full", sslrootcert="/ca.pem")
    with patch("psycopg2.connect") as connect:
        conn = connect_postgres(config)
        with pytest.raises(RuntimeError):
            with postgres_cursor(conn):
                raise RuntimeError("fallo")
    assert connect.call_args.kwargs["sslmode"] == "verify-full"
    assert connect.call_args.kwargs["sslrootcert"] == "/ca.pem"
    assert conn.__exit__.call_args.args[0] is RuntimeError
    conn.close.assert_called_once()


def test_metricas_reintento_idempotente_y_conflicto(config, reporte):
    with patch("boto3.client") as boto, patch("src.serving.metricas.connect_postgres") as connect:
        cursor = connect.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("run-01",), None, (1,), None, None]
        uri = persistir_metricas_aws(config, reporte)
        assert persistir_metricas_aws(config, reporte) == uri
        with pytest.raises(ValueError, match="métricas distintas"):
            persistir_metricas_aws(config, reporte)
    assert uri == "s3://pruebas/results/estadisticas/runs/run-01/metricas_evaluacion.json"
    body = boto.return_value.put_object.call_args.kwargs["Body"]
    assert json.loads(body) == {**reporte, "run_id": "run-01"}
    assert cursor.execute.call_args_list[0].args[1][:6] == (
        "run-01", datetime.fromisoformat(reporte["timestamp_evaluacion"]),
        reporte["modelo"], reporte["target"], 2022, 2023,
    )


def test_s3_inmutable_acepta_mismo_contenido_y_rechaza_otro(config):
    error = ClientError({"ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")
    with patch("boto3.client") as boto:
        s3 = boto.return_value
        s3.put_object.side_effect = error
        stream = s3.get_object.return_value["Body"].__enter__.return_value
        stream.read.return_value = b'{\n  "run_id": "run-01"\n}'
        put_json(config, "key", {"run_id": "run-01"}, immutable=True)
        assert s3.put_object.call_args.kwargs["IfNoneMatch"] == "*"
        with pytest.raises(ValueError, match="contenido distinto"):
            put_json(config, "key", {"run_id": "run-02"}, immutable=True)


def test_metricas_s3_falla_antes_de_insertar(config, reporte):
    with (
        patch("src.serving.metricas.put_json", side_effect=RuntimeError("S3")),
        patch("src.serving.metricas.connect_postgres") as connect,
    ):
        with pytest.raises(RuntimeError, match="S3"):
            persistir_metricas_aws(config, reporte)
    connect.assert_not_called()
