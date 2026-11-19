"""
Pruebas Unitarias para el Módulo de Ingesta (src/ingestion/).
Cubre cálculo de SHA-256, gestión de lotes (BatchControlManager) e ingesta cruda idempotente
tanto en modo Local como en Google Cloud Platform (GCS / Cloud SQL tabla lotes con mocks).
Compatible tanto con 'pytest' como con 'python -m unittest'.
"""

import hashlib
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

# Asegurar módulos mock para Google Cloud si las librerías no están instaladas en el host
if "google" not in sys.modules:
    sys.modules["google"] = MagicMock()
if "google.cloud" not in sys.modules:
    sys.modules["google.cloud"] = MagicMock()
    sys.modules["google"].cloud = sys.modules["google.cloud"]
if "google.cloud.storage" not in sys.modules:
    _mock_storage = MagicMock()
    sys.modules["google.cloud.storage"] = _mock_storage
    sys.modules["google.cloud"].storage = _mock_storage
if "google.cloud.dataproc_v1" not in sys.modules:
    _mock_dp = MagicMock()
    _mock_state = MagicMock()
    _mock_state.DONE = MagicMock()
    _mock_state.DONE.name = "DONE"
    _mock_dp.JobStatus.State = _mock_state
    sys.modules["google.cloud.dataproc_v1"] = _mock_dp
    sys.modules["google.cloud"].dataproc_v1 = _mock_dp

from src.common.config import AppConfig, PostgresConfig
from src.ingestion.batch_manager import (
    BatchControlManager,
    BatchMetadata,
    calculate_file_sha256,
)
from src.ingestion.raw_ingestor import ingest_raw_file
from src.orchestration.run_pipeline import lanzar_job_dataproc


class TestIngestion(unittest.TestCase):
    def setUp(self):
        """Crea directorios temporales aislados para cada prueba."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_pipeline_")
        self.data_dir = os.path.join(self.temp_dir, "data")
        self.cfg = AppConfig(env="local", data_dir_override=self.data_dir)

        # Crear un archivo de prueba con contenido conocido
        self.sample_csv_path = os.path.join(self.temp_dir, "sample_ira.csv")
        self.sample_content = b"Departamento;Provincia;Distrito;Ano;Semana_Epidemiologica;Codigo_Ubigeo\nLA LIBERTAD;TRUJILLO;TRUJILLO;2024;1;130101\n"
        with open(self.sample_csv_path, "wb") as f:
            f.write(self.sample_content)

        # Hash esperado
        self.expected_hash = hashlib.sha256(self.sample_content).hexdigest()

    def tearDown(self):
        """Limpia los directorios temporales creados."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_calculate_file_sha256_accuracy(self):
        """Verifica la precisión criptográfica del hash SHA-256."""
        computed_hash = calculate_file_sha256(self.sample_csv_path)
        self.assertEqual(computed_hash, self.expected_hash)

    def test_calculate_file_sha256_non_existent_file(self):
        """Verifica que un archivo inexistente levante FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            calculate_file_sha256(os.path.join(self.temp_dir, "non_existent.csv"))

    def test_batch_control_registration_and_idempotency_local(self):
        """Verifica el registro de lotes y la detección de duplicados en modo local."""
        mgr = BatchControlManager(config=self.cfg)
        self.assertFalse(mgr.is_batch_processed(self.expected_hash, source="ira"))

        meta = BatchMetadata(
            batch_id="ira_test_01",
            source="ira",
            file_name="sample_ira.csv",
            file_hash=self.expected_hash,
            file_size_bytes=len(self.sample_content),
            load_date="2024-01-01",
            ingest_timestamp="2024-01-01T00:00:00Z",
            destination_path="/tmp/test",
            status="REGISTERED",
        )
        mgr.register_batch(meta)

        self.assertTrue(mgr.is_batch_processed(self.expected_hash, source="ira"))
        self.assertFalse(mgr.is_batch_processed(self.expected_hash, source="malaria"))

    def test_batch_control_status_update_local(self):
        """Verifica la actualización de estado a Silver y Gold en modo local."""
        mgr = BatchControlManager(config=self.cfg)
        meta = BatchMetadata(
            batch_id="ira_test_02",
            source="ira",
            file_name="sample_ira.csv",
            file_hash=self.expected_hash,
            file_size_bytes=len(self.sample_content),
            load_date="2024-01-01",
            ingest_timestamp="2024-01-01T00:00:00Z",
            destination_path="/tmp/test",
            status="REGISTERED",
        )
        mgr.register_batch(meta)
        self.assertEqual(len(mgr.get_pending_batches(source="ira")), 1)

        updated = mgr.update_batch_status(self.expected_hash, "PROCESSED_SILVER")
        self.assertTrue(updated)
        self.assertEqual(len(mgr.get_pending_batches(source="ira")), 0)

    def test_ingest_raw_file_lifecycle_local(self):
        """Verifica la copia física, particionamiento por fecha y omisión de duplicados en local."""
        exito, meta, estado = ingest_raw_file(
            file_path=self.sample_csv_path,
            source="ira",
            force=False,
            config=self.cfg,
        )
        self.assertTrue(exito)
        self.assertIsNotNone(meta)
        self.assertEqual(estado, "INGESTED")
        self.assertTrue(os.path.isfile(meta.destination_path))
        self.assertIn("fecha_ingesta=", meta.destination_path)

        # Segunda ingesta sin force -> Debe omitirse
        exito2, meta2, estado2 = ingest_raw_file(
            file_path=self.sample_csv_path,
            source="ira",
            force=False,
            config=self.cfg,
        )
        self.assertFalse(exito2)
        self.assertIsNone(meta2)
        self.assertEqual(estado2, "SKIPPED_DUPLICATE")

        # Tercera ingesta forzada -> Debe realizarse
        exito3, meta3, estado3 = ingest_raw_file(
            file_path=self.sample_csv_path,
            source="ira",
            force=True,
            config=self.cfg,
        )
        self.assertTrue(exito3)
        self.assertIsNotNone(meta3)
        self.assertEqual(estado3, "INGESTED")

    def test_ingest_invalid_source_raises(self):
        """Verifica que fuentes no reconocidas arrojen ValueError."""
        with self.assertRaises(ValueError):
            ingest_raw_file(
                file_path=self.sample_csv_path,
                source="covid",
                config=self.cfg,
            )

    def test_ingest_raw_file_empty_file(self):
        """Verifica que un archivo de 0 bytes se rechace y no se registre como lote."""
        empty_path = os.path.join(self.temp_dir, "vacio_ira.csv")
        with open(empty_path, "wb"):
            pass  # crea el archivo sin escribir nada (0 bytes)

        exito, meta, estado = ingest_raw_file(
            file_path=empty_path,
            source="ira",
            config=self.cfg,
        )

        self.assertFalse(exito)
        self.assertIsNone(meta)
        self.assertTrue(estado.startswith("EMPTY_FILE"))

        # No debe haber quedado registrado ningún lote para este archivo
        mgr = BatchControlManager(config=self.cfg)
        self.assertEqual(len(mgr.get_pending_batches(source="ira")), 0)

    def test_ingest_raw_file_distinct_columns_not_treated_as_duplicate(self):
        """
        Verifica que dos archivos del mismo origen con columnas/contenido distinto
        (por lo tanto, hash distinto) se ingieran como dos lotes independientes,
        sin que el control de lotes los confunda por duplicados.
        """
        otro_contenido = (
            b"Departamento;Provincia;Distrito;Ano;Semana_Epidemiologica;"
            b"Codigo_Ubigeo;Columna_Extra\n"
            b"LA LIBERTAD;TRUJILLO;TRUJILLO;2024;1;130101;999\n"
        )
        otro_path = os.path.join(self.temp_dir, "sample_ira_v2.csv")
        with open(otro_path, "wb") as f:
            f.write(otro_contenido)

        exito1, meta1, estado1 = ingest_raw_file(
            file_path=self.sample_csv_path,
            source="ira",
            config=self.cfg,
        )
        exito2, meta2, estado2 = ingest_raw_file(
            file_path=otro_path,
            source="ira",
            config=self.cfg,
        )

        self.assertTrue(exito1)
        self.assertTrue(exito2)
        self.assertEqual(estado1, "INGESTED")
        self.assertEqual(estado2, "INGESTED")
        self.assertNotEqual(meta1.file_hash, meta2.file_hash)

        mgr = BatchControlManager(config=self.cfg)
        self.assertEqual(len(mgr.get_pending_batches(source="ira")), 2)

    def test_ingest_raw_file_gcp_mock(self):
        """Verifica que en modo GCP la ingesta apunte a gs:// y suba el blob a Google Cloud Storage."""
        gcp_cfg = AppConfig(
            env="gcp",
            gcs_bucket="bucket-gcp-test",
            gcp_project_id="proyecto-gcp",
        )

        mock_storage_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Mockear storage.Client de Google Cloud y _get_db_connection
        with patch("google.cloud.storage.Client", return_value=mock_storage_client):
            with patch.object(BatchControlManager, "_get_db_connection") as mock_conn:
                mock_cursor = MagicMock()
                mock_conn.return_value.cursor.return_value.__enter__.return_value = mock_cursor
                mock_cursor.fetchone.return_value = None  # No duplicado

                exito, meta, estado = ingest_raw_file(
                    file_path=self.sample_csv_path,
                    source="senamhi",
                    force=False,
                    config=gcp_cfg,
                )

                self.assertTrue(exito)
                self.assertIsNotNone(meta)
                self.assertTrue(meta.destination_path.startswith("gs://bucket-gcp-test/bronze/senamhi/"))
                self.assertEqual(estado, "INGESTED")
                mock_blob.upload_from_filename.assert_called_once()

    def test_batch_control_manager_gcp_table_mock(self):
        """Verifica la persistencia en la tabla 'lotes' de Cloud SQL en modo GCP con mock de psycopg2."""
        gcp_cfg = AppConfig(
            env="gcp",
            gcs_bucket="bucket-test",
            gcp_project_id="proyecto-test",
        )

        mgr = BatchControlManager(config=gcp_cfg)
        meta = BatchMetadata(
            batch_id="malaria_gcp_01",
            source="malaria",
            file_name="malaria.csv",
            file_hash="hash123456",
            file_size_bytes=1000,
            load_date="2024-05-01",
            ingest_timestamp="2024-05-01T12:00:00Z",
            destination_path="gs://bucket-test/bronze/malaria/malaria.csv",
            status="REGISTERED",
        )

        with patch.object(BatchControlManager, "_get_db_connection") as mock_conn:
            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value.__enter__.return_value = mock_cursor

            # 1. Registrar
            mgr.register_batch(meta)
            self.assertTrue(mock_cursor.execute.called)

            # 2. Consultar existencia
            mock_cursor.fetchone.return_value = ("REGISTERED",)
            procesado = mgr.is_batch_processed("hash123456", source="malaria")
            self.assertTrue(procesado)

            # 3. Actualizar estado
            mock_cursor.rowcount = 1
            updated = mgr.update_batch_status("hash123456", "PROCESSED_SILVER")
            self.assertTrue(updated)

    def test_lanzar_job_dataproc_mock(self):
        """Verifica el despacho de jobs a Dataproc con mock de JobControllerClient."""
        gcp_cfg = AppConfig(
            env="gcp",
            gcs_bucket="bucket-test",
            gcp_project_id="proyecto-test",
            gcp_region="us-central1",
            dataproc_cluster_name="cluster-test",
        )

        mock_job_client = MagicMock()
        mock_operation = MagicMock()
        mock_response = MagicMock()
        # Mock estado final DONE
        from google.cloud import dataproc_v1 as dataproc
        mock_response.status.state = dataproc.JobStatus.State.DONE
        mock_operation.result.return_value = mock_response
        mock_job_client.submit_job_as_operation.return_value = mock_operation

        with patch("google.cloud.dataproc_v1.JobControllerClient", return_value=mock_job_client):
            resultado = lanzar_job_dataproc(
                main_python_file_uri="gs://bucket-test/scripts/run_pipeline.py",
                job_args=["--silver", "--gold"],
                config=gcp_cfg,
            )
            self.assertTrue(resultado)
            mock_job_client.submit_job_as_operation.assert_called_once()


if __name__ == "__main__":
    unittest.main()
