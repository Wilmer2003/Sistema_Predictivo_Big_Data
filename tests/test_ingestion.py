"""
Pruebas Unitarias para el Módulo de Ingesta (src/ingestion/).
Cubre cálculo de SHA-256, gestión de lotes (BatchControlManager) e ingesta cruda idempotente
tanto en modo Local como en Amazon Web Services (S3 / Amazon RDS tabla lotes y Amazon EMR con mocks).
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

# Asegurar módulo mock para boto3 si no está instalado en el entorno del host
if "boto3" not in sys.modules:
    sys.modules["boto3"] = MagicMock()

from src.common.config import AppConfig, PostgresConfig
from src.ingestion.batch_manager import (
    BatchControlManager,
    BatchMetadata,
    calculate_file_sha256,
)
from src.ingestion.raw_ingestor import ingest_raw_file
from src.orchestration.run_pipeline import lanzar_step_emr


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

    def test_ingest_raw_file_aws_mock(self):
        """Verifica que en modo AWS la ingesta apunte a s3:// y suba el archivo con boto3."""
        aws_cfg = AppConfig(
            env="aws",
            s3_bucket="bucket-aws-test",
            aws_region="us-east-1",
        )

        mock_s3_client = MagicMock()

        import boto3
        with patch.object(boto3, "client", return_value=mock_s3_client):
            with patch.object(BatchControlManager, "_get_db_connection") as mock_conn:
                mock_cursor = MagicMock()
                mock_conn.return_value.cursor.return_value.__enter__.return_value = mock_cursor
                mock_cursor.fetchone.return_value = None  # No duplicado

                exito, meta, estado = ingest_raw_file(
                    file_path=self.sample_csv_path,
                    source="senamhi",
                    force=False,
                    config=aws_cfg,
                )

                self.assertTrue(exito)
                self.assertIsNotNone(meta)
                self.assertTrue(meta.destination_path.startswith("s3://bucket-aws-test/bronze/senamhi/"))
                self.assertEqual(estado, "INGESTED")
                mock_s3_client.upload_file.assert_called_once()

    def test_batch_control_manager_aws_table_mock(self):
        """Verifica la persistencia en la tabla 'lotes' de Amazon RDS en modo AWS con mock de psycopg2."""
        aws_cfg = AppConfig(
            env="aws",
            s3_bucket="bucket-test",
            aws_region="us-east-1",
        )

        mgr = BatchControlManager(config=aws_cfg)
        meta = BatchMetadata(
            batch_id="malaria_aws_01",
            source="malaria",
            file_name="malaria.csv",
            file_hash="hash123456",
            file_size_bytes=1000,
            load_date="2024-05-01",
            ingest_timestamp="2024-05-01T12:00:00Z",
            destination_path="s3://bucket-test/bronze/malaria/malaria.csv",
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

    def test_lanzar_step_emr_mock(self):
        """Verifica el despacho de pasos a Amazon EMR con mock de boto3 client."""
        aws_cfg = AppConfig(
            env="aws",
            s3_bucket="bucket-test",
            aws_region="us-east-1",
            emr_cluster_id="j-123456789",
        )

        mock_emr_client = MagicMock()
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-123456"]}
        mock_emr_client.describe_step.return_value = {
            "Step": {"Status": {"State": "COMPLETED"}}
        }

        import boto3
        with patch.object(boto3, "client", return_value=mock_emr_client):
            resultado = lanzar_step_emr(
                script_s3_uri="s3://bucket-test/scripts/run_pipeline.py",
                step_args=["--silver", "--gold"],
                config=aws_cfg,
            )
            self.assertTrue(resultado)
            mock_emr_client.add_job_flow_steps.assert_called_once()
            mock_emr_client.describe_step.assert_called_once_with(
                ClusterId="j-123456789", StepId="s-123456"
            )


if __name__ == "__main__":
    unittest.main()
