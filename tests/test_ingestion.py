"""
Pruebas Unitarias para el Módulo de Ingesta (src/ingestion/).
Cubre cálculo de SHA-256, gestión de lotes (BatchControlManager) e ingesta cruda idempotente.
Compatible tanto con 'pytest' como con 'python -m unittest'.
"""

import hashlib
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from src.common.config import AppConfig
from src.ingestion.batch_manager import (
    BatchControlManager,
    BatchMetadata,
    calculate_file_sha256,
)
from src.ingestion.raw_ingestor import ingest_raw_file


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

    def test_batch_control_registration_and_idempotency(self):
        """Verifica el registro de lotes y la detección de duplicados."""
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

        # Ahora debe figurar como procesado
        self.assertTrue(mgr.is_batch_processed(self.expected_hash, source="ira"))
        # Si consultamos por otra fuente, no debe colisionar
        self.assertFalse(mgr.is_batch_processed(self.expected_hash, source="malaria"))

    def test_batch_control_status_update(self):
        """Verifica la actualización de estado a Silver y Gold."""
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

        # Actualizar a Silver
        updated = mgr.update_batch_status(self.expected_hash, "PROCESSED_SILVER")
        self.assertTrue(updated)
        # Ya no debe figurar en lotes pendientes de Silver
        self.assertEqual(len(mgr.get_pending_batches(source="ira")), 0)

    def test_ingest_raw_file_lifecycle(self):
        """Verifica la copia física, particionamiento por fecha y omisión de duplicados."""
        # 1. Primera ingesta
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

        # 2. Segunda ingesta del mismo archivo sin force -> Debe omitirse
        exito2, meta2, estado2 = ingest_raw_file(
            file_path=self.sample_csv_path,
            source="ira",
            force=False,
            config=self.cfg,
        )
        self.assertFalse(exito2)
        self.assertIsNone(meta2)
        self.assertEqual(estado2, "SKIPPED_DUPLICATE")

        # 3. Tercera ingesta forzada -> Debe realizarse
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


if __name__ == "__main__":
    unittest.main()
