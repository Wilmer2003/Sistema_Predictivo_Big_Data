"""
Pruebas Unitarias para el Manejo de Errores en la Capa Silver
(src/preprocessing/base_preprocessor.py::mark_pending_batches_failed).
Valida que un lote con error se marque como 'FAILED' y deje de aparecer
como pendiente, es decir, que no avance a la capa siguiente.
"""

import os
import tempfile
import shutil
import unittest

from src.common.config import AppConfig
from src.ingestion.batch_manager import BatchControlManager, BatchMetadata
from src.preprocessing.base_preprocessor import mark_pending_batches_failed


class TestManejoErroresSilver(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_silver_errores_")
        self.data_dir = os.path.join(self.temp_dir, "data")
        self.cfg = AppConfig(env="local", data_dir_override=self.data_dir)
        self.mgr = BatchControlManager(config=self.cfg)

        self.meta = BatchMetadata(
            batch_id="malaria_test_fail_01",
            source="malaria",
            file_name="dataset_malaria.csv",
            file_hash="hash_de_prueba_0001",
            file_size_bytes=123,
            load_date="2024-01-01",
            ingest_timestamp="2024-01-01T00:00:00Z",
            destination_path="/tmp/test/dataset_malaria.csv",
            status="REGISTERED",
        )
        self.mgr.register_batch(self.meta)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_lote_con_error_se_marca_failed_y_no_avanza(self):
        """Un lote pendiente, al fallar su procesamiento, se marca FAILED y deja de ser pendiente."""
        # Antes del fallo: el lote está pendiente (REGISTERED)
        self.assertEqual(len(self.mgr.get_pending_batches(source="malaria")), 1)

        error_simulado = RuntimeError("CSV corrupto: columna 'edad' con formato inválido")
        mark_pending_batches_failed(self.mgr, "malaria", error_simulado)

        # Después del fallo: ya no debe aparecer como pendiente (no avanza a Silver)
        self.assertEqual(len(self.mgr.get_pending_batches(source="malaria")), 0)

        # Y su estado real debe ser FAILED, no PROCESSED_SILVER
        registro = self.mgr._load_registry_local()
        batch = next(b for b in registro["batches"] if b["batch_id"] == "malaria_test_fail_01")
        self.assertEqual(batch["status"], "FAILED")

    def test_lote_failed_no_bloquea_reintento_con_nuevo_hash(self):
        """Un lote FAILED no debe considerarse 'ya procesado' para un hash distinto (se puede reintentar)."""
        mark_pending_batches_failed(self.mgr, "malaria", RuntimeError("fallo simulado"))

        # Un archivo corregido (hash distinto) para la misma fuente no debe verse afectado
        self.assertFalse(
            self.mgr.is_batch_processed("hash_de_prueba_0002", source="malaria")
        )


if __name__ == "__main__":
    unittest.main()
