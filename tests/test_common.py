"""
Pruebas Unitarias para el Módulo Común (src/common/).
Cubre Configuración, Logging, Estandarización de Ubigeo y Semanas Epidemiológicas.
Compatible tanto con 'pytest' como con 'python -m unittest'.
"""

from datetime import date
import unittest

from src.common.config import (
    AppConfig,
    PostgresConfig,
    CODIGO_DEPARTAMENTO_LA_LIBERTAD,
    NOMBRE_DEPARTAMENTO_LA_LIBERTAD,
)
from src.common.logger import get_logger
from src.common.epi_utils import (
    calculate_epi_week,
    calculate_epi_year_and_week,
    is_valid_semana_epidemiologica,
)
from src.common.geo_utils import (
    clean_ubigeo_str,
    is_ubigeo_la_libertad,
)


# ==============================================================================
# 1. PRUEBAS DE CONFIGURACIÓN Y RUTAS (Local vs Google Cloud)
# ==============================================================================
class TestConfig(unittest.TestCase):
    def test_local_default_paths(self):
        """Verifica que las rutas en entorno local apunten a data/raw, data/processed y data/curated."""
        cfg = AppConfig(env="local", gcs_bucket="")
        self.assertFalse(cfg.is_gcp)
        self.assertTrue(cfg.raw_base_path.endswith("/data/raw"))
        self.assertTrue(cfg.processed_base_path.endswith("/data/processed"))
        self.assertTrue(cfg.curated_base_path.endswith("/data/curated"))
        self.assertTrue(cfg.get_bronze_path("ira").endswith("/data/raw/ira"))
        self.assertTrue(cfg.get_silver_path("malaria").endswith("/data/processed/malaria"))
        self.assertTrue(cfg.get_gold_path("epidemiologia_clima").endswith("/data/curated/epidemiologia_clima"))
        self.assertTrue(cfg.get_model_path("rf_ira_model").endswith("/results/modelos/rf_ira_model"))

    def test_gcp_gcs_paths(self):
        """Verifica que en entorno Google Cloud las rutas se construyan con el prefijo gs://."""
        cfg = AppConfig(
            env="gcp",
            gcs_bucket="mi-bucket-analitica",
            gcp_project_id="sistema-predictivo-upao",
        )
        self.assertTrue(cfg.is_gcp)
        self.assertEqual(cfg.raw_base_path, "gs://mi-bucket-analitica/bronze")
        self.assertEqual(cfg.processed_base_path, "gs://mi-bucket-analitica/silver")
        self.assertEqual(cfg.curated_base_path, "gs://mi-bucket-analitica/gold")
        self.assertEqual(cfg.models_base_path, "gs://mi-bucket-analitica/modelos")
        self.assertEqual(cfg.get_bronze_path("senamhi"), "gs://mi-bucket-analitica/bronze/senamhi")
        self.assertEqual(cfg.get_silver_path("ira"), "gs://mi-bucket-analitica/silver/ira")
        self.assertEqual(cfg.get_gold_path(), "gs://mi-bucket-analitica/gold/epidemiologia_clima")
        self.assertEqual(cfg.get_model_path("rf_ira_model"), "gs://mi-bucket-analitica/modelos/rf_ira_model")

    def test_gcp_missing_bucket_raises_error(self):
        """Valida que levantar rutas GCP sin GCS_BUCKET genere un ValueError explícito."""
        cfg = AppConfig(env="gcp", gcs_bucket="")
        with self.assertRaises(ValueError):
            _ = cfg.raw_base_path

    def test_postgres_jdbc_url(self):
        """Verifica la generación de la URL JDBC y propiedades de Cloud SQL / PostgreSQL."""
        pg = PostgresConfig(
            host="10.0.0.5",
            port=5432,
            db="testdb",
            user="postgres",
            password="pwd",
        )
        self.assertEqual(pg.jdbc_url, "jdbc:postgresql://10.0.0.5:5432/testdb")
        self.assertEqual(pg.connection_properties["user"], "postgres")
        self.assertEqual(pg.connection_properties["driver"], "org.postgresql.Driver")


# ==============================================================================
# 2. PRUEBAS DE LOGGING
# ==============================================================================
class TestLogger(unittest.TestCase):
    def test_get_logger_creation(self):
        """Verifica la creación y nivel del logger unificado."""
        logger = get_logger("test_module", level="DEBUG")
        self.assertEqual(logger.name, "test_module")
        self.assertGreaterEqual(len(logger.handlers), 1)


# ==============================================================================
# 3. PRUEBAS DE SEMANA EPIDEMIOLÓGICA (MINSA / SENAMHI / ISO-8601)
# ==============================================================================
class TestEpiUtils(unittest.TestCase):
    def test_is_valid_semana(self):
        """Verifica el rango válido de semanas epidemiológicas (1 a 53)."""
        self.assertTrue(is_valid_semana_epidemiologica(1))
        self.assertTrue(is_valid_semana_epidemiologica(52))
        self.assertTrue(is_valid_semana_epidemiologica(53))
        self.assertFalse(is_valid_semana_epidemiologica(0))
        self.assertFalse(is_valid_semana_epidemiologica(54))
        self.assertFalse(is_valid_semana_epidemiologica(None))

    def test_calculate_epi_week_senamhi_alignment(self):
        """Verifica concordancia con los datos reales de SENAMHI 2015."""
        # 2015-01-01 (jueves) -> Semana 1
        self.assertEqual(calculate_epi_week("2015-01-01"), 1)
        # 2015-01-04 (domingo) -> Semana 1
        self.assertEqual(calculate_epi_week(date(2015, 1, 4)), 1)
        # 2015-01-05 (lunes) -> Semana 2
        self.assertEqual(calculate_epi_week("2015-01-05"), 2)
        # 2015-01-11 (domingo) -> Semana 2
        self.assertEqual(calculate_epi_week("2015-01-11"), 2)

    def test_calculate_epi_year_and_week(self):
        """Verifica el año y semana epidemiológica en fechas límite."""
        anio, semana = calculate_epi_year_and_week("2015-01-01")
        self.assertEqual(anio, 2015)
        self.assertEqual(semana, 1)

        # Final de año 2020 (año con 53 semanas)
        anio, semana = calculate_epi_year_and_week("2020-12-31")
        self.assertEqual(anio, 2020)
        self.assertEqual(semana, 53)


# ==============================================================================
# 4. PRUEBAS DE ESTANDARIZACIÓN DE UBIGEO Y FILTRADO LA LIBERTAD (13)
# ==============================================================================
class TestGeoUtils(unittest.TestCase):
    def test_clean_ubigeo_valid_formats(self):
        """Verifica normalización de formatos numéricos, texto y flotantes."""
        self.assertEqual(clean_ubigeo_str("130101"), "130101")
        self.assertEqual(clean_ubigeo_str(130101), "130101")
        self.assertEqual(clean_ubigeo_str("130101.0"), "130101")
        self.assertEqual(clean_ubigeo_str(130101.0), "130101")
        self.assertEqual(clean_ubigeo_str(" 130101 "), "130101")

    def test_clean_ubigeo_padding_and_truncation(self):
        """Verifica padding con ceros para longitudes menores y extracción a 6 dígitos."""
        # Padding si es menor de 6 dígitos
        self.assertEqual(clean_ubigeo_str("1301"), "001301")
        # Truncamiento de centro poblado (10 dígitos) a nivel distrital (6 dígitos)
        self.assertEqual(clean_ubigeo_str("1308090002"), "130809")

    def test_clean_ubigeo_null_and_empty(self):
        """Verifica que valores nulos o vacíos retornen None sin excepción."""
        self.assertIsNone(clean_ubigeo_str(None))
        self.assertIsNone(clean_ubigeo_str(""))
        self.assertIsNone(clean_ubigeo_str("   "))
        self.assertIsNone(clean_ubigeo_str("null"))
        self.assertIsNone(clean_ubigeo_str("NaN"))

    def test_is_ubigeo_la_libertad(self):
        """Verifica que solo los ubigeos que inicien con 13 sean reconocidos como La Libertad."""
        self.assertTrue(is_ubigeo_la_libertad("130101"))  # Trujillo
        self.assertTrue(is_ubigeo_la_libertad("130801"))  # Tayabamba
        self.assertFalse(is_ubigeo_la_libertad("150101"))  # Lima
        self.assertFalse(is_ubigeo_la_libertad("020101"))  # Áncash
        self.assertFalse(is_ubigeo_la_libertad(None))


if __name__ == "__main__":
    unittest.main()
