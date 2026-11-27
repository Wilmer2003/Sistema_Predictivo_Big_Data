"""
Pruebas Unitarias para el Módulo de Serving (src/serving/).
Valida la sintaxis e integridad del esquema DDL (schema.sql), configuración JDBC
y manejo robusto de excepciones de conectividad para Amazon RDS / PostgreSQL.
"""

from pathlib import Path
import unittest

from src.common.config import AppConfig, PostgresConfig
from src.serving.cargar_postgres import probar_conexion_postgres


class TestServing(unittest.TestCase):
    def test_schema_sql_file_exists_and_valid(self):
        """Verifica que el archivo schema.sql contenga las tablas del modelo dimensional y la tabla lotes."""
        schema_path = Path(__file__).resolve().parent.parent / "src" / "serving" / "schema.sql"
        self.assertTrue(schema_path.is_file(), f"No se encontró {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            ddl_content = f.read().upper()

        # Validar existencia de la tabla de auditoría de lotes de Amazon RDS
        self.assertIn("CREATE TABLE IF NOT EXISTS PUBLIC.LOTES", ddl_content)
        self.assertIn("CREATE INDEX IF NOT EXISTS IDX_LOTES_FILE_HASH", ddl_content)

        # Validar existencia de las tablas del esquema en estrella
        self.assertIn("CREATE TABLE IF NOT EXISTS DIM_UBIGEO", ddl_content)
        self.assertIn("CREATE TABLE IF NOT EXISTS DIM_TIEMPO", ddl_content)
        self.assertIn("CREATE TABLE IF NOT EXISTS FACT_VIGILANCIA_CLIMA", ddl_content)
        self.assertIn("CREATE TABLE IF NOT EXISTS PREDICCIONES_VIGILANCIA", ddl_content)

        self.assertIn("CREATE TABLE IF NOT EXISTS PUBLIC.METRICAS_EVALUACION", ddl_content)
        self.assertIn("RUN_ID VARCHAR(100) PRIMARY KEY", ddl_content)

        # Validar claves foráneas
        self.assertIn("REFERENCES DIM_UBIGEO", ddl_content)
        self.assertIn("REFERENCES DIM_TIEMPO", ddl_content)

        # Validar índices de rendimiento
        self.assertIn("CREATE INDEX IF NOT EXISTS IDX_FACT_UBIGEO", ddl_content)
        self.assertIn("CREATE INDEX IF NOT EXISTS IDX_FACT_TIEMPO", ddl_content)

    def test_postgres_config_and_jdbc_properties(self):
        """Verifica la construcción de la URL JDBC y propiedades para Spark / Amazon RDS PostgreSQL."""
        pg = PostgresConfig(
            host="10.10.0.5",
            port=5432,
            db="epidemiologia_db",
            user="postgres",
            password="pwd_test",
        )
        self.assertEqual(pg.jdbc_url, "jdbc:postgresql://10.10.0.5:5432/epidemiologia_db")
        self.assertEqual(pg.connection_properties["user"], "postgres")
        self.assertEqual(pg.connection_properties["password"], "pwd_test")
        self.assertEqual(pg.connection_properties["driver"], "org.postgresql.Driver")

    def test_probar_conexion_unreachable_host(self):
        """Verifica que un intento de conexión hacia un host inalcanzable no lance excepción no controlada."""
        cfg = AppConfig(
            postgres=PostgresConfig(host="192.0.2.1", port=5432)  # IP no enrutable (TEST-NET-1)
        )
        resultado = probar_conexion_postgres(cfg)
        self.assertFalse(resultado)


if __name__ == "__main__":
    unittest.main()
