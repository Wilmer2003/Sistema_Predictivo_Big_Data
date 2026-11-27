"""
Pruebas Unitarias para el Módulo de Serving (src/serving/).
Valida la sintaxis e integridad del esquema DDL (schema.sql), configuración JDBC
y manejo robusto de excepciones de conectividad para Amazon RDS / PostgreSQL.
"""

from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from src.common.config import AppConfig, PostgresConfig
from src.modeling.predict import guardar_predicciones
from src.serving.cargar_postgres import (
    cargar_capa_gold_a_postgres,
    cargar_dataframe_a_postgres,
    main,
    probar_conexion_postgres,
)


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

    @patch("psycopg2.connect", side_effect=OSError("Connection refused"))
    def test_probar_conexion_unreachable_host(self, connect):
        """Verifica que un intento de conexión hacia un host inalcanzable no lance excepción no controlada."""
        cfg = AppConfig(
            postgres=PostgresConfig(host="192.0.2.1", port=5432)  # IP no enrutable (TEST-NET-1)
        )
        resultado = probar_conexion_postgres(cfg)
        self.assertFalse(resultado)
        connect.assert_called_once()

    @patch("src.serving.cargar_postgres.cargar_dataframe_a_postgres", return_value=True)
    def test_cargar_predicciones_s3_y_validar_join_temporal(self, cargar):
        cfg = AppConfig(env="aws", s3_bucket="bucket-test")
        ruta_gold = "s3://bucket-test/gold/epidemiologia_clima"
        ruta_preds = f"{ruta_gold}/predicciones_vigilancia"
        spark = MagicMock()
        tiempo = MagicMock()
        preds = MagicMock()
        join = preds.join.return_value
        preds.count.return_value = 2
        spark.read.parquet.side_effect = {
            f"{ruta_gold}/dim_ubigeo": MagicMock(),
            f"{ruta_gold}/dim_tiempo": tiempo,
            f"{ruta_gold}/fact_vigilancia": MagicMock(),
            ruta_preds: preds,
        }.__getitem__

        self.assertEqual(guardar_predicciones(preds, config=cfg), ruta_preds)
        preds.write.mode.return_value.parquet.assert_called_once_with(ruta_preds)

        for sin_tiempo, total_join, exito in [(0, 2, True), (1, 2, False), (0, 3, False)]:
            with self.subTest(sin_tiempo=sin_tiempo, total_join=total_join):
                cargar.reset_mock()
                join.filter.return_value.count.return_value = sin_tiempo
                join.count.return_value = total_join
                resultados = cargar_capa_gold_a_postgres(spark, cfg)
                self.assertEqual(resultados["predicciones_vigilancia"], exito)
                self.assertEqual(cargar.call_count, 4 if exito else 3)
                if exito:
                    cargar.assert_called_with(join, "predicciones_vigilancia", modo="append", config=cfg)

        tiempo.select.assert_called_with("tiempo_id", "ano", "semana_epidemiologica")
        preds.join.assert_called_with(
            tiempo.select.return_value, on=["ano", "semana_epidemiologica"], how="left"
        )

    @patch("psycopg2.extras.execute_values")
    @patch("src.serving.cargar_postgres._conectar")
    def test_insertar_por_clave_y_cerrar_transaccion_si_falla(self, conectar, insertar):
        conn = conectar.return_value
        conn.__exit__.return_value = False
        df = MagicMock(columns=["ubigeo", "tiempo_id"])
        df.toLocalIterator.return_value = iter([("130101", 202401)])
        cfg = AppConfig(postgres=PostgresConfig(schema="vigilancia"))
        insertar.side_effect = ValueError("fila inválida")

        self.assertFalse(cargar_dataframe_a_postgres(df, "predicciones_vigilancia", config=cfg))
        _, consulta, filas = insertar.call_args.args
        self.assertIn("ON CONFLICT", repr(consulta))
        self.assertIn("DO NOTHING", repr(consulta))
        self.assertIn("vigilancia", repr(consulta))
        self.assertEqual(list(filas), [("130101", 202401)])
        self.assertIs(conn.__exit__.call_args.args[0], ValueError)
        conn.close.assert_called_once()

    @patch("src.serving.cargar_postgres.cargar_capa_gold_a_postgres")
    @patch("sys.argv", ["cargar_postgres", "--cargar"])
    def test_carga_parcial_devuelve_error_en_cli(self, cargar):
        cargar.return_value = {"dim_tiempo": True, "predicciones_vigilancia": False}
        self.assertEqual(main(), 1)


if __name__ == "__main__":
    unittest.main()
