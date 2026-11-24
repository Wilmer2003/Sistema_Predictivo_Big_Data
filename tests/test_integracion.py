"""
Pruebas Unitarias para el Módulo de Integración y Capa Gold (src/integration/).
Valida mapeos de estaciones SENAMHI, agregaciones semanales, cálculo de lags
y construcción del modelo dimensional.
"""

import unittest

from conftest import crear_spark_session

from src.integration.dimensional_model import ESTACION_UBIGEO_MAP

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        DoubleType,
    )
    from src.integration.feature_engineering import calcular_lags_temporales
    from src.integration.integrar_datasets import (
        agregar_ira_semanal,
        agregar_senamhi_semanal,
        integrar_fuentes_silver,
        aplicar_feature_engineering_completo,
    )
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


class TestIntegracionLogica(unittest.TestCase):
    def test_estacion_ubigeo_map_validity(self):
        """Verifica que todas las estaciones de SENAMHI mapeen a Ubigeos de La Libertad (13) de 6 dígitos."""
        for estacion, ubigeo in ESTACION_UBIGEO_MAP.items():
            self.assertTrue(
                ubigeo.startswith("13"),
                f"La estación '{estacion}' mapea a un ubigeo fuera de La Libertad: {ubigeo}",
            )
            self.assertEqual(
                len(ubigeo),
                6,
                f"El ubigeo para la estación '{estacion}' no tiene 6 dígitos: {ubigeo}",
            )


@unittest.skipUnless(HAS_PYSPARK, "PySpark no está disponible en este entorno de ejecución.")
class TestIntegracionPySpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = crear_spark_session("Test_Integracion_Gold")

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()

    def test_lags_temporales_calculation(self):
        """Verifica que el cálculo de lags desplace correctamente los valores por semana."""
        schema = StructType([
            StructField("ubigeo", StringType(), True),
            StructField("ano", IntegerType(), True),
            StructField("semana_epidemiologica", IntegerType(), True),
            StructField("casos", IntegerType(), True),
        ])

        datos = [
            ("130101", 2020, 1, 10),
            ("130101", 2020, 2, 20),
            ("130101", 2020, 3, 30),
            ("130101", 2020, 4, 40),
        ]

        df_in = self.spark.createDataFrame(datos, schema)
        df_lags = calcular_lags_temporales(df_in, columnas_a_rezagar=["casos"], max_lag=3)

        filas = df_lags.orderBy("semana_epidemiologica").collect()

        # Semana 1: no tiene lags previos (None)
        self.assertIsNone(filas[0]["casos_lag1"])
        self.assertIsNone(filas[0]["casos_lag2"])
        self.assertIsNone(filas[0]["casos_lag3"])

        # Semana 2: lag1 = 10
        self.assertEqual(filas[1]["casos_lag1"], 10)
        self.assertIsNone(filas[1]["casos_lag2"])

        # Semana 3: lag1 = 20, lag2 = 10
        self.assertEqual(filas[2]["casos_lag1"], 20)
        self.assertEqual(filas[2]["casos_lag2"], 10)

        # Semana 4: lag1 = 30, lag2 = 20, lag3 = 10
        self.assertEqual(filas[3]["casos_lag1"], 30)
        self.assertEqual(filas[3]["casos_lag2"], 20)
        self.assertEqual(filas[3]["casos_lag3"], 10)


if __name__ == "__main__":
    unittest.main()
