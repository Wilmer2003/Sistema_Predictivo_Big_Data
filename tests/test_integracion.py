"""
Pruebas Unitarias para el Módulo de Integración y Capa Gold (src/integration/).
Valida mapeos de estaciones SENAMHI, agregaciones semanales, cálculo de lags
y construcción del modelo dimensional.
"""

import unittest

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
        resumir_cobertura_climatica,
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
        cls.spark = (
            SparkSession.builder
            .appName("Test_Integracion_Gold")
            .master("local[1]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

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

    def test_lags_no_usan_valores_de_semanas_futuras(self):
        """Un valor alto futuro no debe aparecer como rezago de una semana anterior."""
        schema = StructType([
            StructField("ubigeo", StringType(), True),
            StructField("ano", IntegerType(), True),
            StructField("semana_epidemiologica", IntegerType(), True),
            StructField("casos", IntegerType(), True),
        ])
        datos = [
            ("130101", 2020, 1, 10),
            ("130101", 2020, 2, 20),
            ("130101", 2020, 3, 99999),
        ]

        filas = calcular_lags_temporales(
            self.spark.createDataFrame(datos, schema),
            columnas_a_rezagar=["casos"],
            max_lag=1,
        ).orderBy("semana_epidemiologica").collect()

        self.assertIsNone(filas[0]["casos_lag1"])
        self.assertEqual(filas[1]["casos_lag1"], 10)
        self.assertEqual(filas[2]["casos_lag1"], 20)

    def test_clima_se_alinea_por_ano_y_semana_y_resume_cobertura(self):
        """Evita cruces entre años/semanas y mide la cobertura regional por distrito-semana."""
        ira_schema = StructType([
            StructField("Codigo_Ubigeo", StringType(), True),
            StructField("Ano", IntegerType(), True),
            StructField("Semana_Epidemiologica", IntegerType(), True),
            StructField("IRA_menores_2_meses", IntegerType(), True),
            StructField("IRA_2_a_11_meses", IntegerType(), True),
            StructField("IRA_1_a_4_anos", IntegerType(), True),
            StructField("Neumonia_menores_2_meses", IntegerType(), True),
            StructField("Neumonia_2_a_11_meses", IntegerType(), True),
            StructField("Neumonia_1_a_4_anos", IntegerType(), True),
            StructField("Defunciones", IntegerType(), True),
        ])
        ira = self.spark.createDataFrame(
            [
                ("130101", 2020, 1, 1, 0, 0, 0, 0, 0, 0),
                ("130101", 2021, 1, 1, 0, 0, 0, 0, 0, 0),
                ("130101", 2022, 1, 1, 0, 0, 0, 0, 0, 0),
            ],
            ira_schema,
        )
        malaria_schema = StructType([
            StructField("ubigeo", StringType(), True),
            StructField("ano", IntegerType(), True),
            StructField("semana", IntegerType(), True),
            StructField("Malaria_total", IntegerType(), True),
            StructField("B50", IntegerType(), True),
            StructField("B51", IntegerType(), True),
        ])
        malaria = self.spark.createDataFrame([], malaria_schema)
        clima_schema = StructType([
            StructField("Ano", IntegerType(), True),
            StructField("Semana_Epidemiologica", IntegerType(), True),
            StructField("Temperatura_maxima", DoubleType(), True),
            StructField("Temperatura_minima", DoubleType(), True),
            StructField("tc", DoubleType(), True),
            StructField("Precipitacion_acumulada", DoubleType(), True),
        ])
        clima = self.spark.createDataFrame(
            [
                (2020, 1, 20.0, 10.0, 10.0, 0.0),
                (2021, 1, 30.0, 20.0, 20.0, 1.0),
                (2022, 2, 40.0, 30.0, 99.0, 2.0),
            ],
            clima_schema,
        )

        integrado = integrar_fuentes_silver(ira, malaria, clima)
        temperaturas = {
            (fila["ano"], fila["semana_epidemiologica"]): fila["temp_media_promedio"]
            for fila in integrado.collect()
        }
        self.assertEqual(temperaturas[(2020, 1)], 10.0)
        self.assertEqual(temperaturas[(2021, 1)], 20.0)
        self.assertIsNone(temperaturas[(2022, 1)])
        self.assertEqual(
            resumir_cobertura_climatica(integrado),
            {
                "total_distrito_semana": 3,
                "distrito_semana_con_clima": 2,
                "porcentaje_con_clima": 66.67,
            },
        )


if __name__ == "__main__":
    unittest.main()
