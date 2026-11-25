"""
Pruebas Unitarias para el Módulo de Modelado Predictivo (src/modeling/).
Cubre definición de variables, métricas de regresión (RMSE, MAE, R2),
baseline de persistencia y división temporal.
"""

import unittest
from src.modeling.pipeline import DEFAULT_FEATURE_COLS, TARGET_COL_IRA, TARGET_COL_MALARIA
from src.modeling.baseline import calcular_metricas_pure_python

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from src.modeling.pipeline import construir_pipeline_mllib
    from src.modeling.train import division_temporal_train_test
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


class TestModelingLogica(unittest.TestCase):
    def test_default_features_definition(self):
        """Verifica que las variables predictoras incluyan clima y lags epidemiológicos."""
        self.assertIn("temp_media_promedio_lag1", DEFAULT_FEATURE_COLS)
        self.assertIn("precipitacion_total_lag1", DEFAULT_FEATURE_COLS)
        self.assertIn("ira_total_menores_5_lag1", DEFAULT_FEATURE_COLS)
        self.assertEqual(TARGET_COL_IRA, "ira_total_menores_5")
        self.assertEqual(TARGET_COL_MALARIA, "malaria_casos_total")

    def test_calcular_metricas_pure_python_exact_match(self):
        """Verifica métricas perfectas (RMSE=0, MAE=0, R2=1.0)."""
        y_true = [10.0, 20.0, 30.0, 40.0]
        y_pred = [10.0, 20.0, 30.0, 40.0]
        m = calcular_metricas_pure_python(y_true, y_pred)
        self.assertEqual(m["rmse"], 0.0)
        self.assertEqual(m["mae"], 0.0)
        self.assertEqual(m["r2"], 1.0)

    def test_calcular_metricas_pure_python_known_error(self):
        """Verifica el cálculo de error cuadrático y absoluto con valores conocidos."""
        y_true = [10.0, 20.0, 30.0]
        y_pred = [12.0, 18.0, 32.0]  # Errores: +2, -2, +2
        m = calcular_metricas_pure_python(y_true, y_pred)
        self.assertEqual(m["mae"], 2.0)
        self.assertEqual(m["rmse"], 2.0)
        self.assertGreater(m["r2"], 0.9)

    def test_calcular_metricas_mismatched_length_raises(self):
        """Verifica que listas de longitud desigual levanten ValueError."""
        with self.assertRaises(ValueError):
            calcular_metricas_pure_python([1.0, 2.0], [1.0])


@unittest.skipUnless(HAS_PYSPARK, "PySpark no está disponible en este entorno de ejecución.")
class TestModelingPySpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.spark = (
                SparkSession.builder
                .appName("Test_Modeling")
                .master("local[1]")
                .config("spark.driver.memory", "1g")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            cls.spark.sparkContext.setLogLevel("ERROR")
        except Exception as e:
            raise unittest.SkipTest(f"Java Gateway de PySpark no pudo inicializarse: {e}")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark") and cls.spark:
            cls.spark.stop()

    def test_division_temporal_train_test(self):
        """Verifica que la división temporal separe estrictamente por año."""
        schema = StructType([
            StructField("ano", IntegerType(), True),
            StructField("valor", IntegerType(), True),
        ])
        datos = [
            (2018, 10),
            (2020, 20),
            (2022, 30),
            (2023, 40),
            (2024, 50),
        ]
        df = self.spark.createDataFrame(datos, schema)
        train_df, test_df = division_temporal_train_test(df, split_year=2022)

        self.assertEqual(train_df.count(), 3)
        self.assertEqual(test_df.count(), 2)

        anos_train = [r["ano"] for r in train_df.collect()]
        anos_test = [r["ano"] for r in test_df.collect()]

        self.assertTrue(all(a <= 2022 for a in anos_train))
        self.assertTrue(all(a > 2022 for a in anos_test))


if __name__ == "__main__":
    unittest.main()
