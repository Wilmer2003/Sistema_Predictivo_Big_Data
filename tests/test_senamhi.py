"""
Pruebas Unitarias para la Limpieza de SENAMHI (src/preprocessing/limpiar_senamhi.py).
Valida construcción de fecha y semana epidemiológica, filtros de consistencia física
(PP >= 0, Tmin <= Tmax) y deduplicación por Estación + Fecha.
"""

import unittest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        DoubleType,
    )
    from src.preprocessing.limpiar_senamhi import transformar_datos_senamhi
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


@unittest.skipUnless(HAS_PYSPARK, "PySpark no está disponible en este entorno de ejecución.")
class TestLimpiezaSENAMHI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.spark = (
                SparkSession.builder
                .appName("Test_Limpieza_SENAMHI")
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

    def test_transformar_datos_senamhi_rangos_fisicos_y_fecha(self):
        """Verifica construcción de fecha, consistencia física y filtros."""
        schema = StructType([
            StructField("Departamento", StringType(), True),
            StructField("Estacion", StringType(), True),
            StructField("Ano", IntegerType(), True),
            StructField("Mes", IntegerType(), True),
            StructField("Dia", IntegerType(), True),
            StructField("Temperatura_maxima", DoubleType(), True),
            StructField("Temperatura_minima", DoubleType(), True),
            StructField("tc", DoubleType(), True),
            StructField("Precipitacion_acumulada", DoubleType(), True),
        ])

        datos = [
            # Registro válido normal
            ("LA LIBERTAD", "TRUJILLO", 2021, 5, 10, 26.5, 17.0, 21.0, 0.0),
            # Registro con inconsistencia física (Tmin > Tmax: 28.0 > 20.0 -> debe filtrarse)
            ("LA LIBERTAD", "TRUJILLO", 2021, 5, 11, 20.0, 28.0, 22.0, 0.0),
            # Registro con precipitación negativa (< 0 -> debe filtrarse)
            ("LA LIBERTAD", "TRUJILLO", 2021, 5, 12, 25.0, 16.0, 20.0, -5.0),
            # Registro fuera de rango temporal (2014 -> debe filtrarse)
            ("LA LIBERTAD", "HUAMACHUCO", 2014, 5, 10, 18.0, 8.0, 12.0, 2.5),
            # Registro duplicado exacto del primero (debe ser deduplicado)
            ("LA LIBERTAD", "TRUJILLO", 2021, 5, 10, 26.5, 17.0, 21.0, 0.0),
        ]

        df_in = self.spark.createDataFrame(datos, schema)
        df_out = transformar_datos_senamhi(df_in)

        # Solo debe quedar 1 registro válido
        self.assertEqual(df_out.count(), 1)
        row = df_out.first()
        self.assertEqual(row["fecha"], "2021-05-10")
        self.assertEqual(row["ano_epi"], 2021)
        # Validar consistencia
        self.assertLessEqual(row["Temperatura_minima"], row["Temperatura_maxima"])
        self.assertGreaterEqual(row["Precipitacion_acumulada"], 0.0)


if __name__ == "__main__":
    unittest.main()
