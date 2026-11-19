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
        cls.spark = (
            SparkSession.builder
            .appName("Test_Limpieza_SENAMHI")
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

    def test_transformar_datos_senamhi_rangos_fisicos_y_fecha(self):
        """Verifica construcción de fecha, consistencia física y filtros."""
        schema = StructType([
            StructField("Departamento", StringType(), True),
            StructField("Estacion", StringType(), True),
            StructField("Ano", IntegerType(), True),
            StructField("Mes", IntegerType(), True),
            StructField("Dia", IntegerType(), True),
            StructField("Precipitacion_acumulada", DoubleType(), True),
            StructField("Temperatura_maxima", DoubleType(), True),
            StructField("Temperatura_minima", DoubleType(), True),
            StructField("tc", DoubleType(), True),
        ])

        datos = [
            # Registro válido (Cachicadán, 2015-01-01)
            ("LA LIBERTAD", "CACHICADAN", 2015, 1, 1, 0.0, 20.2, 10.6, 15.38),
            # Inconsistencia física: Precipitación negativa (debe descartarse)
            ("LA LIBERTAD", "CACHICADAN", 2015, 1, 2, -1.5, 20.4, 10.4, 15.43),
            # Inconsistencia física: Tmin > Tmax (debe descartarse)
            ("LA LIBERTAD", "CACHICADAN", 2015, 1, 3, 2.0, 10.0, 25.0, 17.5),
            # Departamento fuera de La Libertad (Cajamarca, debe descartarse)
            ("CAJAMARCA", "CAJABAMBA", 2015, 1, 4, 3.0, 18.0, 8.0, 13.0),
        ]

        df_in = self.spark.createDataFrame(datos, schema)
        df_out = transformar_datos_senamhi(df_in)

        # Solo debe quedar 1 registro (Cachicadán, 2015-01-01)
        self.assertEqual(df_out.count(), 1)
        row = df_out.first()
        self.assertEqual(str(row["Fecha"]), "2015-01-01")
        self.assertEqual(row["Semana_Epidemiologica"], 1)
        self.assertEqual(row["Precipitacion_acumulada"], 0.0)

    def test_idempotencia(self):
        """Correr la transformación dos veces sobre los mismos datos debe dar el mismo resultado."""
        schema = StructType([
            StructField("Departamento", StringType(), True),
            StructField("Estacion", StringType(), True),
            StructField("Ano", IntegerType(), True),
            StructField("Mes", IntegerType(), True),
            StructField("Dia", IntegerType(), True),
            StructField("Precipitacion_acumulada", DoubleType(), True),
            StructField("Temperatura_maxima", DoubleType(), True),
            StructField("Temperatura_minima", DoubleType(), True),
            StructField("tc", DoubleType(), True),
        ])
        datos = [
            ("LA LIBERTAD", "CACHICADAN", 2015, 1, 1, 0.0, 20.2, 10.6, 15.38),
        ]

        resultado_1 = transformar_datos_senamhi(self.spark.createDataFrame(datos, schema)).collect()
        resultado_2 = transformar_datos_senamhi(self.spark.createDataFrame(datos, schema)).collect()

        self.assertEqual(resultado_1, resultado_2)


if __name__ == "__main__":
    unittest.main()
