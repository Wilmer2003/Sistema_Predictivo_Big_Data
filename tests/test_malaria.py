"""
Pruebas Unitarias para la Limpieza de Malaria (src/preprocessing/limpiar_malaria.py).
Valida normalización de Ubigeo y localcod, filtro La Libertad (13),
diagnósticos B50/B51 y deduplicación.
"""

import unittest

from conftest import crear_spark_session

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
    )
    from src.preprocessing.limpiar_malaria import transformar_datos_malaria
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


@unittest.skipUnless(HAS_PYSPARK, "PySpark no está disponible en este entorno de ejecución.")
class TestLimpiezaMalaria(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = crear_spark_session("Test_Limpieza_Malaria")

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()

    def test_transformar_datos_malaria_filtros(self):
        """Verifica filtros de diagnósticos B50/B51, La Libertad y deduplicación."""
        schema = StructType([
            StructField("departamento", StringType(), True),
            StructField("provincia", StringType(), True),
            StructField("distrito", StringType(), True),
            StructField("localidad", StringType(), True),
            StructField("enfermedad", StringType(), True),
            StructField("ano", IntegerType(), True),
            StructField("semana", IntegerType(), True),
            StructField("diagnostic", StringType(), True),
            StructField("diresa", StringType(), True),
            StructField("ubigeo", StringType(), True),
            StructField("localcod", StringType(), True),
            StructField("edad", IntegerType(), True),
            StructField("tipo_edad", StringType(), True),
            StructField("sexo", StringType(), True),
            StructField("Malaria_total", IntegerType(), True),
            StructField("B50", IntegerType(), True),
            StructField("B51", IntegerType(), True),
        ])

        datos = [
            # Registro válido B51 (Pataz, La Libertad)
            ("LA LIBERTAD", "PATAZ", "PATAZ", "NIMPANA", "MALARIA", 2018, 12, "B51", "13", "130809", "1308090002", 25, "A", "M", 1, 0, 1),
            # Diagnóstico inválido (B52, debe descartarse)
            ("LA LIBERTAD", "PATAZ", "PATAZ", "NIMPANA", "MALARIA", 2018, 12, "B52", "13", "130809", "1308090002", 25, "A", "M", 1, 0, 0),
            # Departamento ajeno (Loreto, debe descartarse)
            ("LORETO", "MAYNAS", "IQUITOS", "PUNCHANA", "MALARIA", 2018, 12, "B51", "16", "160101", "1601010001", 30, "A", "F", 1, 0, 1),
            # Registro año 2012 (fuera de rango 2015-2024)
            ("LA LIBERTAD", "BOLIVAR", "BAMBAMARCA", "CALEMAR", "MALARIA", 2012, 5, "B51", "13", "130302", "1303020001", 18, "A", "F", 1, 0, 1),
        ]

        df_in = self.spark.createDataFrame(datos, schema)
        df_out = transformar_datos_malaria(df_in)

        # Solo debe quedar 1 registro (Pataz, B51, 2018)
        self.assertEqual(df_out.count(), 1)
        row = df_out.first()
        self.assertEqual(row["ubigeo"], "130809")
        self.assertEqual(row["diagnostic"], "B51")
        self.assertEqual(row["ano"], 2018)


if __name__ == "__main__":
    unittest.main()
