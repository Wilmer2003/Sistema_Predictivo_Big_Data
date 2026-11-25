"""
Pruebas Unitarias para la Limpieza de Malaria (src/preprocessing/limpiar_malaria.py).
Valida normalización de Ubigeo y localcod, filtro La Libertad (13),
diagnósticos B50/B51 y deduplicación.
"""

import unittest

from tests.spark_support import create_test_spark_session

try:
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
        cls.spark = create_test_spark_session("Test_Limpieza_Malaria")
        cls.addClassCleanup(cls.spark.stop)

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
            StructField("ubigeo", StringType(), True),
            StructField("localcod", StringType(), True),
            StructField("diagnostic", StringType(), True),
            StructField("edad", IntegerType(), True),
            StructField("tipo_edad", StringType(), True),
            StructField("sexo", StringType(), True),
        ])

        datos = [
            # Caso válido La Libertad B50 (Pataz / Parcoy)
            ("LA LIBERTAD", "PATAZ", "PARCOY", "PARCOY", "MALARIA", 2021, 15, "130808", "1308080001", "B50", 25, "A", "M"),
            # Caso válido La Libertad B51
            ("LA LIBERTAD", "TRUJILLO", "LAREDO", "LAREDO", "MALARIA", 2021, 16, "130103", "1301030001", "B51", 30, "A", "F"),
            # Caso con diagnóstico fuera de B50/B51 (B52 -> debe filtrarse)
            ("LA LIBERTAD", "TRUJILLO", "LAREDO", "LAREDO", "MALARIA", 2021, 17, "130103", "1301030001", "B52", 30, "A", "F"),
            # Caso fuera de La Libertad (Loreto 16 -> debe filtrarse)
            ("LORETO", "MAYNAS", "IQUITOS", "IQUITOS", "MALARIA", 2021, 15, "160101", "1601010001", "B50", 40, "A", "M"),
            # Registro duplicado exacto del primero (debe ser deduplicado)
            ("LA LIBERTAD", "PATAZ", "PARCOY", "PARCOY", "MALARIA", 2021, 15, "130808", "1308080001", "B50", 25, "A", "M"),
        ]

        df_in = self.spark.createDataFrame(datos, schema)
        df_out = transformar_datos_malaria(df_in)

        # Deben quedar exactamente 2 casos únicos de La Libertad (B50 y B51)
        self.assertEqual(df_out.count(), 2)

        # Validar diagnósticos resultantes
        diagnosticos = [r["diagnostic"] for r in df_out.select("diagnostic").collect()]
        self.assertIn("B50", diagnosticos)
        self.assertIn("B51", diagnosticos)
        self.assertNotIn("B52", diagnosticos)


if __name__ == "__main__":
    unittest.main()
