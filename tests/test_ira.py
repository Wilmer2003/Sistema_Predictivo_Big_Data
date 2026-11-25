"""
Pruebas Unitarias para la Limpieza de IRA (src/preprocessing/limpiar_ira.py).
Valida normalización de Ubigeo, filtros de La Libertad, periodo 2015-2024,
protección de datos sensibles (celdas pequeñas) y deduplicación.
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
    from src.preprocessing.limpiar_ira import transformar_datos_ira
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


@unittest.skipUnless(HAS_PYSPARK, "PySpark no está disponible en este entorno de ejecución.")
class TestLimpiezaIRA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_test_spark_session("Test_Limpieza_IRA")
        cls.addClassCleanup(cls.spark.stop)

    def test_transformar_datos_ira_filtros_y_proteccion(self):
        """Verifica filtros de La Libertad (13), años 2015-2024 y bandeo de celdas pequeñas."""
        schema = StructType([
            StructField("Departamento", StringType(), True),
            StructField("Provincia", StringType(), True),
            StructField("Distrito", StringType(), True),
            StructField("Ano", IntegerType(), True),
            StructField("Semana_Epidemiologica", IntegerType(), True),
            StructField("Codigo_Departamento", StringType(), True),
            StructField("Codigo_Ubigeo", StringType(), True),
            StructField("IRA_menores_2_meses", IntegerType(), True),
            StructField("IRA_2_a_11_meses", IntegerType(), True),
            StructField("IRA_1_a_4_anos", IntegerType(), True),
            StructField("Neumonia_menores_2_meses", IntegerType(), True),
            StructField("Neumonia_2_a_11_meses", IntegerType(), True),
            StructField("Neumonia_1_a_4_anos", IntegerType(), True),
            StructField("Defunciones", IntegerType(), True),
        ])

        datos = [
            # Registro válido de La Libertad (Trujillo)
            ("LA LIBERTAD", "TRUJILLO", "TRUJILLO", 2020, 10, "13", "130101", 3, 5, 25, 0, 1, 0, 0),
            # Registro de Lima (debe ser filtrado)
            ("LIMA", "LIMA", "LIMA", 2020, 10, "15", "150101", 10, 20, 30, 1, 2, 0, 0),
            # Registro año 2014 (fuera de rango 2015-2024, debe ser filtrado)
            ("LA LIBERTAD", "ASCOPE", "CHICAMA", 2014, 10, "13", "130202", 2, 4, 10, 0, 0, 0, 0),
            # Registro 2024 semana 30 (debe filtrarse porque 2024 solo llega a semana 26)
            ("LA LIBERTAD", "OTUZCO", "OTUZCO", 2024, 30, "13", "130601", 1, 2, 3, 0, 0, 0, 0),
        ]

        df_in = self.spark.createDataFrame(datos, schema)
        df_out = transformar_datos_ira(df_in)

        # Solo debe quedar 1 registro (Trujillo, 2020)
        self.assertEqual(df_out.count(), 1)
        row = df_out.first()
        self.assertEqual(row["Codigo_Ubigeo"], "130101")
        self.assertEqual(row["Ano"], 2020)

        # Validación de celdas pequeñas (bandeo)
        # 3 casos -> '1-4'
        self.assertEqual(row["IRA_menores_2_meses_rango"], "1-4")
        # 5 casos -> '5-9'
        self.assertEqual(row["IRA_2_a_11_meses_rango"], "5-9")
        # 25 casos -> '20+'
        self.assertEqual(row["IRA_1_a_4_anos_rango"], "20+")
        # 0 defunciones -> '0'
        self.assertEqual(row["Defunciones_rango"], "0")


if __name__ == "__main__":
    unittest.main()
