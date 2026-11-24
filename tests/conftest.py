import os
import sys

# Usar el mismo intérprete de Python para el driver y los workers de Spark.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def crear_spark_session(app_name):
    """Crea una sesión Spark con la configuración común de las pruebas."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark
