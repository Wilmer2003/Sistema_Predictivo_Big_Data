from src.common.config import AppConfig, SparkSettings
from src.common.spark_session import get_spark_session


def create_test_spark_session(app_name):
    config = AppConfig(
        env="local",
        spark=SparkSettings(
            master="local[1]",
            driver_memory="1g",
            executor_memory="1g",
            shuffle_partitions=1,
            log_level="ERROR",
        ),
    )
    return get_spark_session(
        app_name,
        config=config,
        extra_configs={"spark.jars.packages": "", "spark.ui.enabled": "false"},
    )
