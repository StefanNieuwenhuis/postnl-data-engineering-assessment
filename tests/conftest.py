from typing import Any, Generator

import pytest

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, Any, None]:
    """
    Session-scoped SparkSession
    :return: SparkSession
    """

    builder = (
        SparkSession.builder.appName("postnl-de-assessment-unit-tests")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()