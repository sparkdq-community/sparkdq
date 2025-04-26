import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Creates a spark session that is optimized for unit tests."""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark

    spark.stop()
