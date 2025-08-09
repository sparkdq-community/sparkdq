import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Provide a Spark session optimized for unit testing environments.

    Configures a local Spark session with minimal resource allocation and
    reduced parallelism to ensure fast, deterministic test execution while
    maintaining compatibility with PySpark testing utilities.
    """
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
