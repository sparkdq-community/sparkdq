import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.date_checks.date_max_check import DateMaxCheck, DateMaxCheckConfig
from sparkdq.exceptions import MissingColumnError


def test_date_max_check_config_valid() -> None:
    """
    Validates that DateMaxCheckConfig correctly instantiates a DateMaxCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, and max_value.
    """
    config = DateMaxCheckConfig(
        check_id="check_max_date",
        columns=["record_date"],
        max_value="2023-12-31",
    )
    check = config.to_check()

    assert isinstance(check, DateMaxCheck)
    assert check.check_id == "check_max_date"
    assert check.columns == ["record_date"]
    assert check.max_value == "2023-12-31"


def test_date_max_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that DateMaxCheck flags rows where the date column value exceeds max_value.

    A row is marked as failed if the 'record_date' is after '2023-12-31'.
    """
    data = [
        Row(record_date="2023-12-30"),
        Row(record_date="2023-12-31"),
        Row(record_date="2024-01-01"),
    ]
    schema = StructType([StructField("record_date", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("record_date", to_date("record_date"))

    config = DateMaxCheckConfig(
        check_id="date_max_check",
        columns=["record_date"],
        max_value="2023-12-31",
    )
    check = config.to_check()
    result_df = check.validate(df)

    expected_data = [
        ("2023-12-30", False),
        ("2023-12-31", True),
        ("2024-01-01", True),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["record_date", "date_max_check"])
    expected_df = expected_raw.withColumn("record_date", to_date("record_date"))

    assertDataFrameEqual(result_df, expected_df)


def test_date_max_check_validate_inclusive_true(spark: SparkSession) -> None:
    """
    Verifies that DateMaxCheck with inclusive=True includes the boundary date as valid.

    A row is marked as failed only if the 'record_date' is after '2023-12-31'.
    """
    data = [
        Row(record_date="2023-12-30"),
        Row(record_date="2023-12-31"),
        Row(record_date="2024-01-01"),
    ]
    schema = StructType([StructField("record_date", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("record_date", to_date("record_date"))

    config = DateMaxCheckConfig(
        check_id="date_max_inclusive_check",
        columns=["record_date"],
        max_value="2023-12-31",
        inclusive=True,
    )
    check = config.to_check()
    result_df = check.validate(df)

    expected_data = [
        ("2023-12-30", False),
        ("2023-12-31", False),
        ("2024-01-01", True),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["record_date", "date_max_inclusive_check"])
    expected_df = expected_raw.withColumn("record_date", to_date("record_date"))

    assertDataFrameEqual(result_df, expected_df)


def test_date_max_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that DateMaxCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = DateMaxCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        max_value="2023-12-31",
    )
    check = config.to_check()

    with pytest.raises(MissingColumnError):
        check.validate(df)
