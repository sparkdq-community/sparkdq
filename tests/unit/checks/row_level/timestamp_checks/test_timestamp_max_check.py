import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.timestamp_checks.timestamp_max_check import (
    TimestampMaxCheck,
    TimestampMaxCheckConfig,
)
from sparkdq.exceptions import MissingColumnError


def test_timestamp_max_check_config_valid() -> None:
    """
    Validates that TimestampMaxCheckConfig correctly instantiates a TimestampMaxCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, and max_value.
    """
    config = TimestampMaxCheckConfig(
        check_id="check_max_timestamp",
        columns=["event_time"],
        max_value="2023-12-31 23:59:59",
    )
    check = config.to_check()

    assert isinstance(check, TimestampMaxCheck)
    assert check.check_id == "check_max_timestamp"
    assert check.columns == ["event_time"]
    assert check.max_value == "2023-12-31 23:59:59"


def test_timestamp_max_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that TimestampMaxCheck flags rows where the timestamp column value exceeds max_value.

    A row is marked as failed if the 'event_time' is after '2023-12-31 23:59:59'.
    """
    data = [
        Row(event_time="2023-12-31 23:59:58"),
        Row(event_time="2023-12-31 23:59:59"),
        Row(event_time="2024-01-01 00:00:00"),
    ]
    schema = StructType([StructField("event_time", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("event_time", to_timestamp("event_time"))

    config = TimestampMaxCheckConfig(
        check_id="timestamp_max_check",
        columns=["event_time"],
        max_value="2023-12-31 23:59:59",
    )
    check = config.to_check()
    result_df = check.validate(df)

    expected_data = [
        ("2023-12-31 23:59:58", False),
        ("2023-12-31 23:59:59", True),
        ("2024-01-01 00:00:00", True),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["event_time", "timestamp_max_check"])
    expected_df = expected_raw.withColumn("event_time", to_timestamp("event_time"))

    assertDataFrameEqual(result_df, expected_df)


def test_timestamp_max_check_validate_inclusive_true(spark: SparkSession) -> None:
    """
    Verifies that TimestampMaxCheck correctly respects inclusive=True.

    A row is marked as failed only if the timestamp is strictly greater than max_value.
    """
    data = [
        Row(event_time="2023-12-31 23:59:58"),  # should pass
        Row(event_time="2023-12-31 23:59:59"),  # should pass
        Row(event_time="2024-01-01 00:00:00"),  # should fail
    ]
    schema = StructType([StructField("event_time", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("event_time", to_timestamp("event_time"))

    config = TimestampMaxCheckConfig(
        check_id="timestamp_max_inclusive",
        columns=["event_time"],
        max_value="2023-12-31 23:59:59",
        inclusive=True,
    )
    check = config.to_check()
    result_df = check.validate(df)

    expected_data = [
        ("2023-12-31 23:59:58", False),
        ("2023-12-31 23:59:59", False),
        ("2024-01-01 00:00:00", True),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["event_time", "timestamp_max_inclusive"])
    expected_df = expected_raw.withColumn("event_time", to_timestamp("event_time"))

    assertDataFrameEqual(result_df, expected_df)


def test_timestamp_max_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that TimestampMaxCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = TimestampMaxCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        max_value="2023-12-31 23:59:59",
    )
    check = config.to_check()

    with pytest.raises(MissingColumnError):
        check.validate(df)
