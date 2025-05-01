import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.timestamp_checks.timestamp_between_check import (
    TimestampBetweenCheck,
    TimestampBetweenCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError


def test_timestamp_between_check_config_valid() -> None:
    """
    Validates that TimestampBetweenCheckConfig correctly instantiates a TimestampBetweenCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, min_value, and max_value.
    """
    config = TimestampBetweenCheckConfig(
        check_id="check_timestamp_range",
        columns=["event_time"],
        min_value="2020-01-01 00:00:00",
        max_value="2023-12-31 23:59:59",
    )
    check = config.to_check()

    assert isinstance(check, TimestampBetweenCheck)
    assert check.check_id == "check_timestamp_range"
    assert check.columns == ["event_time"]
    assert check.min_value == "2020-01-01 00:00:00"
    assert check.max_value == "2023-12-31 23:59:59"


def test_timestamp_between_check_config_invalid_range() -> None:
    """
    Validates that TimestampBetweenCheckConfig raises an InvalidCheckConfigurationError
    when min_value is greater than max_value.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        TimestampBetweenCheckConfig(
            check_id="check_invalid_range",
            columns=["event_time"],
            min_value="2023-12-31 23:59:59",
            max_value="2020-01-01 00:00:00",
        )


def test_timestamp_between_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that TimestampBetweenCheck flags rows where the timestamp column value is outside the
    allowed range.

    A row is marked as failed if the 'event_time' is before '2020-01-01 00:00:00' or
    after '2023-12-31 23:59:59'.
    """
    data = [
        Row(event_time="2019-12-31 23:59:59"),
        Row(event_time="2020-01-01 00:00:00"),
        Row(event_time="2021-01-01 00:00:00"),
        Row(event_time="2023-12-31 23:59:59"),
        Row(event_time="2024-01-01 00:00:00"),
    ]
    schema = StructType([StructField("event_time", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("event_time", to_timestamp("event_time"))

    config = TimestampBetweenCheckConfig(
        check_id="timestamp_between_check",
        columns=["event_time"],
        min_value="2020-01-01 00:00:00",
        max_value="2023-12-31 23:59:59",
    )
    check = config.to_check()
    result_df = check.validate(df)

    expected_data = [
        ("2019-12-31 23:59:59", True),
        ("2020-01-01 00:00:00", True),
        ("2021-01-01 00:00:00", False),
        ("2023-12-31 23:59:59", True),
        ("2024-01-01 00:00:00", True),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["event_time", "timestamp_between_check"])
    expected_df = expected_raw.withColumn("event_time", to_timestamp("event_time"))

    assertDataFrameEqual(result_df, expected_df)


def test_timestamp_between_check_validate_inclusive_bounds(spark: SparkSession) -> None:
    """
    Verifies that TimestampBetweenCheck respects inclusive=True on both bounds.

    A row on either boundary should be valid.
    """
    data = [
        Row(event_time="2020-01-01 00:00:00"),  # lower bound
        Row(event_time="2023-12-31 23:59:59"),  # upper bound
        Row(event_time="2019-12-31 23:59:59"),  # below min
        Row(event_time="2024-01-01 00:00:00"),  # above max
    ]
    schema = StructType([StructField("event_time", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("event_time", to_timestamp("event_time"))

    config = TimestampBetweenCheckConfig(
        check_id="timestamp_between_inclusive",
        columns=["event_time"],
        min_value="2020-01-01 00:00:00",
        max_value="2023-12-31 23:59:59",
        inclusive=(True, True),
    )
    check = config.to_check()
    result_df = check.validate(df)

    expected_data = [
        ("2020-01-01 00:00:00", False),
        ("2023-12-31 23:59:59", False),
        ("2019-12-31 23:59:59", True),
        ("2024-01-01 00:00:00", True),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["event_time", "timestamp_between_inclusive"])
    expected_df = expected_raw.withColumn("event_time", to_timestamp("event_time"))

    assertDataFrameEqual(result_df, expected_df)


def test_timestamp_between_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that TimestampBetweenCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = TimestampBetweenCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        min_value="2020-01-01 00:00:00",
        max_value="2023-12-31 23:59:59",
    )
    check = config.to_check()

    with pytest.raises(MissingColumnError):
        check.validate(df)
