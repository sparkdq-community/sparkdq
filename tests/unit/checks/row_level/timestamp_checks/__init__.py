import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.timestamp_checks.timestamp_min_check import (
    TimestampMinCheck,
    TimestampMinCheckConfig,
)
from sparkdq.exceptions import MissingColumnError


def test_timestamp_min_check_config_valid() -> None:
    """
    Validates that TimestampMinCheckConfig correctly instantiates a TimestampMinCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, and min_value.
    """
    # Arrange: Create a config with valid parameters
    config = TimestampMinCheckConfig(
        check_id="check_min_timestamp",
        columns=["event_time"],
        min_value="2020-01-01 00:00:00",
    )

    # Act: Convert config to check instance
    check = config.to_check()

    # Assert: Check instance has correct attributes
    assert isinstance(check, TimestampMinCheck)
    assert check.check_id == "check_min_timestamp"
    assert check.columns == ["event_time"]
    assert check.min_value == "2020-01-01 00:00:00"


def test_timestamp_min_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that TimestampMinCheck flags rows where the timestamp column value is before min_value.

    A row is marked as failed if the 'event_time' is before '2020-01-01 00:00:00'.
    """
    # Arrange: Create a DataFrame with true TimestampType column
    data = [
        Row(event_time="2019-12-31 23:59:59"),  # should fail (before min)
        Row(event_time="2020-01-01 00:00:00"),  # should pass (on boundary)
        Row(event_time="2021-06-15 12:00:00"),  # should pass
    ]
    schema = StructType([StructField("event_time", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("event_time", to_timestamp("event_time"))

    config = TimestampMinCheckConfig(
        check_id="timestamp_min_check",
        columns=["event_time"],
        min_value="2020-01-01 00:00:00",
    )
    check = config.to_check()

    # Act: Apply the TimestampMinCheck
    result_df = check.validate(df)

    # Assert: Expect True where 'event_time' is before the threshold, otherwise False
    expected_data = [
        ("2019-12-31 23:59:59", True),
        ("2020-01-01 00:00:00", False),
        ("2021-06-15 12:00:00", False),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["event_time", "timestamp_min_check"])
    expected_df = expected_raw.withColumn("event_time", to_timestamp("event_time"))

    assertDataFrameEqual(result_df, expected_df)


def test_timestamp_min_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that TimestampMinCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    # Arrange: DataFrame does not contain the required 'missing' column
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = TimestampMinCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        min_value="2020-01-01 00:00:00",
    )
    check = config.to_check()

    # Act & Assert: Expect MissingColumnError when the column is not present
    with pytest.raises(MissingColumnError):
        check.validate(df)
