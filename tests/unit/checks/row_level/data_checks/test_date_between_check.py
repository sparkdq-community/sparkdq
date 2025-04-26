import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.date_checks.date_between_check import DateBetweenCheck, DateBetweenCheckConfig
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError


def test_date_between_check_config_valid() -> None:
    """
    Validates that DateBetweenCheckConfig correctly instantiates a DateBetweenCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, min_value, and max_value.
    """
    # Arrange: Create a config with valid parameters
    config = DateBetweenCheckConfig(
        check_id="check_date_range",
        columns=["record_date"],
        min_value="2020-01-01",
        max_value="2023-12-31",
    )

    # Act: Convert config to check instance
    check = config.to_check()

    # Assert: Check instance has correct attributes
    assert isinstance(check, DateBetweenCheck)
    assert check.check_id == "check_date_range"
    assert check.columns == ["record_date"]
    assert check.min_value == "2020-01-01"
    assert check.max_value == "2023-12-31"


def test_date_between_check_config_invalid_range() -> None:
    """
    Validates that DateBetweenCheckConfig raises an InvalidCheckConfigurationError
    when min_value is greater than max_value.
    """
    # Arrange & Act & Assert: Expect error on invalid range (min > max)
    with pytest.raises(InvalidCheckConfigurationError):
        DateBetweenCheckConfig(
            check_id="check_invalid_range",
            columns=["record_date"],
            min_value="2023-12-31",
            max_value="2020-01-01",
        )


def test_date_between_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that DateBetweenCheck flags rows where the date column value is outside the allowed range.

    A row is marked as failed if the 'record_date' is before '2020-01-01' or after '2023-12-31'.
    """
    # Arrange: Create a DataFrame with true DateType column
    data = [
        Row(record_date="2019-12-31"),  # should fail (before min)
        Row(record_date="2020-01-01"),  # should pass (on lower boundary)
        Row(record_date="2023-12-31"),  # should pass (on upper boundary)
        Row(record_date="2024-01-01"),  # should fail (after max)
    ]
    schema = StructType([StructField("record_date", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("record_date", to_date("record_date"))

    config = DateBetweenCheckConfig(
        check_id="date_between_check",
        columns=["record_date"],
        min_value="2020-01-01",
        max_value="2023-12-31",
    )
    check = config.to_check()

    # Act: Apply the DateBetweenCheck
    result_df = check.validate(df)

    # Assert: Expect True where 'record_date' is outside the range, otherwise False
    expected_data = [
        ("2019-12-31", True),
        ("2020-01-01", False),
        ("2023-12-31", False),
        ("2024-01-01", True),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["record_date", "date_between_check"])
    expected_df = expected_raw.withColumn("record_date", to_date("record_date"))

    assertDataFrameEqual(result_df, expected_df)


def test_date_between_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that DateBetweenCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    # Arrange: DataFrame does not contain the required 'missing' column
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = DateBetweenCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        min_value="2020-01-01",
        max_value="2023-12-31",
    )
    check = config.to_check()

    # Act & Assert: Expect MissingColumnError when the column is not present
    with pytest.raises(MissingColumnError):
        check.validate(df)
