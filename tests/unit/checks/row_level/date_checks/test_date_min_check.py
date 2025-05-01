import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.date_checks.date_min_check import DateMinCheck, DateMinCheckConfig
from sparkdq.exceptions import MissingColumnError


def test_date_min_check_config_valid() -> None:
    """
    Validates that DateMinCheckConfig correctly instantiates a DateMinCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, and min_value.
    """
    # Arrange: Create a config with valid parameters
    config = DateMinCheckConfig(
        check_id="check_min_date",
        columns=["record_date"],
        min_value="2020-01-01",
    )

    # Act: Convert config to check instance
    check = config.to_check()

    # Assert: Check instance has correct attributes
    assert isinstance(check, DateMinCheck)
    assert check.check_id == "check_min_date"
    assert check.columns == ["record_date"]
    assert check.min_value == "2020-01-01"


def test_date_min_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that DateMinCheck flags rows where the date column value is before min_value.

    A row is marked as failed if the 'record_date' is before '2020-01-01'.
    """
    # Arrange: Create a DataFrame with true DateType column
    data = [
        Row(record_date="2019-12-31"),  # should fail
        Row(record_date="2020-01-01"),  # should fail
        Row(record_date="2021-06-15"),  # should pass
    ]
    schema = StructType([StructField("record_date", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("record_date", to_date("record_date"))

    config = DateMinCheckConfig(
        check_id="date_min_check",
        columns=["record_date"],
        min_value="2020-01-01",
    )
    check = config.to_check()

    # Act: Apply the DateMinCheck
    result_df = check.validate(df)

    # Assert: Expect True where 'record_date' is before '2020-01-01', otherwise False
    expected_data = [
        ("2019-12-31", True),
        ("2020-01-01", True),
        ("2021-06-15", False),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["record_date", "date_min_check"])
    expected_df = expected_raw.withColumn("record_date", to_date("record_date"))

    assertDataFrameEqual(result_df, expected_df)


def test_date_min_check_validate_inclusive_true(spark: SparkSession) -> None:
    """
    Verifies that DateMinCheck with inclusive=True treats the boundary value as valid.
    """
    # Arrange: Create a DataFrame with values around the boundary
    data = [
        Row(record_date="2019-12-31"),
        Row(record_date="2020-01-01"),
        Row(record_date="2021-06-15"),
    ]
    schema = StructType([StructField("record_date", StringType(), True)])
    df_raw = spark.createDataFrame(data, schema)
    df = df_raw.withColumn("record_date", to_date("record_date"))

    config = DateMinCheckConfig(
        check_id="date_min_inclusive_check", columns=["record_date"], min_value="2020-01-01", inclusive=True
    )
    check = config.to_check()

    # Act: Apply the DateMinCheck
    result_df = check.validate(df)

    # Assert: Expect only date below threshold to fail
    expected_data = [
        ("2019-12-31", True),
        ("2020-01-01", False),
        ("2021-06-15", False),
    ]
    expected_raw = spark.createDataFrame(expected_data, ["record_date", "date_min_inclusive_check"])
    expected_df = expected_raw.withColumn("record_date", to_date("record_date"))

    assertDataFrameEqual(result_df, expected_df)


def test_date_min_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that DateMinCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    # Arrange: DataFrame does not contain the required 'missing' column
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = DateMinCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        min_value="2020-01-01",
    )
    check = config.to_check()

    # Act & Assert: Expect MissingColumnError when the column is not present
    with pytest.raises(MissingColumnError):
        check.validate(df)
