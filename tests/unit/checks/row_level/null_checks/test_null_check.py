import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.null_checks.null_check import NullCheck, NullCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError


def test_null_check_validate_single_column(spark: SparkSession) -> None:
    """
    Validates that NullCheck flags rows with null values in a single specified column.

    A row is marked as failed if the 'name' column is null.
    """
    # Arrange: Create a sample DataFrame with null and non-null values in column 'name'
    df = spark.createDataFrame([(1, "John"), (2, None)], ["id", "name"])
    check = NullCheck(check_id="test", columns=["name"])

    # Act: Apply the NullCheck
    result_df = check.validate(df)

    # Assert: Expect True (fail) where 'name' is null, otherwise False
    expected_df = spark.createDataFrame([(1, "John", False), (2, None, True)], ["id", "name", "test"])
    assertDataFrameEqual(result_df, expected_df)


def test_null_check_validate_multiple_columns(spark: SparkSession) -> None:
    """
    Validates that NullCheck fails if any of the specified columns are null.

    A row is marked as failed if either 'name' or 'email' is null.
    """
    # Arrange: Create a DataFrame with some nulls in 'name' and 'email'
    df = spark.createDataFrame(
        [
            (1, "John", "john@example.com"),
            (2, None, "jane@example.com"),
            (3, "Max", None),
            (4, None, None),
        ],
        ["id", "name", "email"],
    )
    check = NullCheck(check_id="check_nulls", columns=["name", "email"])

    # Act: Apply the NullCheck
    result_df = check.validate(df)

    # Assert: Fail if either 'name' or 'email' is null
    expected_df = spark.createDataFrame(
        [
            (1, "John", "john@example.com", False),
            (2, None, "jane@example.com", True),
            (3, "Max", None, True),
            (4, None, None, True),
        ],
        ["id", "name", "email", "check_nulls"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_null_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that NullCheck raises MissingColumnError if a specified column is not present.

    The check should immediately fail at runtime when accessing a missing column.
    """
    # Arrange: Create DataFrame missing the column 'missing'
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    check = NullCheck(check_id="check", columns=["name", "missing"])

    # Act & Assert: Expect MissingColumnError
    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_null_check_config_to_check_instantiates_correct_check() -> None:
    """
    Validates that NullCheckConfig creates a proper NullCheck instance via to_check().
    """
    # Arrange: Create config object with two target columns
    config = NullCheckConfig(
        check_id="null_check_config_test", columns=["email", "user_id"], severity=Severity.WARNING
    )

    # Act: Convert config to concrete check instance
    check = config.to_check()

    # Assert: Instance has correct values
    assert isinstance(check, NullCheck)
    assert check.check_id == "null_check_config_test"
    assert check.columns == ["email", "user_id"]
    assert check.severity == Severity.WARNING
