from datetime import date

import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.row_level.contained_checks.is_contained_in_check import (
    IsContainedInCheck,
    IsContainedInCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_is_contained_in_check_passes_when_all_values_allowed(spark: SparkSession) -> None:
    """
    Validates that IsContainedInCheck passes when all values are within the allowed set.

    Given a DataFrame where all column values are contained in the allowed lists,
    the check should pass for every row.
    """
    # Arrange
    df = spark.createDataFrame([("ACTIVE", "DE"), ("INACTIVE", "FR")], ["status", "country"])
    check = IsContainedInCheck(
        check_id="test_check", allowed_values={"status": ["ACTIVE", "INACTIVE"], "country": ["DE", "FR"]}
    )

    # Act
    result_df = check.validate(df)

    # Assert
    result = result_df.select("test_check").collect()
    assert all(row.test_check for row in result)


def test_is_contained_in_check_fails_when_unallowed_values_present(spark: SparkSession) -> None:
    """
    Validates that IsContainedInCheck fails when some values are not contained in the allowed sets.

    Given a DataFrame where at least one value is not allowed,
    the check should fail for the affected row(s).
    """
    # Arrange
    df = spark.createDataFrame([("ACTIVE", "DE"), ("PENDING", "FR")], ["status", "country"])
    check = IsContainedInCheck(
        check_id="test_check", allowed_values={"status": ["ACTIVE", "INACTIVE"], "country": ["DE", "FR"]}
    )

    # Act
    result_df = check.validate(df)

    # Assert
    result = result_df.select("test_check").collect()
    assert result[0].test_check is True
    assert result[1].test_check is False


@pytest.mark.parametrize(
    "allowed_values, expected_passed",
    [
        ({"status": ["ACTIVE"]}, [True, False]),
        ({"country": ["FR"]}, [False, True]),
    ],
)
def test_is_contained_in_check_various_columns(spark: SparkSession, allowed_values, expected_passed) -> None:
    """
    Validates that IsContainedInCheck works independently for different columns.

    Test cases where only specific columns are validated.
    """
    # Arrange
    df = spark.createDataFrame([("ACTIVE", "DE"), ("INACTIVE", "FR")], ["status", "country"])
    check = IsContainedInCheck(
        check_id="test_check",
        allowed_values=allowed_values,
    )

    # Act
    result_df = check.validate(df)

    # Assert
    result = [row.test_check for row in result_df.select("test_check").collect()]
    assert result == expected_passed


def test_is_contained_in_check_config_valid() -> None:
    """
    Validates that IsContainedInCheckConfig is created successfully with valid allowed_values.
    """
    # Arrange & Act
    config = IsContainedInCheckConfig(check_id="test", allowed_values={"status": ["ACTIVE", "INACTIVE"]})

    # Assert
    assert config.allowed_values == {"status": ["ACTIVE", "INACTIVE"]}
    assert config.check_class == IsContainedInCheck


def test_is_contained_in_check_config_invalid_empty_allowed_values() -> None:
    """
    Validates that IsContainedInCheckConfig raises an error if allowed_values is empty.
    """
    # Arrange & Act & Assert
    with pytest.raises(InvalidCheckConfigurationError):
        IsContainedInCheckConfig(check_id="test", allowed_values={})


@pytest.mark.parametrize(
    "input_data, allowed_values, expected_results",
    [
        # Integer values
        ([(1,), (2,), (3,)], {"value": [1, 2]}, [True, True, False]),
        # String values
        ([("A",), ("B",), ("C",)], {"value": ["A", "B"]}, [True, True, False]),
        # Date values
        ([(date(2024, 1, 1),), (date(2024, 5, 1),)], {"value": [date(2024, 1, 1)]}, [True, False]),
    ],
)
def test_is_contained_in_check_with_various_types(
    spark: SparkSession, input_data, allowed_values, expected_results
) -> None:
    """
    Validates that IsContainedInCheck works correctly with different data types.

    Test for integers, strings, and dates.
    """
    # Arrange
    df = spark.createDataFrame(input_data, ["value"])
    check = IsContainedInCheck(check_id="test_check", allowed_values=allowed_values)

    # Act
    result_df = check.validate(df)

    # Assert
    result = [row.test_check for row in result_df.select("test_check").collect()]
    assert result == expected_results
