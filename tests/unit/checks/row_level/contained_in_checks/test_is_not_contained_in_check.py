from datetime import date

import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.row_level.contained_checks.is_not_contained_in_check import (
    IsNotContainedInCheck,
    IsNotContainedInCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_is_not_contained_in_check_passes_when_no_forbidden_values_present(spark: SparkSession) -> None:
    """
    Validates that IsNotContainedInCheck passes when no forbidden values are present.

    Given a DataFrame where no column contains forbidden values,
    the check should pass for every row.
    """
    # Arrange
    df = spark.createDataFrame([("ACTIVE", "DE"), ("INACTIVE", "FR")], ["status", "country"])
    check = IsNotContainedInCheck(
        check_id="test_check", forbidden_values={"status": ["PENDING"], "country": ["IT"]}
    )

    # Act
    result_df = check.validate(df)

    # Assert
    result = result_df.select("test_check").collect()
    assert all(row.test_check for row in result)


def test_is_not_contained_in_check_fails_when_forbidden_values_present(spark: SparkSession) -> None:
    """
    Validates that IsNotContainedInCheck fails when forbidden values are found.

    Given a DataFrame where at least one forbidden value is present,
    the check should fail for the affected row(s).
    """
    # Arrange
    df = spark.createDataFrame([("ACTIVE", "DE"), ("DELETED", "FR")], ["status", "country"])
    check = IsNotContainedInCheck(
        check_id="test_check", forbidden_values={"status": ["DELETED"], "country": ["IT"]}
    )

    # Act
    result_df = check.validate(df)

    # Assert
    result = result_df.select("test_check").collect()
    assert result[0].test_check is True
    assert result[1].test_check is False


@pytest.mark.parametrize(
    "forbidden_values, expected_passed",
    [
        ({"status": ["PENDING"]}, [True, True]),
        ({"country": ["DE"]}, [False, True]),
    ],
)
def test_is_not_contained_in_check_various_columns(
    spark: SparkSession, forbidden_values, expected_passed
) -> None:
    """
    Validates that IsNotContainedInCheck works independently for different forbidden columns.

    Test cases where only specific forbidden values are checked.
    """
    # Arrange
    df = spark.createDataFrame([("ACTIVE", "DE"), ("INACTIVE", "FR")], ["status", "country"])
    check = IsNotContainedInCheck(
        check_id="test_check",
        forbidden_values=forbidden_values,
    )

    # Act
    result_df = check.validate(df)

    # Assert
    result = [row.test_check for row in result_df.select("test_check").collect()]
    assert result == expected_passed


def test_is_not_contained_in_check_config_valid() -> None:
    """
    Validates that IsNotContainedInCheckConfig is created successfully with valid forbidden_values.
    """
    # Arrange & Act
    config = IsNotContainedInCheckConfig(
        check_id="test", forbidden_values={"status": ["DELETED", "CANCELLED"]}
    )

    # Assert
    assert config.forbidden_values == {"status": ["DELETED", "CANCELLED"]}
    assert config.check_class == IsNotContainedInCheck


def test_is_not_contained_in_check_config_invalid_empty_forbidden_values() -> None:
    """
    Validates that IsNotContainedInCheckConfig raises an error if forbidden_values is empty.
    """
    # Arrange & Act & Assert
    with pytest.raises(InvalidCheckConfigurationError):
        IsNotContainedInCheckConfig(check_id="test", forbidden_values={})


@pytest.mark.parametrize(
    "input_data, forbidden_values, expected_results",
    [
        # Integer values
        ([(1,), (2,), (3,)], {"value": [3]}, [True, True, False]),
        # String values
        ([("A",), ("B",), ("C",)], {"value": ["C"]}, [True, True, False]),
        # Date values
        ([(date(2024, 1, 1),), (date(2024, 5, 1),)], {"value": [date(2024, 5, 1)]}, [True, False]),
    ],
)
def test_is_not_contained_in_check_with_various_types(
    spark: SparkSession, input_data, forbidden_values, expected_results
) -> None:
    """
    Validates that IsNotContainedInCheck works correctly with different data types.

    Test for integers, strings, and dates.
    """
    # Arrange
    df = spark.createDataFrame(input_data, ["value"])
    check = IsNotContainedInCheck(check_id="test_check", forbidden_values=forbidden_values)

    # Act
    result_df = check.validate(df)

    # Assert
    result = [row.test_check for row in result_df.select("test_check").collect()]
    assert result == expected_results
