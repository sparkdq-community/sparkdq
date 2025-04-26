import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.aggregate.count_checks.count_exact_check import (
    RowCountExactCheck,
    RowCountExactCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_row_count_exact_passes(spark: SparkSession) -> None:
    """
    Validates that RowCountExactCheck passes when the DataFrame row count
    matches the expected_count.

    Given a DataFrame with 4 rows, and a configured expected_count=4,
    the check should pass and return the correct actual row count.
    """
    # Arrange: Create a DataFrame with 4 rows
    df = spark.range(4)
    check = RowCountExactCheck(check_id="test", expected_count=4)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check passes and returns the correct metric
    assert result.passed is True
    assert result.metrics["actual_row_count"] == 4
    assert result.metrics["expected_row_count"] == 4


def test_row_count_exact_fails(spark: SparkSession) -> None:
    """
    Validates that RowCountExactCheck fails when the DataFrame row count
    does not match the expected_count.

    Given a DataFrame with 5 rows, and a configured expected_count=4,
    the check should fail and return the actual row count.
    """
    # Arrange: Create a DataFrame with 5 rows
    df = spark.range(5)
    check = RowCountExactCheck(check_id="test", expected_count=4)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check fails and returns the actual row count
    assert result.passed is False
    assert result.metrics["actual_row_count"] == 5
    assert result.metrics["expected_row_count"] == 4


def test_row_count_exact_config_valid() -> None:
    """
    Validates that a RowCountExactCheckConfig is created successfully with a valid expected_count.

    Given expected_count=7, the config object should be instantiated with correct values
    and linked to the correct check class.
    """
    # Arrange & Act: Create a valid configuration
    config = RowCountExactCheckConfig(check_id="test", expected_count=7)

    # Assert: Configuration is correctly initialized
    assert config.expected_count == 7
    assert config.check_class == RowCountExactCheck


def test_row_count_exact_config_invalid_negative_expected() -> None:
    """
    Validates that RowCountExactCheckConfig raises an error when expected_count is negative.

    Given expected_count=-1, an InvalidCheckConfigurationError should be raised during instantiation.
    """
    # Arrange & Act & Assert: Instantiating config with negative expected_count should raise an error
    with pytest.raises(InvalidCheckConfigurationError):
        RowCountExactCheckConfig(check_id="test", expected_count=-1)
