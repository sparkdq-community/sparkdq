import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.aggregate.count_checks.count_max_check import (
    RowCountMaxCheck,
    RowCountMaxCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_row_count_max_passes(spark: SparkSession) -> None:
    """
    Validates that RowCountMaxCheck passes when the DataFrame row count
    is less than or equal to max_count.

    Given a DataFrame with 3 rows, and a configured max_count=5,
    the check should pass and return the correct actual row count.
    """
    # Arrange: Create a DataFrame with 3 rows
    df = spark.range(3)
    check = RowCountMaxCheck(check_id="test", max_count=5)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check passes and returns the correct metric
    assert result.passed is True
    assert result.metrics["actual_row_count"] == 3
    assert result.metrics["max_expected"] == 5


def test_row_count_max_fails(spark: SparkSession) -> None:
    """
    Validates that RowCountMaxCheck fails when the DataFrame row count
    exceeds the configured max_count.

    Given a DataFrame with 6 rows, and a configured max_count=5,
    the check should fail and return the actual row count.
    """
    # Arrange: Create a DataFrame with 6 rows
    df = spark.range(6)
    check = RowCountMaxCheck(check_id="test", max_count=5)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check fails and returns the actual row count
    assert result.passed is False
    assert result.metrics["actual_row_count"] == 6
    assert result.metrics["max_expected"] == 5


def test_row_count_max_config_valid() -> None:
    """
    Validates that a RowCountMaxCheckConfig is created successfully with a valid max_count.

    Given max_count=10, the config object should be instantiated with correct values
    and linked to the correct check class.
    """
    # Arrange & Act: Create a valid configuration
    config = RowCountMaxCheckConfig(check_id="test", max_count=10)

    # Assert: Configuration is correctly initialized
    assert config.max_count == 10
    assert config.check_class == RowCountMaxCheck


def test_row_count_max_config_invalid_negative_max() -> None:
    """
    Validates that RowCountMaxCheckConfig raises an error when max_count is not greater than 0.

    Given max_count=0, an InvalidCheckConfigurationError should be raised during instantiation.
    """
    # Arrange & Act & Assert: Instantiating config with invalid max_count should raise an error
    with pytest.raises(InvalidCheckConfigurationError):
        RowCountMaxCheckConfig(check_id="test", max_count=0)
