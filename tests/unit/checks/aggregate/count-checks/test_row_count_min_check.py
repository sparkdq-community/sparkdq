import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.aggregate.count_checks.count_min_check import (
    RowCountMinCheck,
    RowCountMinCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_row_count_min_passes(spark: SparkSession) -> None:
    """
    Validates that RowCountMinCheck passes when the DataFrame row count is greater than or equal to min_count.

    Given a DataFrame with 3 rows, and a configured min_count=3,
    the check should pass and return the correct actual row count.
    """
    # Arrange: Create a DataFrame with 3 rows
    df = spark.range(3)
    check = RowCountMinCheck(check_id="test", min_count=3)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check passes and returns the correct metric
    assert result.passed is True
    assert result.metrics["actual_row_count"] == 3
    assert result.metrics["min_expected"] == 3


def test_row_count_min_fails(spark: SparkSession) -> None:
    """
    Validates that RowCountMinCheck fails when the DataFrame has fewer rows than min_count.

    Given a DataFrame with 2 rows, and a configured min_count=5,
    the check should fail and return the actual row count.
    """
    # Arrange: Create a DataFrame with 2 rows and configure the check
    df = spark.range(2)
    check = RowCountMinCheck(check_id="test", min_count=5)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check fails and returns the actual row count
    assert result.passed is False
    assert result.metrics["actual_row_count"] == 2
    assert result.metrics["min_expected"] == 5


def test_row_count_min_config_valid() -> None:
    """
    Validates that a RowCountMinCheckConfig is created successfully with a valid min_count.

    Given min_count=5, the config object should be instantiated with correct values
    and linked to the correct check class.
    """
    # Arrange & Act: Create a valid configuration
    config = RowCountMinCheckConfig(check_id="test", min_count=5)

    # Assert: Configuration is correctly initialized
    assert config.min_count == 5
    assert config.check_class == RowCountMinCheck


def test_row_count_min_config_invalid_negative_min() -> None:
    """
    Validates that RowCountMinCheckConfig raises an error when min_count is negative.

    Given min_count=-1, an InvalidCheckConfigurationError should be raised during instantiation.
    """
    # Arrange & Act & Assert: Instantiating config with negative min_count should raise an error
    with pytest.raises(InvalidCheckConfigurationError):
        RowCountMinCheckConfig(check_id="test", min_count=-1)
