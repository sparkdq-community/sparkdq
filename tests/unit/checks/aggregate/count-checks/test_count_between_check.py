import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.aggregate.count_checks.count_between_check import (
    RowCountBetweenCheck,
    RowCountBetweenCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_row_count_between_passes(spark: SparkSession) -> None:
    """
    Validates that RowCountBetweenCheck passes when the DataFrame row count is within the specified bounds.

    Given a DataFrame with 3 rows, and a configured range of [2, 5],
    the check should pass and return the correct actual row count.
    """
    # Arrange: Create a DataFrame with 3 rows
    df = spark.range(3)
    check = RowCountBetweenCheck(check_id="test", min_count=2, max_count=5)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check passes and returns the correct metric
    assert result.passed is True
    assert result.metrics["actual_row_count"] == 3


def test_row_count_too_few_rows(spark: SparkSession) -> None:
    """
    Validates that RowCountBetweenCheck fails when the DataFrame has fewer rows than min_count.

    Given a DataFrame with 2 rows, and a configured range of [3, 5],
    the check should fail and return the correct actual row count.
    """
    # Arrange: Create a DataFrame with 2 rows and configure the check
    df = spark.range(2)
    check = RowCountBetweenCheck(check_id="test", min_count=3, max_count=5)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check fails and returns the actual row count
    assert result.passed is False
    assert result.metrics["actual_row_count"] == 2


def test_row_count_too_many_rows(spark: SparkSession) -> None:
    """
    Validates that RowCountBetweenCheck fails when the DataFrame has more rows than max_count.

    Given a DataFrame with 6 rows, and a configured range of [1, 5],
    the check should fail and return the correct actual row count.
    """
    # Arrange: Create a DataFrame with 6 rows and configure the check
    df = spark.range(6)
    check = RowCountBetweenCheck(check_id="test", min_count=1, max_count=5)

    # Act: Evaluate the row count
    result = check._evaluate_logic(df)

    # Assert: The check fails and returns the actual row count
    assert result.passed is False
    assert result.metrics["actual_row_count"] == 6


def test_row_count_between_config_valid() -> None:
    """
    Validates that a RowCountBetweenCheckConfig is created successfully when min_count <= max_count.

    Given min_count=5 and max_count=10,
    the config object should be instantiated with correct values and linked to the correct check class.
    """
    # Arrange & Act: Create a valid configuration
    config = RowCountBetweenCheckConfig(check_id="test", min_count=5, max_count=10)

    # Assert: Configuration is correctly initialized
    assert config.min_count == 5
    assert config.max_count == 10
    assert config.check_class == RowCountBetweenCheck


def test_row_count_between_config_invalid_range() -> None:
    """
    Validates that RowCountBetweenCheckConfig raises an error when min_count > max_count.

    Given min_count=10 and max_count=5, an InvalidCheckConfigurationError should be
    raised during instantiation.
    """
    # Arrange & Act & Assert: Instantiating config with invalid range should raise an error
    with pytest.raises(InvalidCheckConfigurationError):
        RowCountBetweenCheckConfig(check_id="test", min_count=10, max_count=5)


def test_config_rejects_negative_min_count():
    """
    Validates that RowCountBetweenCheckConfig raises an error when min_count is negative.

    Given min_count=-1 and max_count=10, an InvalidCheckConfigurationError should be
    raised during instantiation.
    """
    # Arrange & Act & Assert: Instantiating config with negative min_count should raise an error
    with pytest.raises(InvalidCheckConfigurationError):
        RowCountBetweenCheckConfig(check_id="test", min_count=-1, max_count=10)
