import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.aggregate.count_checks.count_between_check import (
    RowCountBetweenCheck,
    RowCountBetweenCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_row_count_between_passes(spark: SparkSession) -> None:
    """
    Verify that RowCountBetweenCheck passes when dataset size falls within configured boundaries.

    The validation should succeed when the actual row count meets the range criteria,
    returning comprehensive metrics for successful validation outcomes.
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
    Verify that RowCountBetweenCheck fails when dataset size falls below minimum threshold.

    The validation should fail when the actual row count is insufficient, returning
    detailed metrics that explain the validation failure and actual count values.
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
    Verify that RowCountBetweenCheck fails when dataset size exceeds maximum threshold.

    The validation should fail when the actual row count is excessive, returning
    detailed metrics that explain the validation failure and actual count values.
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
    Verify that RowCountBetweenCheckConfig accepts valid range parameters during instantiation.

    The configuration should successfully initialize with logically consistent boundary
    parameters and maintain proper associations with the corresponding check class.
    """
    # Arrange & Act: Create a valid configuration
    config = RowCountBetweenCheckConfig(check_id="test", min_count=5, max_count=10)

    # Assert: Configuration is correctly initialized
    assert config.min_count == 5
    assert config.max_count == 10
    assert config.check_class == RowCountBetweenCheck


def test_row_count_between_config_invalid_range() -> None:
    """
    Verify that RowCountBetweenCheckConfig rejects logically inconsistent range parameters.

    The configuration should perform validation during instantiation and raise
    appropriate exceptions when boundary parameters create impossible conditions.
    """
    # Arrange & Act & Assert: Instantiating config with invalid range should raise an error
    with pytest.raises(InvalidCheckConfigurationError):
        RowCountBetweenCheckConfig(check_id="test", min_count=10, max_count=5)


def test_config_rejects_negative_min_count():
    """
    Verify that RowCountBetweenCheckConfig rejects negative minimum count parameters.

    The configuration should validate parameter ranges during instantiation and
    prevent the creation of checks with meaningless negative count thresholds.
    """
    # Arrange & Act & Assert: Instantiating config with negative min_count should raise an error
    with pytest.raises(InvalidCheckConfigurationError):
        RowCountBetweenCheckConfig(check_id="test", min_count=-1, max_count=10)
