import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.aggregate.completeness_checks.completeness_ratio_check import (
    CompletenessRatioCheck,
    CompletenessRatioCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_completeness_ratio_check_passes_when_ratio_above_threshold(spark: SparkSession) -> None:
    """
    Validates that CompletenessRatioCheck passes when the non-null ratio is above the threshold.

    Given 3 non-null and 2 null values in 'value', with a min_ratio of 0.5, the check should pass.
    """
    # Arrange
    df = spark.createDataFrame(
        [
            (1, "a"),
            (2, None),
            (3, "b"),
            (4, None),
            (5, "c"),
        ],
        ["id", "value"],
    )
    check = CompletenessRatioCheck(check_id="check", column="value", min_ratio=0.5)

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is True
    assert result.metrics["actual_ratio"] == 0.6


def test_completeness_ratio_check_fails_when_ratio_below_threshold(spark: SparkSession) -> None:
    """
    Validates that CompletenessRatioCheck fails when the non-null ratio is below the threshold.

    Given 3 non-null and 2 null values in 'value', with a min_ratio of 0.8, the check should fail.
    """
    df = spark.createDataFrame(
        [
            (1, "a"),
            (2, None),
            (3, "b"),
            (4, None),
            (5, "c"),
        ],
        ["id", "value"],
    )
    check = CompletenessRatioCheck(check_id="check", column="value", min_ratio=0.8)
    result = check._evaluate_logic(df)
    assert result.passed is False
    assert result.metrics["actual_ratio"] == 0.6


def test_completeness_ratio_check_exact_threshold(spark: SparkSession) -> None:
    """
    Validates that CompletenessRatioCheck passes when the non-null ratio is exactly at the threshold.
    """
    df = spark.createDataFrame([(1, "a"), (2, None), (3, "b"), (4, None), (5, "c")], ["id", "value"])
    check = CompletenessRatioCheck(check_id="check", column="value", min_ratio=0.6)
    result = check._evaluate_logic(df)
    assert result.passed is True


def test_completeness_ratio_check_empty_dataframe(spark: SparkSession) -> None:
    """
    Validates that CompletenessRatioCheck passes for empty DataFrame (assumes 100% complete).
    """
    df = spark.createDataFrame([], schema="value STRING")
    check = CompletenessRatioCheck(check_id="check", column="value", min_ratio=0.9)
    result = check._evaluate_logic(df)
    assert result.passed is True
    assert result.metrics["actual_ratio"] == 1.0


def test_completeness_ratio_config_rejects_invalid_ratio() -> None:
    """
    Validates that CompletenessRatioCheckConfig raises error for min_ratio > 1.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        CompletenessRatioCheckConfig(check_id="invalid", column="x", min_ratio=1.2)
