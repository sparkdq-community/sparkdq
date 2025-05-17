import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.aggregate.uniqueness_checks.unique_ratio_check import (
    UniqueRatioCheck,
    UniqueRatioCheckConfig,
)
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_unique_ratio_check_passes_with_sufficient_uniqueness(spark: SparkSession) -> None:
    """
    Validates that UniqueRatioCheck passes when the uniqueness ratio meets or exceeds the minimum threshold.

    Given a DataFrame with 5 rows and 4 unique non-null values in the 'user_id' column,
    the uniqueness ratio is 0.8, which passes the configured minimum of 0.7.
    """
    df = spark.createDataFrame(
        [
            ("a",),
            ("b",),
            ("c",),
            ("a",),
            ("d",),
        ],
        ["user_id"],
    )
    check = UniqueRatioCheck(check_id="check", column="user_id", min_ratio=0.7)
    result = check._evaluate_logic(df)
    assert isinstance(result, AggregateEvaluationResult)
    assert result.passed is True
    assert result.metrics["actual_ratio"] >= 0.7


def test_unique_ratio_check_fails_with_low_uniqueness(spark: SparkSession) -> None:
    """
    Validates that UniqueRatioCheck fails when the uniqueness ratio is below the configured threshold.
    """
    df = spark.createDataFrame(
        [
            ("a",),
            ("a",),
            ("a",),
            ("b",),
            ("b",),
        ],
        ["user_id"],
    )
    check = UniqueRatioCheck(check_id="check", column="user_id", min_ratio=0.6)
    result = check._evaluate_logic(df)
    assert result.passed is False
    assert result.metrics["actual_ratio"] < 0.6


def test_unique_ratio_check_handles_nulls_gracefully(spark: SparkSession) -> None:
    """
    Validates that UniqueRatioCheck skips null values when calculating uniqueness.
    """
    df = spark.createDataFrame(
        [
            ("a",),
            ("a",),
            (None,),
            (None,),
            ("b",),
        ],
        ["user_id"],
    )
    check = UniqueRatioCheck(check_id="check", column="user_id", min_ratio=0.5)
    result = check._evaluate_logic(df)
    assert result.metrics["unique_count"] == 2
    assert result.metrics["total_count"] == 5


def test_unique_ratio_check_config_valid() -> None:
    """
    Validates that UniqueRatioCheckConfig creates a valid check instance with correct parameters.
    """
    config = UniqueRatioCheckConfig(check_id="uniqueness_test", column="col1", min_ratio=0.9)
    check = config.to_check()
    assert isinstance(check, UniqueRatioCheck)
    assert check.column == "col1"
    assert check.min_ratio == 0.9


def test_unique_ratio_check_config_invalid_threshold() -> None:
    """
    Validates that UniqueRatioCheckConfig raises an error if the min_ratio is outside the 0–1 range.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        UniqueRatioCheckConfig(check_id="invalid_ratio", column="col1", min_ratio=1.2)
