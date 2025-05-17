import pytest
from pyspark.sql import Row, SparkSession

from sparkdq.checks.aggregate.uniqueness_checks.distinct_ratio_check import (
    DistinctRatioCheck,
    DistinctRatioCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_distinct_ratio_check_config_valid() -> None:
    """
    Validates that DistinctRatioCheckConfig correctly instantiates a DistinctRatioCheck
    with valid configuration parameters.
    """
    # Arrange & Act: Create a valid configuration
    config = DistinctRatioCheckConfig(
        check_id="check_distinct_ratio",
        column="city",
        min_ratio=0.5,
    )
    check = config.to_check()

    # Assert
    assert isinstance(check, DistinctRatioCheck)
    assert check.column == "city"
    assert check.min_ratio == 0.5


def test_distinct_ratio_check_config_invalid_ratio() -> None:
    """
    Validates that DistinctRatioCheckConfig raises an error for invalid min_ratio values.
    """
    # Act & Assert: Ratio < 0 or > 1 should raise error
    with pytest.raises(InvalidCheckConfigurationError):
        DistinctRatioCheckConfig(check_id="test", column="x", min_ratio=-0.1)

    with pytest.raises(InvalidCheckConfigurationError):
        DistinctRatioCheckConfig(check_id="test", column="x", min_ratio=1.5)


def test_distinct_ratio_check_passes(spark: SparkSession) -> None:
    """
    Validates that DistinctRatioCheck passes when the distinct ratio meets or exceeds the minimum.
    """
    # Arrange: 3 distinct values (Berlin, Hamburg, München) over 4 rows (0.75 ratio)
    df = spark.createDataFrame(
        [
            Row(city="Berlin"),
            Row(city="Berlin"),
            Row(city="Hamburg"),
            Row(city="München"),
        ]
    )
    check = DistinctRatioCheck(check_id="check", column="city", min_ratio=0.7)

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is True
    assert result.metrics["distinct_ratio"] == 0.75


def test_distinct_ratio_check_fails(spark: SparkSession) -> None:
    """
    Validates that DistinctRatioCheck fails when the distinct ratio is below the minimum.
    """
    df = spark.createDataFrame(
        [
            Row(city="Berlin"),
            Row(city="Berlin"),
            Row(city="Berlin"),
            Row(city="München"),
        ]
    )
    check = DistinctRatioCheck(check_id="check", column="city", min_ratio=0.6)

    result = check._evaluate_logic(df)

    assert result.passed is False
    assert result.metrics["distinct_ratio"] == 0.5


def test_distinct_ratio_check_ignores_nulls(spark: SparkSession) -> None:
    """
    Validates that DistinctRatioCheck ignores nulls in the distinct count.
    """
    df = spark.createDataFrame(
        [
            Row(city=None),
            Row(city="Berlin"),
            Row(city="Berlin"),
            Row(city=None),
            Row(city="Hamburg"),
        ]
    )
    check = DistinctRatioCheck(check_id="check", column="city", min_ratio=0.5)

    result = check._evaluate_logic(df)

    assert result.passed is True
    assert 0.66 <= result.metrics["distinct_ratio"] <= 0.67
