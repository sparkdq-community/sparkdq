from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, TimestampType

from sparkdq.checks.aggregate.freshness_checks.freshness_check import FreshnessCheck, FreshnessCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_freshness_check_passes_when_recent_timestamp(spark: SparkSession, mocker) -> None:
    """
    Validates that FreshnessCheck passes when the most recent timestamp is within the allowed interval.

    Given a timestamp 1 minute ago and a freshness threshold of 2 minutes,
    the check should pass.
    """
    # Arrange
    now = datetime(2025, 5, 21, 12, 0, 0)
    max_ts = now - timedelta(minutes=1)
    schema = StructType(
        [
            StructField("ts", TimestampType(), True),
        ]
    )
    df = spark.createDataFrame([(max_ts,)], schema=schema)

    check = FreshnessCheck(
        check_id="fresh_check_pass",
        column="ts",
        period="minute",
        interval=2,
        severity=Severity.CRITICAL,
    )

    mocker.patch(
        "sparkdq.checks.aggregate.freshness_checks.freshness_check.F.current_timestamp",
        return_value=F.lit(now),
    )

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert isinstance(result, AggregateEvaluationResult)
    assert result.passed is True
    assert result.metrics["max_timestamp"] == str(max_ts)
    assert result.metrics["freshness_threshold"] == "2 minute"


def test_freshness_check_fails_when_timestamp_too_old(spark: SparkSession, mocker) -> None:
    """
    Validates that FreshnessCheck fails when the most recent timestamp exceeds the allowed interval.

    Given a timestamp 10 minutes ago and a freshness threshold of 5 minutes,
    the check should fail.
    """
    # Arrange
    now = datetime(2025, 5, 21, 12, 0, 0)
    max_ts = now - timedelta(minutes=10)
    schema = StructType(
        [
            StructField("ts", TimestampType(), True),
        ]
    )
    df = spark.createDataFrame([(max_ts,)], schema=schema)

    check = FreshnessCheck(
        check_id="fresh_check_fail",
        column="ts",
        period="minute",
        interval=5,
        severity=Severity.CRITICAL,
    )

    mocker.patch(
        "sparkdq.checks.aggregate.freshness_checks.freshness_check.F.current_timestamp",
        return_value=F.lit(now),
    )

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert isinstance(result, AggregateEvaluationResult)
    assert result.passed is False
    assert result.metrics["max_timestamp"] == str(max_ts)
    assert result.metrics["freshness_threshold"] == "5 minute"


def test_freshness_check_fails_when_column_is_null(spark: SparkSession, mocker) -> None:
    """
    Validates that FreshnessCheck fails when the timestamp column contains only null values.

    Given a DataFrame with null timestamps, the check should fail and max_timestamp should be None.
    """
    # Arrange
    now = datetime(2025, 5, 21, 12, 0, 0)
    schema = StructType(
        [
            StructField("ts", TimestampType(), True),
        ]
    )
    df = spark.createDataFrame([(None,), (None,)], schema=schema)

    check = FreshnessCheck(
        check_id="fresh_check_nulls",
        column="ts",
        period="hour",
        interval=1,
        severity=Severity.CRITICAL,
    )

    mocker.patch(
        "sparkdq.checks.aggregate.freshness_checks.freshness_check.F.current_timestamp",
        return_value=F.lit(now),
    )

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert isinstance(result, AggregateEvaluationResult)
    assert result.passed is False
    assert result.metrics["max_timestamp"] is None
    assert result.metrics["freshness_threshold"] == "1 hour"


def test_freshness_check_config_valid() -> None:
    """
    Validates that FreshnessCheckConfig correctly instantiates a FreshnessCheck
    with valid configuration.
    """
    config = FreshnessCheckConfig(
        check_id="freshness-check", column="last_updated", interval=6, period="hour"
    )

    check = config.to_check()

    assert isinstance(check, FreshnessCheck)
    assert check.check_id == "freshness-check"
    assert check.column == "last_updated"
    assert check.interval == 6
    assert check.period == "hour"


def test_freshness_check_config_invalid_interval() -> None:
    """
    Validates that FreshnessCheckConfig raises error when interval is zero or negative.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        FreshnessCheckConfig(check_id="invalid", column="updated_at", interval=0, period="day")

    with pytest.raises(InvalidCheckConfigurationError):
        FreshnessCheckConfig(check_id="invalid", column="updated_at", interval=-1, period="day")


def test_freshness_check_config_invalid_period_literal() -> None:
    """
    Ensures that invalid period values are rejected by Pydantic via Literal enforcement.
    """
    with pytest.raises(ValueError):
        FreshnessCheckConfig(
            check_id="fail",
            column="updated_at",
            interval=5,
            period="invalid",  # not in allowed Literal list
        )
