import pytest
from pyspark.sql import Row, SparkSession

from sparkdq.checks.aggregate.completeness_checks.columns_are_complete_check import (
    ColumnsAreCompleteCheck,
    ColumnsAreCompleteCheckConfig,
)
from sparkdq.exceptions import MissingColumnError


def test_columns_are_complete_check_config_valid() -> None:
    """
    Validates that ColumnsAreCompleteCheckConfig correctly instantiates a ColumnsAreCompleteCheck.
    """
    config = ColumnsAreCompleteCheckConfig(
        check_id="complete_columns_check", columns=["trip_id", "pickup_time"]
    )
    check = config.to_check()

    assert isinstance(check, ColumnsAreCompleteCheck)
    assert check.check_id == "complete_columns_check"
    assert check.columns == ["trip_id", "pickup_time"]


def test_columns_are_complete_check_passes(spark: SparkSession) -> None:
    """
    Passes when all specified columns contain no null values.
    """
    df = spark.createDataFrame(
        [
            Row(trip_id=1, pickup_time="2023-01-01"),
            Row(trip_id=2, pickup_time="2023-01-02"),
        ]
    )
    check = ColumnsAreCompleteCheck(check_id="check", columns=["trip_id", "pickup_time"])
    result = check._evaluate_logic(df)

    assert result.passed is True
    assert result.metrics["null_counts"] == {"trip_id": 0, "pickup_time": 0}


def test_columns_are_complete_check_fails_when_any_null(spark: SparkSession) -> None:
    """
    Fails when at least one specified column contains nulls.
    """
    df = spark.createDataFrame(
        [
            Row(trip_id=1, pickup_time=None),
            Row(trip_id=None, pickup_time="2023-01-02"),
        ]
    )
    check = ColumnsAreCompleteCheck(check_id="check", columns=["trip_id", "pickup_time"])
    result = check._evaluate_logic(df)

    assert result.passed is False
    assert result.metrics["null_counts"] == {"trip_id": 1, "pickup_time": 1}


def test_columns_are_complete_check_missing_column_raises(spark: SparkSession) -> None:
    """
    Raises MissingColumnError if a specified column is not present in the DataFrame.
    """
    df = spark.createDataFrame([Row(trip_id=1, pickup_time="2023-01-01")])
    check = ColumnsAreCompleteCheck(check_id="check", columns=["trip_id", "missing"])

    with pytest.raises(MissingColumnError):
        check._evaluate_logic(df)
