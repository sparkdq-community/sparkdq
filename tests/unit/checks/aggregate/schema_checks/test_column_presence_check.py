import pytest
from pyspark.sql import SparkSession

from sparkdq.checks.aggregate.schema_checks.column_presence_check import (
    ColumnPresenceCheck,
    ColumnPresenceCheckConfig,
)


def test_column_presence_check_passes_when_all_required_columns_exist(spark: SparkSession) -> None:
    """
    Validates that ColumnPresenceCheck passes when all required columns are present in the DataFrame.
    """
    # Arrange
    df = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "amount"])
    check = ColumnPresenceCheck(check_id="test", required_columns=["id", "name"])

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is True
    assert result.metrics["missing_columns"] == []


def test_column_presence_check_fails_when_columns_are_missing(spark: SparkSession) -> None:
    """
    Validates that ColumnPresenceCheck fails when one or more required columns are missing from the DataFrame.
    """
    # Arrange
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    check = ColumnPresenceCheck(check_id="test", required_columns=["id", "amount"])

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is False
    assert result.metrics["missing_columns"] == ["amount"]


def test_column_presence_check_with_multiple_missing_columns(spark: SparkSession) -> None:
    """
    Validates that ColumnPresenceCheck correctly reports multiple missing columns.
    """
    # Arrange
    df = spark.createDataFrame([(1,)], ["id"])
    check = ColumnPresenceCheck(check_id="test", required_columns=["id", "name", "amount"])

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is False
    assert sorted(result.metrics["missing_columns"]) == ["amount", "name"]


@pytest.mark.parametrize(
    "required_columns, expected_passed",
    [
        (["id"], True),
        (["name"], False),
        (["id", "name"], False),
    ],
)
def test_column_presence_check_various_cases(
    spark: SparkSession, required_columns: list[str], expected_passed: bool
) -> None:
    """
    Validates that ColumnPresenceCheck behaves correctly for various required column configurations.
    """
    # Arrange
    df = spark.createDataFrame([(1,)], ["id"])
    check = ColumnPresenceCheck(check_id="test", required_columns=required_columns)

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is expected_passed


def test_column_presence_check_config_accepts_valid_required_columns() -> None:
    """
    Validates that ColumnPresenceCheckConfig is created successfully when required_columns is provided.
    """
    # Arrange & Act
    config = ColumnPresenceCheckConfig(check_id="test", required_columns=["id", "name"])

    # Assert
    assert config.required_columns == ["id", "name"]
    assert config.check_class == ColumnPresenceCheck
