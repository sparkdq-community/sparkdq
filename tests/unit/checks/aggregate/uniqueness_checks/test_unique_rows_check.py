import pytest
from pyspark.sql import Row, SparkSession

from sparkdq.checks.aggregate.uniqueness_checks.unique_rows_check import (
    UniqueRowsCheck,
    UniqueRowsCheckConfig,
)
from sparkdq.exceptions import MissingColumnError


def test_unique_rows_check_passes_when_all_rows_are_unique(spark: SparkSession) -> None:
    """
    Validates that UniqueRowsCheck passes when all rows are unique.

    Given a DataFrame with 3 unique rows, the check should pass with zero duplicate groups.
    """
    # Arrange
    df = spark.createDataFrame(
        [
            Row(id=1, name="Alice"),
            Row(id=2, name="Bob"),
            Row(id=3, name="Charlie"),
        ]
    )
    check = UniqueRowsCheck(check_id="test")

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is True
    assert result.metrics["duplicate_row_groups"] == 0


def test_unique_rows_check_fails_when_duplicates_exist(spark: SparkSession) -> None:
    """
    Validates that UniqueRowsCheck fails when duplicate rows exist.

    Given a DataFrame with 2 identical rows, the check should fail.
    """
    # Arrange
    df = spark.createDataFrame(
        [
            Row(id=1, name="Alice"),
            Row(id=1, name="Alice"),
            Row(id=2, name="Bob"),
        ]
    )
    check = UniqueRowsCheck(check_id="test")

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is False
    assert result.metrics["duplicate_row_groups"] == 1


def test_unique_rows_check_with_subset_columns(spark: SparkSession) -> None:
    """
    Validates that UniqueRowsCheck can use a subset of columns for uniqueness.

    Given multiple rows with duplicate 'id' values, the check should detect duplicates
    based only on 'id'.
    """
    # Arrange
    df = spark.createDataFrame(
        [
            Row(id=1, name="Alice"),
            Row(id=1, name="Bob"),
            Row(id=2, name="Alice"),
            Row(id=2, name="Bob"),
        ]
    )
    check = UniqueRowsCheck(check_id="test", subset_columns=["id"])

    # Act
    result = check._evaluate_logic(df)

    # Assert
    assert result.passed is False
    assert result.metrics["duplicate_row_groups"] == 2


def test_unique_rows_check_fails_with_missing_column(spark: SparkSession) -> None:
    """
    Validates that UniqueRowsCheck raises MissingColumnError if a specified column is missing.
    """
    # Arrange
    df = spark.createDataFrame([Row(id=1)])
    check = UniqueRowsCheck(check_id="test", subset_columns=["missing"])

    # Act & Assert
    with pytest.raises(MissingColumnError):
        check._evaluate_logic(df)


def test_unique_rows_check_config_creates_check() -> None:
    """
    Validates that UniqueRowsCheckConfig creates a proper UniqueRowsCheck instance via to_check().
    """
    # Arrange
    config = UniqueRowsCheckConfig(check_id="test", subset_columns=["id"])

    # Act
    check = config.to_check()

    # Assert
    assert isinstance(check, UniqueRowsCheck)
    assert check.check_id == "test"
    assert check.subset_columns == ["id"]
