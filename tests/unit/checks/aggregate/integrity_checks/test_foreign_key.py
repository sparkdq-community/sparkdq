import pytest
from pyspark.sql import Row

from sparkdq.checks.aggregate.integrity_checks.foreign_key_check import ForeignKeyCheck
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError


def test_foreign_key_check_passes_when_all_values_match(spark):
    """
    Validates that ForeignKeyCheck passes when all values in the source column
    are present in the reference dataset.
    """
    # Arrange
    df = spark.createDataFrame([Row(id=1), Row(id=2)])
    ref_df = spark.createDataFrame([Row(id=1), Row(id=2), Row(id=3)])

    check = ForeignKeyCheck(check_id="fk-pass", column="id", reference_dataset="ref", reference_column="id")
    check.inject_reference_datasets({"ref": ref_df})

    # Act
    result = check.evaluate(df)

    # Assert
    assert result.passed
    assert result.metrics["missing_foreign_keys"] == 0
    assert result.metrics["total_rows"] == 2
    assert result.metrics["missing_ratio"] == 0.0


def test_foreign_key_check_fails_when_values_missing(spark):
    """
    Validates that ForeignKeyCheck fails when some values in the source column
    are not present in the reference dataset.
    """
    # Arrange
    df = spark.createDataFrame([Row(id=1), Row(id=99)])
    ref_df = spark.createDataFrame([Row(id=1), Row(id=2)])

    check = ForeignKeyCheck(
        check_id="fk-fail",
        column="id",
        reference_dataset="ref",
        reference_column="id",
        severity=Severity.WARNING,
    )
    check.inject_reference_datasets({"ref": ref_df})

    # Act
    result = check.evaluate(df)

    # Assert
    assert not result.passed
    assert result.metrics["missing_foreign_keys"] == 1
    assert result.metrics["total_rows"] == 2
    assert result.metrics["missing_ratio"] == 0.5


def test_foreign_key_check_raises_when_source_column_missing(spark):
    """
    Validates that ForeignKeyCheck raises an error if the source column is missing.
    """
    # Arrange
    df = spark.createDataFrame([Row(x=1)])
    ref_df = spark.createDataFrame([Row(id=1)])

    check = ForeignKeyCheck(check_id="fk-error", column="id", reference_dataset="ref", reference_column="id")
    check.inject_reference_datasets({"ref": ref_df})

    # Act & Assert
    with pytest.raises(MissingColumnError):
        check.evaluate(df)


def test_foreign_key_check_raises_when_reference_column_missing(spark):
    """
    Validates that ForeignKeyCheck raises an error if the reference column is missing.
    """
    # Arrange
    df = spark.createDataFrame([Row(id=1)])
    ref_df = spark.createDataFrame([Row(x=1)])

    check = ForeignKeyCheck(
        check_id="fk-error-ref", column="id", reference_dataset="ref", reference_column="id"
    )
    check.inject_reference_datasets({"ref": ref_df})

    # Act & Assert
    with pytest.raises(MissingColumnError):
        check.evaluate(df)
