"""
Tests for BatchDQEngine, the batch-mode interface for running data quality checks.

These tests verify:
- That the engine refuses to run without a registered CheckSet
- That registered row-level checks are executed correctly
- That the resulting DataFrame contains the expected validation metadata

BatchDQEngine serves as the entry point for batch validation pipelines.
"""

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.severity import Severity
from sparkdq.engine.batch.dq_engine import BatchDQEngine
from sparkdq.engine.batch.validation_result import BatchValidationResult
from sparkdq.exceptions import MissingCheckSetError
from sparkdq.management.check_set import CheckSet


class DummyRowCheck(BaseRowCheck):
    def __init__(self, check_id: str, column: str, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column

    def validate(self, df: DataFrame) -> DataFrame:
        return df.withColumn(self.check_id, df[self.column] == "invalid")


class DummyCheckSet(CheckSet):
    def get_all(self) -> list:
        return [DummyRowCheck(check_id="test", column="value", severity=Severity.CRITICAL)]


def test_run_batch_raises_if_checkset_missing(spark: SparkSession) -> None:
    """
    Validates that run_batch() raises MissingCheckSetError
    if no CheckSet has been assigned to the engine.
    """
    # Arrange
    engine = BatchDQEngine()

    # Act & Assert
    with pytest.raises(MissingCheckSetError):
        engine.run_batch(spark.range(3))


def test_run_batch_returns_validation_result(spark: SparkSession) -> None:
    """
    Validates that run_batch() runs all checks and returns a BatchValidationResult.

    The result should contain:
    - The original input columns
    - Annotated validation columns (_dq_passed, _dq_errors)
    - Row failure detection based on severity
    """
    # Arrange: Prepare input DataFrame and engine with DummyCheck
    df = spark.createDataFrame(
        [
            Row(id=1, value="ok"),
            Row(id=2, value="invalid"),
        ]
    )

    engine = BatchDQEngine(DummyCheckSet())

    # Act
    result = engine.run_batch(df)

    # Assert
    assert isinstance(result, BatchValidationResult)
    assert result.input_columns == ["id", "value"]

    result_df = result.df.orderBy("id").collect()
    assert result_df[0]["_dq_passed"] is True
    assert result_df[1]["_dq_passed"] is False
    assert "_dq_errors" in result.df.columns
