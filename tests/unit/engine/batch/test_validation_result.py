"""
Unit tests for BatchValidationResult, which encapsulates the outcome
of a batch data quality run.

These tests validate:
- Filtering logic for passed, failed, and warning rows
- The inclusion of appropriate columns in each case
- The structure and contents of the _dq_errors array
- Correct computation of summary statistics

BatchValidationResult is the main interface between validation output
and consumers of clean or dirty data, making these tests critical.
"""

import pytest
from pyspark.sql import Row, SparkSession

from sparkdq.core.severity import Severity
from sparkdq.engine.batch.validation_result import BatchValidationResult


@pytest.fixture
def sample_df(spark: SparkSession):
    """
    Provides a small annotated DataFrame for testing pass, fail, and warning behavior.
    """
    return spark.createDataFrame(
        [
            Row(id=1, value="valid", _dq_passed=True, _dq_errors=[]),
            Row(id=2, value="warn", _dq_passed=True, _dq_errors=[{"severity": "warning", "check": "dummy"}]),
            Row(
                id=3, value="fail", _dq_passed=False, _dq_errors=[{"severity": "critical", "check": "dummy"}]
            ),
        ]
    )


def test_pass_df_returns_only_valid_rows(sample_df) -> None:
    """
    Validates that pass_df() returns only rows where _dq_passed is True,
    and only includes the original input columns.
    """
    # Arrange
    result = BatchValidationResult(
        df=sample_df,
        aggregate_results=[],
        input_columns=["id", "value"],
    )

    # Act
    df_pass = result.pass_df()
    ids = [row["id"] for row in df_pass.collect()]

    # Assert
    assert set(ids) == {1, 2}
    assert df_pass.columns == ["id", "value"]


def test_fail_df_returns_only_failed_rows(sample_df) -> None:
    """
    Validates that fail_df() returns rows with _dq_passed == False
    and includes _dq_errors and _dq_passed columns.

    Also checks that the _dq_errors column contains the expected critical error.
    """
    # Arrange
    result = BatchValidationResult(
        df=sample_df,
        aggregate_results=[],
        input_columns=["id", "value"],
    )

    # Act
    df_fail = result.fail_df()
    rows = df_fail.collect()

    # Assert
    assert len(rows) == 1
    row = rows[0]

    assert row["id"] == 3
    assert "_dq_errors" in df_fail.columns
    assert "_dq_passed" in df_fail.columns

    # Validate _dq_errors content
    errors = row["_dq_errors"]
    assert isinstance(errors, list)
    assert len(errors) == 1
    assert errors[0]["severity"] == Severity.CRITICAL.value
    assert errors[0]["check"] == "dummy"


def test_warn_df_returns_only_warnings(sample_df) -> None:
    """
    Validates that warn_df() returns rows that passed,
    but contain warning-level entries in _dq_errors.

    Also verifies the warning entry structure in _dq_errors.
    """
    # Arrange
    result = BatchValidationResult(
        df=sample_df,
        aggregate_results=[],
        input_columns=["id", "value"],
    )

    # Act
    df_warn = result.warn_df()
    rows = df_warn.collect()

    # Assert
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == 2
    assert "_dq_errors" in df_warn.columns

    # Validate _dq_errors content
    errors = row["_dq_errors"]
    assert isinstance(errors, list)
    assert len(errors) == 1
    assert errors[0]["severity"] == Severity.WARNING.value
    assert errors[0]["check"] == "dummy"


def test_summary_computes_correct_statistics(sample_df) -> None:
    """
    Validates that summary() computes total, passed, failed, and warning counts,
    as well as the pass rate, based on annotated DataFrame content.
    """
    # Arrange
    result = BatchValidationResult(
        df=sample_df,
        aggregate_results=[],
        input_columns=["id", "value"],
    )

    # Act
    summary = result.summary()

    # Assert
    assert summary.total_records == 3
    assert summary.passed_records == 2
    assert summary.failed_records == 1
    assert summary.warning_records == 1
    assert 0.66 <= summary.pass_rate <= 0.67
