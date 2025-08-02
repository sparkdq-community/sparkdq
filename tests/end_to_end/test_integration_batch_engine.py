"""
End-to-end integration tests for the complete data quality validation pipeline.

This test suite validates the full integration of framework components including:
- Configuration-driven check definition through external dictionaries
- Check instantiation and registration via CheckSet and CheckFactory
- Complete validation execution through BatchDQEngine
- Result processing and summary generation for validation outcomes

These tests ensure that all framework components work together seamlessly
in realistic validation scenarios, providing confidence in production deployments.
"""

import pytest
from pyspark.sql import Row, SparkSession

from sparkdq.engine import BatchDQEngine, BatchValidationResult
from sparkdq.management import CheckSet


@pytest.fixture
def input_df(spark: SparkSession):
    return spark.createDataFrame(
        [
            Row(id=1, name="Alice"),
            Row(id=2, name=None),
            Row(id=3, name="Bob"),
        ]
    )


def test_full_batch_flow_with_multiple_checks(input_df) -> None:
    """
    Verify complete end-to-end validation pipeline functionality with multiple check types.

    This integration test validates the entire workflow from configuration loading
    through result generation, ensuring all framework components collaborate
    correctly in realistic validation scenarios.
    """
    # Arrange
    config_dicts = [
        {"check-id": "test1", "check": "null-check", "columns": ["name"], "severity": "warning"},
        {
            "check-id": "test2",
            "check": "row-count-between-check",
            "min_count": 4,
            "max_count": 5,
            "severity": "critical",
        },
    ]

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config_dicts)

    engine = BatchDQEngine(check_set)

    # Act
    result: BatchValidationResult = engine.run_batch(input_df)

    # Assert: Fail due to row count; 1 warning row due to null
    summary = result.summary()
    assert summary.total_records == 3
    assert summary.passed_records == 0
    assert summary.failed_records == 3

    failed_ids = {row["id"] for row in result.fail_df().collect()}
    assert failed_ids == {1, 2, 3}
