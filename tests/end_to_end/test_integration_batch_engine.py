"""
Integration tests for the full DQ pipeline using BatchDQEngine and CheckSet.

These tests simulate real-world usage:
- Configuring checks via config dictionaries
- Executing a validation run on sample data
- Validating outputs (fail/warn/pass/summary)

This ensures components like CheckSet, CheckFactory, BatchCheckRunner,
and BatchDQEngine work together as expected.
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
    Runs a full integration test:
    - Loads checks via config dicts
    - Executes them via BatchDQEngine
    - Validates DataFrame annotation and summary output
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
