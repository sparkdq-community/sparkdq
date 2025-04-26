"""
Tests for the evaluate() method of BaseAggregateCheck.

This module verifies that the evaluation interface wraps the core logic
correctly and returns a complete, standardized result object.

The test uses a dummy implementation with fixed metrics to isolate
and inspect the result structure.

This ensures:
- Consistency of result schema
- Proper mapping of check metadata (name, severity, message, parameters)
- Compatibility with reporting and engine logic
"""

from pyspark.sql import DataFrame, SparkSession

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.check_results import AggregateCheckResult, AggregateEvaluationResult
from sparkdq.core.severity import Severity


class DummyAggregateCheck(BaseAggregateCheck):
    """
    Dummy implementation of BaseAggregateCheck for testing evaluate().
    """

    def __init__(self, check_id: str, threshold: int, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.threshold = threshold

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        return AggregateEvaluationResult(passed=True, metrics={"threshold": self.threshold})


def test_evaluate_returns_aggregate_check_result(spark: SparkSession) -> None:
    """
    Validates that `evaluate()` returns a correctly constructed AggregateCheckResult
    including metadata and the result from `_evaluate_logic()`.

    Given a dummy check with a static threshold,
    the result should include the check name, severity, parameters, and metrics.
    """
    # Arrange: Create a dummy DataFrame and dummy check
    df = spark.range(5)
    check_id = "test"
    check = DummyAggregateCheck(check_id=check_id, threshold=100, severity=Severity.WARNING)

    # Act: Evaluate the check
    result: AggregateCheckResult = check.evaluate(df)

    # Assert: Result structure is correct
    assert isinstance(result, AggregateCheckResult)
    assert result.passed is True
    assert result.check_id == check_id
    assert result.severity == Severity.WARNING
    assert result.check == "DummyAggregateCheck"
    assert result.parameters == {"threshold": 100}
    assert result.metrics == {"threshold": 100}
