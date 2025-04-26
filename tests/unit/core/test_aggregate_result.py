from sparkdq.core.check_results import AggregateCheckResult, AggregateEvaluationResult
from sparkdq.core.severity import Severity


def test_check_evaluation_result_to_dict() -> None:
    """
    Validates that CheckEvaluationResult correctly serializes its fields to a dictionary.

    Given a passed status and a metrics dictionary,
    the output of `to_dict()` should match the expected structure.
    """
    # Arrange
    result = AggregateEvaluationResult(passed=True, metrics={"row_count": 100})

    # Act
    result_dict = result.to_dict()

    # Assert
    assert result_dict == {"passed": True, "metrics": {"row_count": 100}}


def test_aggregate_check_result_fields() -> None:
    """
    Validates that AggregateCheckResult stores metadata and evaluation results correctly.

    Given values for all fields,
    the object should expose them via attributes as expected.
    """
    # Arrange
    evaluation = AggregateEvaluationResult(passed=False, metrics={"min_value": 3})
    result = AggregateCheckResult(
        check="min-value-check",
        check_id="test",
        severity=Severity.CRITICAL,
        parameters={"column": "amount", "threshold": 5},
        result=evaluation,
    )

    # Assert: Direct field access
    assert result.check == "min-value-check"
    assert result.check_id == "test"
    assert result.severity == Severity.CRITICAL
    assert result.parameters == {"column": "amount", "threshold": 5}
    assert result.result == evaluation


def test_aggregate_check_result_properties() -> None:
    """
    Validates that the `passed` and `metrics` properties delegate to the embedded CheckEvaluationResult.

    These shortcuts simplify downstream reporting logic.
    """
    # Arrange
    evaluation = AggregateEvaluationResult(passed=True, metrics={"max": 999})
    result = AggregateCheckResult(
        check="max-check",
        check_id="test",
        severity=Severity.WARNING,
        parameters={"column": "total"},
        result=evaluation,
    )

    # Assert: Property passthrough
    assert result.passed is True
    assert result.metrics == {"max": 999}


def test_aggregate_check_result_to_dict() -> None:
    """
    Validates that AggregateCheckResult serializes itself (and its nested result)
    into a complete dictionary structure.

    This ensures compatibility with loggers, reporters, or API outputs.
    """
    # Arrange
    evaluation = AggregateEvaluationResult(passed=False, metrics={"ratio": 0.82})
    result = AggregateCheckResult(
        check="ratio-check",
        check_id="test",
        severity=Severity.WARNING,
        parameters={"min_ratio": 0.85},
        result=evaluation,
    )

    # Act
    result_dict = result.to_dict()

    # Assert: Full nested structure
    assert result_dict == {
        "check": "ratio-check",
        "check-id": "test",
        "severity": "warning",
        "parameters": {"min_ratio": 0.85},
        "result": {"passed": False, "metrics": {"ratio": 0.82}},
    }
