"""
Tests for BaseAggregateCheckConfig and its `.to_check()` behavior.

This module verifies that aggregate checks can be declaratively instantiated
from config classes, using Pydantic validation and class references.

These tests ensure that:
- Config objects pass parameters to their associated checks
- The check is correctly created with severity and other fields
- Developers can safely rely on this pattern in DQEngine pipelines
"""

from typing import ClassVar

from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity


class DummyAggregateCheck(BaseAggregateCheck):
    def __init__(self, check_id: str, threshold: int, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.threshold = threshold

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        return AggregateEvaluationResult(passed=True, metrics={"threshold": self.threshold})


class DummyAggregateCheckConfig(BaseAggregateCheckConfig):
    """
    Minimal example config for testing BaseAggregateCheckConfig behavior.
    """

    check_class: ClassVar[type] = DummyAggregateCheck
    threshold: int


def test_to_check_creates_aggregate_check_instance() -> None:
    """
    Validates that `to_check()` on a valid AggregateCheckConfig
    returns the correct check instance with all parameters set.

    This confirms that aggregate checks can be created declaratively from config.
    """
    # Arrange: Create config object
    check_id = "test"
    config = DummyAggregateCheckConfig(check_id=check_id, threshold=42, severity=Severity.WARNING)

    # Act: Instantiate the check
    check = config.to_check()

    # Assert: Instance is correct and properly configured
    assert isinstance(check, DummyAggregateCheck)
    assert check.threshold == 42
    assert check.check_id == check_id
    assert check.severity == Severity.WARNING
