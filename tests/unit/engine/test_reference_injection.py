from unittest.mock import MagicMock

from sparkdq.core import Severity
from sparkdq.core.base_check import BaseAggregateCheck, IntegrityCheckMixin
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.engine.batch.check_runner import BatchCheckRunner


def test_injection_of_reference_datasets(spark) -> None:
    """
    Verifies that integrity checks receive reference datasets via injection.

    This test ensures that all checks implementing IntegrityCheckMixin have their
    `inject_reference_datasets(...)` method called during execution.
    """

    # Arrange
    df = spark.createDataFrame([(1,)], ["x"])
    reference_df = spark.createDataFrame([(1,)], ["x"])
    reference_datasets = {"ref": reference_df}

    class MockCheck(BaseAggregateCheck, IntegrityCheckMixin):
        def __init__(self) -> None:
            super().__init__(check_id="mock-check", severity=Severity.CRITICAL)
            self.inject_reference_datasets = MagicMock()

        def _evaluate_logic(self, df):
            return AggregateEvaluationResult(passed=True, metrics={})

    check = MockCheck()
    runner = BatchCheckRunner(fail_levels=[Severity.CRITICAL])

    # Act
    runner.run(df, [check], reference_datasets=reference_datasets)

    # Assert
    check.inject_reference_datasets.assert_called_once_with(reference_datasets)
