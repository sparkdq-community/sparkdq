from pyspark.sql import DataFrame, Row, SparkSession

from sparkdq.core.base_check import BaseAggregateCheck, BaseRowCheck
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.engine.batch.check_runner import BatchCheckRunner


class DummyRowCheck(BaseRowCheck):
    def __init__(self, check_id: str, column: str, severity: Severity):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column

    def validate(self, df: DataFrame) -> DataFrame:
        return df.withColumn(self.check_id, df[self.column] == "invalid")


class DummyAggregateCheck(BaseAggregateCheck):
    def __init__(self, check_id: str, name: str, passed: bool, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self._name = name
        self._passed = passed

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        return AggregateEvaluationResult(passed=self._passed, metrics={"test": "test"})

    @property
    def name(self) -> str:
        return self._name

    @property
    def message(self) -> str:
        return f"{self.name} failed"

    def _parameters(self) -> dict:
        return {}


def test_run_with_row_and_aggregate_checks(spark: SparkSession) -> None:
    """
    Validates that BatchCheckRunner runs both row-level and aggregate-level checks.

    - Row-level check should annotate _dq_errors and _dq_passed
    - Failing aggregate check should be appended to _dq_errors and _dq_aggregate_errors
    """
    # Arrange: Input DataFrame with one invalid row
    df = spark.createDataFrame(
        [
            Row(id=1, value="valid"),
            Row(id=2, value="invalid"),
        ]
    )

    runner = BatchCheckRunner(fail_levels=[Severity.CRITICAL])

    checks = [
        DummyRowCheck(check_id="test1", column="value", severity=Severity.CRITICAL),
        DummyAggregateCheck(check_id="test2", name="agg-check", passed=False),
    ]

    # Act
    validated_df, aggregate_results = runner.run(df, checks)

    # Assert
    assert "_dq_passed" in validated_df.columns
    assert "_dq_errors" in validated_df.columns

    rows = validated_df.orderBy("id").collect()

    # Row 1: valid → passed = True
    print(rows)
    assert rows[0]["_dq_passed"] is False
    assert rows[0]["_dq_errors"]  # still has aggregate error

    # Row 2: invalid → passed = False due to row check
    assert rows[1]["_dq_passed"] is False

    # Aggregate results
    assert len(aggregate_results) == 1
    assert aggregate_results[0].check == "agg-check"
    assert aggregate_results[0].passed is False
