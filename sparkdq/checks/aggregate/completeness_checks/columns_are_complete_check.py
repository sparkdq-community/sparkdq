from typing import Any, List

from pydantic import Field
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.observable_check import ObservableAggregateCheck
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class ColumnsAreCompleteCheck(BaseAggregateCheck, ObservableAggregateCheck):
    """
    Aggregate-level data quality check that ensures the specified columns are fully populated.

    A dataset fails the check if **any** of the listed columns contain one or more null values.

    This check is useful for enforcing strict completeness constraints on required fields.

    Attributes:
        columns (List[str]): The list of columns that must not contain null values.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a ColumnsAreCompleteCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): Names of the columns to validate for full completeness.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns

    def aggregations(self) -> dict[str, Column]:
        # Metric key per column: "null__{col}" to avoid collisions with other checks
        return {f"null__{col}": F.sum(F.col(col).isNull().cast("int")) for col in self.columns}

    def _evaluate_from_agg_results(self, results: dict[str, Any]) -> AggregateEvaluationResult:
        null_counts = {col: results[f"null__{col}"] for col in self.columns}
        failed_columns = [col for col, null_count in null_counts.items() if null_count > 0]
        return AggregateEvaluationResult(
            passed=len(failed_columns) == 0,
            metrics={
                "null_counts": null_counts,
                "failed_columns": failed_columns,
            },
        )

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        row = df.agg(*[expr.alias(k) for k, expr in self.aggregations().items()]).first()
        return self._evaluate_from_agg_results(row.asDict())  # type: ignore[union-attr]


@register_check_config(check_name="columns-are-complete-check")
class ColumnsAreCompleteCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for the ColumnsAreCompleteCheck.

    This configuration defines a completeness requirement for multiple columns.
    The check fails if **any** of the specified columns contain null values.

    Attributes:
        columns (List[str]): List of required columns that must be fully populated.
    """

    check_class = ColumnsAreCompleteCheck
    columns: List[str] = Field(..., description="List of columns that must contain no null values")
