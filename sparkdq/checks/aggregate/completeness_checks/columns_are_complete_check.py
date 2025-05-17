from typing import List

from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class ColumnsAreCompleteCheck(BaseAggregateCheck):
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

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate whether all specified columns are complete (contain no null values).

        If any of the specified columns contains at least one null, the check fails.

        Args:
            df (DataFrame): The input Spark DataFrame.

        Returns:
            AggregateEvaluationResult: An object indicating whether all required columns are complete.
                Includes per-column null count as metrics.
        """
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        # Count nulls per column and return as dictionary (i.e., { column_name: null_count })
        null_counts = (
            df.select(*[F.sum(F.col(col).isNull().cast("int")).alias(col) for col in self.columns])
            .first()
            .asDict()  # type: ignore
        )

        failed_columns = [col for col, null_count in null_counts.items() if null_count > 0]
        passed = len(failed_columns) == 0

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "null_counts": null_counts,
                "failed_columns": failed_columns,
            },
        )


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
