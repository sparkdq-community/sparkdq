from typing import List, Optional

from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class UniqueRowsCheck(BaseAggregateCheck):
    """
    Aggregate-level data quality check that ensures row-level uniqueness in the DataFrame.

    A row is considered non-unique if the same combination of values appears more than once,
    either across all columns or a user-defined subset.
    """

    def __init__(
        self,
        check_id: str,
        subset_columns: Optional[List[str]] = None,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a UniqueRowsCheck instance.

        Args:
            check_id (str): Unique identifier for the check.
            subset_columns (Optional[List[str]]): List of columns to define uniqueness.
                If None, all columns are used.
            severity (Severity): Severity level assigned if the check fails.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.subset_columns = subset_columns

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate the uniqueness of rows in the DataFrame.

        Returns:
            AggregateEvaluationResult: Indicates whether any duplicated row combinations exist,
            and reports how many were found.
        """
        cols = self.subset_columns or df.columns

        for col in cols:
            if col not in df.columns:
                raise MissingColumnError(col, df.columns)

        duplicate_groups = df.groupBy(cols).count().filter(F.col("count") > 1)

        duplicate_count = duplicate_groups.count()
        passed = duplicate_count == 0

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "duplicate_row_groups": duplicate_count,
                "checked_columns": cols,
            },
        )


@register_check_config(check_name="unique-rows-check")
class UniqueRowsCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration for the UniqueRowsCheck.

    This check verifies that no duplicate row combinations exist in the dataset.
    Uniqueness can be enforced across all columns or a selected subset.

    Attributes:
        subset_columns (Optional[List[str]]): List of columns to define uniqueness.
            If not provided, all columns are used.
    """

    check_class = UniqueRowsCheck
    subset_columns: Optional[List[str]] = Field(
        default=None,
        alias="subset-columns",
        description="List of columns used to determine row uniqueness. Defaults to all columns.",
    )
