from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class CompletenessRatioCheck(BaseAggregateCheck):
    """
    Aggregate-level check that verifies whether the proportion of non-null values in a column
    meets or exceeds a specified threshold.

    This check is useful for detecting partially complete data in optional or semi-required columns.
    A dataset fails this check if the completeness ratio (non-null values / total rows) is too low.
    """

    def __init__(self, check_id: str, column: str, min_ratio: float, severity: Severity = Severity.CRITICAL):
        """
        Initialize a new CompletenessRatioCheck.

        Args:
            check_id (str): Unique identifier for the check.
            column (str): Name of the column to assess.
            min_ratio (float): Minimum allowed ratio of non-null values (0 to 1).
            severity (Severity, optional): Severity level of the check result.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.min_ratio = min_ratio

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate the completeness ratio (non-null / total) for the target column.

        Args:
            df (DataFrame): Spark DataFrame.

        Returns:
            AggregateEvaluationResult: Check result with ratio metrics and pass/fail status.
        """
        total_count = df.count()
        non_null_count = df.filter(F.col(self.column).isNotNull()).count()

        actual_ratio = non_null_count / total_count if total_count > 0 else 1.0
        passed = actual_ratio >= self.min_ratio

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "column": self.column,
                "total_count": total_count,
                "non_null_count": non_null_count,
                "min_required_ratio": self.min_ratio,
                "actual_ratio": actual_ratio,
            },
        )


@register_check_config(check_name="completeness-ratio-check")
class CompletenessRatioCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration for CompletenessRatioCheck.

    Attributes:
        column (str): Column name to assess.
        min_ratio (float): Minimum allowed non-null ratio (between 0.0 and 1.0).
    """

    check_class = CompletenessRatioCheck

    column: str = Field(..., description="The name of the column to check for completeness")
    min_ratio: float = Field(
        ...,
        alias="min-ratio",
        description="Minimum ratio of non-null values (0.0–1.0)",
    )

    @model_validator(mode="after")
    def validate_threshold(self) -> "CompletenessRatioCheckConfig":
        """
        Ensures the min_ratio is between 0 and 1 (inclusive).
        """
        if not (0.0 <= self.min_ratio <= 1.0):
            raise InvalidCheckConfigurationError(
                f"min-ratio must be between 0.0 and 1.0 (got {self.min_ratio})"
            )
        return self
