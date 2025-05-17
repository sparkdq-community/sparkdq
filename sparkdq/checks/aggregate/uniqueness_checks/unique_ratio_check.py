from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class UniqueRatioCheck(BaseAggregateCheck):
    """
    Aggregate-level check that verifies whether the ratio of distinct values in a given column
    meets or exceeds a defined threshold.

    This check helps detect columns with low uniqueness or excessive duplication.
    """

    def __init__(self, check_id: str, column: str, min_ratio: float, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.min_ratio = min_ratio

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Calculates the ratio of distinct, non-null values in the column relative to total row count.

        Fails if the ratio is less than `min_ratio`.
        """
        total_count = df.count()
        unique_count = df.select(self.column).distinct().na.drop().count()

        actual_ratio = unique_count / total_count if total_count > 0 else 0.0
        passed = actual_ratio >= self.min_ratio

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "column": self.column,
                "total_count": total_count,
                "unique_count": unique_count,
                "actual_ratio": actual_ratio,
                "min_required_ratio": self.min_ratio,
            },
        )


@register_check_config(check_name="unique-ratio-check")
class UniqueRatioCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for the UniqueRatioCheck.

    Attributes:
        column (str): The column to check for uniqueness.
        min_ratio (float): The minimum acceptable ratio of distinct values (0.0 - 1.0).
    """

    check_class = UniqueRatioCheck
    column: str = Field(..., description="Target column to assess uniqueness ratio")
    min_ratio: float = Field(
        ..., description="Minimum required ratio of unique (non-null) values", alias="min-ratio"
    )

    @model_validator(mode="after")
    def validate_ratio(self) -> "UniqueRatioCheckConfig":
        if not 0.0 <= self.min_ratio <= 1.0:
            raise InvalidCheckConfigurationError(f"min_ratio ({self.min_ratio}) must be between 0.0 and 1.0")
        return self
