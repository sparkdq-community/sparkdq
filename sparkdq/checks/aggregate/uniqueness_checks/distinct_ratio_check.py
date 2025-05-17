from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class DistinctRatioCheck(BaseAggregateCheck):
    """
    Aggregate-level check that validates whether a column contains a sufficient ratio of distinct values.

    Attributes:
        column (str): The name of the column to evaluate.
        min_ratio (float): Minimum required ratio of distinct non-null values (between 0 and 1).
    """

    def __init__(self, check_id: str, column: str, min_ratio: float, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.min_ratio = min_ratio

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate the distinct ratio of the column against the threshold.

        Returns:
            AggregateEvaluationResult: Passed if the ratio of distinct non-null values is >= min_ratio.
        """
        non_null_df = df.select(self.column).filter(F.col(self.column).isNotNull())
        distinct_count = non_null_df.distinct().count()
        total_count = non_null_df.count()

        ratio = distinct_count / total_count if total_count > 0 else 0.0
        passed = ratio >= self.min_ratio

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "distinct_count": distinct_count,
                "row_count": total_count,
                "distinct_ratio": ratio,
                "min_expected_ratio": self.min_ratio,
            },
        )


@register_check_config(check_name="distinct-ratio-check")
class DistinctRatioCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for DistinctRatioCheck.

    Attributes:
        column (str): The column to evaluate for distinctness.
        min_ratio (float): Minimum required ratio of distinct values (between 0 and 1).
    """

    check_class = DistinctRatioCheck
    column: str = Field(..., description="Column to evaluate for distinct values")
    min_ratio: float = Field(
        ...,
        alias="min-ratio",
        description="Minimum required ratio of distinct values",
    )

    @model_validator(mode="after")
    def validate_ratio(self) -> "DistinctRatioCheckConfig":
        if not (0.0 <= self.min_ratio <= 1.0):
            raise InvalidCheckConfigurationError(f"min-ratio ({self.min_ratio}) must be between 0.0 and 1.0")
        return self
