from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class RowCountMinCheck(BaseAggregateCheck):
    """
    Aggregate-level data quality check that ensures a DataFrame contains at least a minimum number of rows.

    It is evaluated once per DataFrame and produces a pass/fail result
    along with row count metrics.

    Attributes:
        min_count (int): The minimum acceptable number of rows.
    """

    def __init__(self, check_id: str, min_count: int, severity: Severity = Severity.CRITICAL):
        """
        Initialize a new RowCountMinCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            min_count (int): The minimum allowed number of rows.
            severity (Severity, optional): The severity level to assign if the check fails.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.min_count = min_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate the row count of the given DataFrame against the defined minimum.

        If the row count is at least `min_count`, the check passes.
        Otherwise, it fails and the actual count is included in the result metrics.

        Args:
            df (DataFrame): The Spark DataFrame to evaluate.

        Returns:
            AggregateEvaluationResult: An object indicating the outcome of the check,
            including relevant row count metrics.
        """
        actual = df.count()
        passed = actual >= self.min_count
        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "actual_row_count": actual,
                "min_expected": self.min_count,
            },
        )


@register_check_config(check_name="row-count-min-check")
class RowCountMinCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for the RowCountMinCheck.

    This configuration defines a minimum row count requirement for a dataset. It ensures that the
    ``min_count`` parameter is provided and is non-negative.

    Attributes:
        min_count (int): Minimum number of rows expected in the dataset.
    """

    check_class = RowCountMinCheck
    min_count: int = Field(..., description="Minimum number of rows expected")

    @model_validator(mode="after")
    def validate_min(self) -> "RowCountMinCheckConfig":
        """
        Validate that the configured ``min_count`` is greater than 0.

        Returns:
            RowCountMinCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If ``min_count`` is negative.
        """
        if self.min_count <= 0:
            raise InvalidCheckConfigurationError(f"min_count ({self.min_count}) must be greater than 0")
        return self
