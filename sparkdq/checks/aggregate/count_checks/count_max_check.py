from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class RowCountMaxCheck(BaseAggregateCheck):
    """
    Aggregate-level data quality check that ensures a DataFrame does not exceed a maximum number of rows.

    It is evaluated once per DataFrame and produces a pass/fail result
    along with row count metrics.

    Attributes:
        max_count (int): The maximum acceptable number of rows.
    """

    def __init__(self, check_id: str, max_count: int, severity: Severity = Severity.CRITICAL):
        """
        Initialize a new RowCountMaxCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            max_count (int): The maximum allowed number of rows.
            severity (Severity, optional): The severity level to assign if the check fails.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.max_count = max_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate the row count of the given DataFrame against the defined maximum.

        If the row count is less than or equal to `max_count`, the check passes.
        Otherwise, it fails and the actual count is included in the result metrics.

        Args:
            df (DataFrame): The Spark DataFrame to evaluate.

        Returns:
            AggregateEvaluationResult: An object indicating the outcome of the check,
            including relevant row count metrics.
        """
        actual = df.count()
        passed = actual <= self.max_count
        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "actual_row_count": actual,
                "max_expected": self.max_count,
            },
        )


@register_check_config(check_name="row-count-max-check")
class RowCountMaxCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for the RowCountMaxCheck.

    This configuration defines a maximum row count requirement for a dataset. It ensures that the
    ``max_count`` parameter is provided and has a positive value.

    Attributes:
        max_count (int): Maximum number of rows allowed in the dataset.
    """

    check_class = RowCountMaxCheck
    max_count: int = Field(..., description="Maximum number of rows allowed")

    @model_validator(mode="after")
    def validate_max(self) -> "RowCountMaxCheckConfig":
        """
        Validate that the configured ``max_count`` is greater than 0.

        Returns:
            RowCountMaxCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If ``max_count`` is not greater than 0.
        """
        if self.max_count <= 0:
            raise InvalidCheckConfigurationError(f"max_count ({self.max_count}) must be greater than 0")
        return self
