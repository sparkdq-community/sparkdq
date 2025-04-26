from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.factory.check_config_registry import register_check_config


class RowCountBetweenCheck(BaseAggregateCheck):
    """
    Aggregate-level data quality check that ensures the total number of rows in a DataFrame
    is within a specified inclusive range [min_count, max_count].

    It is evaluated once per DataFrame and produces a pass/fail result
    along with row count metrics.

    Attributes:
        min_count (int): The minimum acceptable number of rows.
        max_count (int): The maximum acceptable number of rows.
    """

    def __init__(self, check_id: str, min_count: int, max_count: int, severity: Severity = Severity.CRITICAL):
        """
        Initialize a new RowCountBetweenCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            min_count (int): The minimum allowed number of rows.
            max_count (int): The maximum allowed number of rows.
            severity (Severity, optional): The severity level to assign if the check fails.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.min_count = min_count
        self.max_count = max_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate the row count of the given DataFrame against the defined bounds.

        If the row count is within the range [min_count, max_count], the check passes.
        Otherwise, it fails and the actual count is included in the result metrics.

        Args:
            df (DataFrame): The Spark DataFrame to evaluate.

        Returns:
            AggregateEvaluationResult: An object indicating the outcome of the check,
            including relevant row count metrics.
        """
        actual = df.count()
        passed = self.min_count <= actual <= self.max_count
        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "actual_row_count": actual,
                "min_expected": self.min_count,
                "max_expected": self.max_count,
            },
        )


@register_check_config(check_name="row-count-between-check")
class RowCountBetweenCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for the RowCountBetweenCheck.

    This config is used to define acceptable row count bounds in a
    data validation pipeline. It ensures that:
    - both `min_count` and `max_count` are provided,
    - and that `min_count <= max_count`.

    It is typically used when defining checks via JSON, YAML, or dict-based configs.

    Attributes:
        min_count (int): Minimum number of rows expected in the dataset.
        max_count (int): Maximum number of rows allowed in the dataset.
    """

    check_class = RowCountBetweenCheck
    min_count: int = Field(..., description="Minimum number of rows expected")
    max_count: int = Field(..., description="Maximum number of rows allowed")

    @model_validator(mode="after")
    def validate_range(self) -> "RowCountBetweenCheckConfig":
        """
        Validate the logical consistency of the configured bounds.

        This method ensures that `min_count` is not greater than `max_count`.
        If violated, a configuration-level exception is raised immediately
        to prevent runtime failures.

        Returns:
            RowCountBetweenCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If `min_count > max_count`.
        """
        if self.min_count < 0:
            raise InvalidCheckConfigurationError(
                f"min_count ({self.min_count}) must be greater than or equal to 0"
            )
        if self.min_count > self.max_count:
            raise InvalidCheckConfigurationError(
                f"min_count ({self.min_count}) must not be greater than max_count ({self.max_count})"
            )
        return self
