from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class RowCountExactCheck(BaseAggregateCheck):
    """
    Aggregate-level data quality check that ensures a DataFrame contains exactly a specified number of rows.

    This check is useful for strict data contract validations where the row count must match a known value,
    such as reference datasets, test snapshots, or controlled static inputs.

    Attributes:
        expected_count (int): The exact number of rows expected.
    """

    def __init__(self, check_id: str, expected_count: int, severity: Severity = Severity.CRITICAL):
        """
        Initialize a new RowCountExactCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            expected_count (int): The required number of rows.
            severity (Severity, optional): The severity level to assign if the check fails.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.expected_count = expected_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate the row count of the given DataFrame against the exact expected count.

        If the row count equals `expected_count`, the check passes.
        Otherwise, it fails and the actual count is included in the result metrics.

        Args:
            df (DataFrame): The Spark DataFrame to evaluate.

        Returns:
            AggregateEvaluationResult: An object indicating the outcome of the check,
            including relevant row count metrics.
        """
        actual = df.count()
        passed = actual == self.expected_count
        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "actual_row_count": actual,
                "expected_row_count": self.expected_count,
            },
        )


@register_check_config(check_name="row-count-exact-check")
class RowCountExactCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for the RowCountExactCheck.

    This configuration defines an exact row count requirement for a dataset. It ensures that the
    ``expected_count`` parameter is provided and is non-negative.

    Attributes:
        expected_count (int): The exact number of rows expected in the dataset.
    """

    check_class = RowCountExactCheck
    expected_count: int = Field(..., description="Exact number of rows required")

    @model_validator(mode="after")
    def validate_expected(self) -> "RowCountExactCheckConfig":
        """
        Validate that the configured expected_count is greater than 0.

        Returns:
            RowCountExactCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If ``expected_count`` is negative.
        """
        if self.expected_count < 0:
            raise InvalidCheckConfigurationError(
                f"expected_count ({self.expected_count}) must be zero or positive"
            )
        return self
