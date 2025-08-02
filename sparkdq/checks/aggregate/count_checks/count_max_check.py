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
    Dataset-level validation check that enforces maximum row count limits.

    Validates that the total number of records in a dataset remains within a
    configured maximum threshold, preventing excessive data volume that could
    impact processing performance or storage constraints. This check is essential
    for detecting data explosion scenarios, runaway processes, or capacity limit
    violations.

    The check provides comprehensive metrics including actual counts and configured
    limits to support detailed analysis of validation outcomes.

    Attributes:
        max_count (int): Maximum acceptable number of records in the dataset.
    """

    def __init__(self, check_id: str, max_count: int, severity: Severity = Severity.CRITICAL):
        """
        Initialize the maximum row count validation check with threshold configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            max_count (int): Maximum acceptable number of records in the dataset.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.max_count = max_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Execute the maximum row count validation against the configured threshold.

        Computes the total row count of the dataset and evaluates it against the
        configured maximum threshold. The validation passes when the actual count
        remains within or below the maximum limit.

        Args:
            df (DataFrame): The dataset to evaluate for maximum count compliance.

        Returns:
            AggregateEvaluationResult: Validation outcome including pass/fail status
                and comprehensive metrics comparing actual count to maximum threshold.
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
    Configuration schema for maximum row count validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce maximum row count limits. The configuration includes logical
    validation to ensure count parameters are positive and meaningful.

    Attributes:
        max_count (int): Maximum acceptable number of records in the dataset.
    """

    check_class = RowCountMaxCheck
    max_count: int = Field(..., description="Maximum number of rows allowed", alias="max-count")

    @model_validator(mode="after")
    def validate_max(self) -> "RowCountMaxCheckConfig":
        """
        Validate the logical consistency of the configured maximum count parameter.

        Ensures that the maximum count parameter is positive and meaningful for
        dataset validation purposes. This validation prevents configuration errors
        that would result in nonsensical validation conditions.

        Returns:
            RowCountMaxCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the maximum count parameter is
                zero or negative, indicating an invalid configuration.
        """
        if self.max_count <= 0:
            raise InvalidCheckConfigurationError(f"max-count ({self.max_count}) must be greater than 0")
        return self
