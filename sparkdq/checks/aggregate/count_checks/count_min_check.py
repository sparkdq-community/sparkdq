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
    Dataset-level validation check that enforces minimum row count thresholds.

    Validates that the total number of records in a dataset meets or exceeds a
    configured minimum threshold, ensuring adequate data volume for downstream
    processing. This check is essential for detecting incomplete data loads,
    processing failures, or insufficient data scenarios.

    The check provides comprehensive metrics including actual counts and configured
    thresholds to support detailed analysis of validation outcomes.

    Attributes:
        min_count (int): Minimum acceptable number of records in the dataset.
    """

    def __init__(self, check_id: str, min_count: int, severity: Severity = Severity.CRITICAL):
        """
        Initialize the minimum row count validation check with threshold configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            min_count (int): Minimum acceptable number of records in the dataset.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.min_count = min_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Execute the minimum row count validation against the configured threshold.

        Computes the total row count of the dataset and evaluates it against the
        configured minimum threshold. The validation passes when the actual count
        meets or exceeds the minimum requirement.

        Args:
            df (DataFrame): The dataset to evaluate for minimum count compliance.

        Returns:
            AggregateEvaluationResult: Validation outcome including pass/fail status
                and comprehensive metrics comparing actual count to minimum threshold.
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
    Configuration schema for minimum row count validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce minimum row count thresholds. The configuration includes logical
    validation to ensure count parameters are positive and meaningful.

    Attributes:
        min_count (int): Minimum acceptable number of records in the dataset.
    """

    check_class = RowCountMinCheck
    min_count: int = Field(..., description="Minimum number of rows expected", alias="min-count")

    @model_validator(mode="after")
    def validate_min(self) -> "RowCountMinCheckConfig":
        """
        Validate the logical consistency of the configured minimum count parameter.

        Ensures that the minimum count parameter is positive and meaningful for
        dataset validation purposes. This validation prevents configuration errors
        that would result in nonsensical validation conditions.

        Returns:
            RowCountMinCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the minimum count parameter is
                zero or negative, indicating an invalid configuration.
        """
        if self.min_count <= 0:
            raise InvalidCheckConfigurationError(f"min-count ({self.min_count}) must be greater than 0")
        return self
