from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class RowCountBetweenCheck(BaseAggregateCheck):
    """
    Dataset-level validation check that enforces row count boundaries.

    Validates that the total number of records in a dataset falls within a
    specified inclusive range, ensuring data volume compliance with business
    expectations. This check is essential for detecting data pipeline issues
    such as incomplete data loads, unexpected data growth, or processing failures.

    The check provides comprehensive metrics including actual counts and configured
    thresholds to support detailed analysis of validation outcomes.

    Attributes:
        min_count (int): Minimum acceptable number of records in the dataset.
        max_count (int): Maximum acceptable number of records in the dataset.
    """

    def __init__(self, check_id: str, min_count: int, max_count: int, severity: Severity = Severity.CRITICAL):
        """
        Initialize the row count boundary validation check.

        Args:
            check_id (str): Unique identifier for this check instance.
            min_count (int): Minimum acceptable number of records in the dataset.
            max_count (int): Maximum acceptable number of records in the dataset.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.min_count = min_count
        self.max_count = max_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Execute the row count validation against the configured boundaries.

        Computes the total row count of the dataset and evaluates it against the
        configured minimum and maximum thresholds. The validation passes when the
        actual count falls within the inclusive range, and fails otherwise.

        Args:
            df (DataFrame): The dataset to evaluate for row count compliance.

        Returns:
            AggregateEvaluationResult: Validation outcome including pass/fail status
                and comprehensive metrics comparing actual count to configured boundaries.
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
    Configuration schema for row count boundary validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce dataset size constraints. The configuration includes logical
    validation to ensure boundary parameters are consistent and meaningful.

    This configuration enables declarative check definition through external
    configuration sources while ensuring parameter validity at configuration time.

    Attributes:
        min_count (int): Minimum acceptable number of records in the dataset.
        max_count (int): Maximum acceptable number of records in the dataset.
    """

    check_class = RowCountBetweenCheck
    min_count: int = Field(..., description="Minimum number of rows expected", alias="min-count")
    max_count: int = Field(..., description="Maximum number of rows allowed", alias="max-count")

    @model_validator(mode="after")
    def validate_range(self) -> "RowCountBetweenCheckConfig":
        """
        Validate the logical consistency of the configured boundary parameters.

        Ensures that the minimum and maximum count parameters form a valid range
        and that both values are non-negative. This validation prevents
        configuration errors that would result in impossible validation conditions.

        Returns:
            RowCountBetweenCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the boundary parameters are
                logically inconsistent or contain invalid values.
        """
        if self.min_count < 0:
            raise InvalidCheckConfigurationError(
                f"min-count ({self.min_count}) must be greater than or equal to 0"
            )
        if self.min_count > self.max_count:
            raise InvalidCheckConfigurationError(
                f"min-count ({self.min_count}) must not be greater than max-count ({self.max_count})"
            )
        return self
