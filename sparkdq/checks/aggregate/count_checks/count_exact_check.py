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
    Dataset-level validation check that enforces exact row count requirements.

    Validates that the total number of records in a dataset matches a precise
    expected count, ensuring strict data contract compliance. This check is
    essential for validating reference datasets, controlled test data, or
    scenarios where exact record counts are critical for downstream processing.

    The check provides comprehensive metrics comparing actual counts to expected
    values to support detailed analysis of validation outcomes.

    Attributes:
        expected_count (int): Exact number of records required in the dataset.
    """

    def __init__(self, check_id: str, expected_count: int, severity: Severity = Severity.CRITICAL):
        """
        Initialize the exact row count validation check with target count configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            expected_count (int): Exact number of records required in the dataset.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.expected_count = expected_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Execute the exact row count validation against the configured target count.

        Computes the total row count of the dataset and evaluates it against the
        configured expected count. The validation passes only when the actual
        count exactly matches the expected value.

        Args:
            df (DataFrame): The dataset to evaluate for exact count compliance.

        Returns:
            AggregateEvaluationResult: Validation outcome including pass/fail status
                and comprehensive metrics comparing actual count to expected value.
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
    Configuration schema for exact row count validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce precise row count requirements. The configuration includes logical
    validation to ensure count parameters are non-negative and meaningful.

    Attributes:
        expected_count (int): Exact number of records required in the dataset.
    """

    check_class = RowCountExactCheck
    expected_count: int = Field(..., description="Exact number of rows required", alias="expected-count")

    @model_validator(mode="after")
    def validate_expected(self) -> "RowCountExactCheckConfig":
        """
        Validate the logical consistency of the configured expected count parameter.

        Ensures that the expected count parameter is non-negative and meaningful
        for dataset validation purposes. This validation prevents configuration
        errors that would result in impossible validation conditions.

        Returns:
            RowCountExactCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the expected count parameter is
                negative, indicating an invalid configuration.
        """
        if self.expected_count < 0:
            raise InvalidCheckConfigurationError(
                f"expected-count ({self.expected_count}) must be zero or positive"
            )
        return self
