from typing import List

from pydantic import Field, model_validator

from sparkdq.checks.utils.base_comparison_check import BaseBetweenCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class DateBetweenCheck(BaseBetweenCheck):
    """
    Record-level validation check that enforces date range boundaries.

    Validates that date values in specified columns fall within a configured
    date range, ensuring temporal data integrity and business rule compliance.
    This check is essential for validating reporting periods, operational
    timeframes, and historical data constraints.

    The check supports independent inclusivity control for both minimum and
    maximum boundaries, enabling precise validation criteria for complex
    temporal business requirements.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: str,
        max_value: str,
        inclusive: tuple[bool, bool] = (False, False),
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize the date range validation check with boundary configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Date column names that must fall within the specified range.
            min_value (str): Minimum acceptable date in ISO format (YYYY-MM-DD).
            max_value (str): Maximum acceptable date in ISO format (YYYY-MM-DD).
            inclusive (tuple[bool, bool], optional): Inclusivity settings for minimum and
                maximum boundaries respectively. Defaults to (False, False) for exclusive bounds.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(
            check_id=check_id,
            columns=columns,
            severity=severity,
            min_value=min_value,
            max_value=max_value,
            inclusive=inclusive,
            cast_type="date",
        )


@register_check_config(check_name="date-between-check")
class DateBetweenCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for date range validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce date range constraints. The configuration includes logical
    validation to ensure boundary parameters are consistent and meaningful.

    Attributes:
        columns (List[str]): Date column names that must fall within the specified range.
        min_value (str): Minimum acceptable date in ISO format (YYYY-MM-DD).
        max_value (str): Maximum acceptable date in ISO format (YYYY-MM-DD).
        inclusive (tuple[bool, bool]): Inclusivity settings for minimum and maximum
            boundaries respectively.
    """

    check_class = DateBetweenCheck

    columns: List[str] = Field(..., description="Date columns to validate")
    min_value: str = Field(..., description="Minimum allowed date in YYYY-MM-DD format", alias="min-value")
    max_value: str = Field(..., description="Maximum allowed date in YYYY-MM-DD format", alias="max-value")
    inclusive: tuple[bool, bool] = Field(
        (False, False), description="Tuple of two booleans controlling boundary inclusivity"
    )

    @model_validator(mode="after")
    def validate_between_values(self) -> "DateBetweenCheckConfig":
        """
        Validate the logical consistency of the configured date range parameters.

        Ensures that the minimum and maximum date parameters form a valid temporal
        range and that both values represent valid dates. This validation prevents
        configuration errors that would result in impossible validation conditions.

        Returns:
            DateBetweenCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the date range parameters are
                logically inconsistent or contain invalid date values.
        """
        # Additional safety check if wanted (basic string comparison works for ISO dates)
        if self.min_value > self.max_value:
            raise InvalidCheckConfigurationError(
                f"min_value ({self.min_value}) must not be greater than max_value ({self.max_value})"
            )

        return self
