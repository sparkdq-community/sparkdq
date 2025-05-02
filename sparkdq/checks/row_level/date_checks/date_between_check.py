from typing import List

from pydantic import Field, model_validator

from sparkdq.checks.utils.base_comparison_check import BaseBetweenCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class DateBetweenCheck(BaseBetweenCheck):
    """
    Row-level data quality check that verifies that date values fall within a specified date range.

    A row fails the check if any of the target columns contain a date before `min_value` or after `max_value`,
    depending on the configured inclusivity.
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
        Initialize a new DateBetweenCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of date columns to check.
            min_value (str): Minimum allowed date in 'YYYY-MM-DD' format.
            max_value (str): Maximum allowed date in 'YYYY-MM-DD' format.
            inclusive (tuple[bool, bool], optional): Inclusion flags for (min_value, max_value).
            severity (Severity, optional): Severity level of the check result. Defaults to Severity.CRITICAL.
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
    Declarative configuration model for DateBetweenCheck.

    Attributes:
        columns (List[str]): Date columns to validate.
        min_value (str): Minimum allowed date in 'YYYY-MM-DD' format.
        max_value (str): Maximum allowed date in 'YYYY-MM-DD' format.
        inclusive (tuple[bool, bool]): Inclusion flags for min and max boundaries.
    """

    check_class = DateBetweenCheck

    columns: List[str] = Field(..., description="Date columns to validate")
    min_value: str = Field(..., description="Minimum allowed date in YYYY-MM-DD format")
    max_value: str = Field(..., description="Maximum allowed date in YYYY-MM-DD format")
    inclusive: tuple[bool, bool] = Field(
        (False, False), description="Tuple of two booleans controlling boundary inclusivity"
    )

    @model_validator(mode="after")
    def validate_between_values(self) -> "DateBetweenCheckConfig":
        """
        Validates that min_value and max_value are properly configured
        and that ``min_value`` is not greater than ``max_value``.

        Returns:
            DateBetweenCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If min_value or max_value are not set or if min_value > max_value.
        """
        # Additional safety check if wanted (basic string comparison works for ISO dates)
        if self.min_value > self.max_value:
            raise InvalidCheckConfigurationError(
                f"min_value ({self.min_value}) must not be greater than max_value ({self.max_value})"
            )

        return self
