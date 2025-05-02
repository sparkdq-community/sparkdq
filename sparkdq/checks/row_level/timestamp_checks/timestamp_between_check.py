from typing import List

from pydantic import Field, model_validator

from sparkdq.checks.utils.base_comparison_check import BaseBetweenCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class TimestampBetweenCheck(BaseBetweenCheck):
    """
    Row-level data quality check that verifies timestamp values are within a defined range.

    A row fails the check if **any** of the specified columns contain a timestamp value that is
    less than `min_value` or greater than `max_value`. Boundary inclusiveness is configurable.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: str,
        max_value: str,
        inclusive: tuple[bool, bool],
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new TimestampBetweenCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): Timestamp columns to validate.
            min_value (str): Minimum allowed timestamp in ISO format.
            max_value (str): Maximum allowed timestamp in ISO format.
            inclusive (tuple): Tuple of booleans indicating whether to include the bounds.
            severity (Severity): Severity level of the check result.
        """
        super().__init__(
            check_id=check_id,
            columns=columns,
            severity=severity,
            min_value=min_value,
            max_value=max_value,
            inclusive=inclusive,
            cast_type="timestamp",
        )


@register_check_config(check_name="timestamp-between-check")
class TimestampBetweenCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for TimestampBetweenCheck.

    Attributes:
        columns (List[str]): The list of timestamp columns to validate.
        min_value (str): Minimum allowed timestamp.
        max_value (str): Maximum allowed timestamp.
        inclusive (tuple): Optional tuple of booleans for boundary inclusion.
    """

    check_class = TimestampBetweenCheck

    columns: List[str] = Field(..., description="Timestamp columns to check")
    min_value: str = Field(..., description="Minimum allowed timestamp (ISO format)")
    max_value: str = Field(..., description="Maximum allowed timestamp (ISO format)")
    inclusive: tuple[bool, bool] = Field((False, False), description="Whether to include [min, max] bounds")

    @model_validator(mode="after")
    def validate_between_values(self) -> "TimestampBetweenCheckConfig":
        """
        Validates that ``min_value`` and ``max_value`` are properly configured
        and that ``min_value`` is not greater than ``max_value``.

        Raises:
            InvalidCheckConfigurationError: If min_value or max_value are not set or if min_value > max_value.
        """
        if self.min_value > self.max_value:
            raise InvalidCheckConfigurationError(
                f"min_value ({self.min_value}) must not be greater than max_value ({self.max_value})"
            )
        return self
