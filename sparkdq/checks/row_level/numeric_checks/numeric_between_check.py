from decimal import Decimal
from typing import List, Tuple

from pydantic import Field, model_validator

from sparkdq.checks.utils.base_comparison_check import BaseBetweenCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class NumericBetweenCheck(BaseBetweenCheck):
    """
    Row-level data quality check that verifies that numeric values in the specified columns
    lie between a defined minimum (`min_value`) and maximum (`max_value`) threshold.

    A row fails the check if **any** of the target columns contain a value below `min_value`
    or above `max_value`.
    """

    pass  # Fully handled by BaseBetweenCheck


@register_check_config(check_name="numeric-between-check")
class NumericBetweenCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the NumericBetweenCheck.

    This configuration defines both a lower and upper bound constraint on one or more numeric columns.
    It ensures that all specified columns contain only values between the configured `min_value`
    and `max_value`. Violations are flagged per row.

    Attributes:
        columns (List[str]): The list of numeric columns to validate.
        min_value (float | int | Decimal): The minimum allowed value (inclusive).
        max_value (float | int | Decimal): The maximum allowed value (inclusive).
        inclusive (tuple[bool, bool]): Inclusion flags for min and max boundaries.
    """

    check_class = NumericBetweenCheck
    columns: List[str] = Field(..., description="The list of numeric columns to check for value range")
    inclusive: Tuple[bool, bool] = Field(
        default=(False, False),
        description="Whether min and max values are inclusive (min_inclusive, max_inclusive)",
    )
    min_value: float | int | Decimal = Field(
        ..., description="The minimum allowed value (inclusive) for the specified columns"
    )
    max_value: float | int | Decimal = Field(
        ..., description="The maximum allowed value (inclusive) for the specified columns"
    )

    @model_validator(mode="after")
    def validate_between_values(self) -> "NumericBetweenCheckConfig":
        """
        Validates that ``min_value`` and ``max_value`` are properly configured
        and that ``min_value`` is not greater than ``max_value``.

        Returns:
            NumericBetweenCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If min_value or max_value are not set or if min_value > max_value.
        """
        if self.min_value > self.max_value:
            raise InvalidCheckConfigurationError(
                f"min_value ({self.min_value}) must not be greater than max_value ({self.max_value})"
            )
        return self
