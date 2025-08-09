from decimal import Decimal
from typing import List, Tuple

from pydantic import Field, model_validator

from sparkdq.checks.utils.base_comparison_check import BaseBetweenCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class NumericBetweenCheck(BaseBetweenCheck):
    """
    Record-level validation check that enforces numeric range boundaries.

    Validates that numeric values in specified columns fall within a configured
    numeric range, ensuring data integrity and business rule compliance. This
    check is essential for validating acceptable measurement ranges, financial
    limits, or operational parameters in data quality validation scenarios.

    The check supports independent inclusivity control for both minimum and
    maximum boundaries, enabling precise validation criteria for complex
    numeric business requirements.
    """

    pass  # Fully handled by BaseBetweenCheck


@register_check_config(check_name="numeric-between-check")
class NumericBetweenCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for numeric range validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce numeric range constraints. The configuration includes logical
    validation to ensure boundary parameters are consistent and meaningful.

    This configuration enables declarative check definition through external
    configuration sources while ensuring parameter validity at configuration time.

    Attributes:
        columns (List[str]): Numeric column names that must fall within the specified range.
        min_value (float | int | Decimal): Minimum acceptable numeric value for the valid range.
        max_value (float | int | Decimal): Maximum acceptable numeric value for the valid range.
        inclusive (tuple[bool, bool]): Inclusivity settings for minimum and maximum
            boundaries respectively.
    """

    check_class = NumericBetweenCheck
    columns: List[str] = Field(..., description="The list of numeric columns to check for value range")
    inclusive: Tuple[bool, bool] = Field(
        default=(False, False),
        description="Whether min and max values are inclusive (min_inclusive, max_inclusive)",
    )
    min_value: float | int | Decimal = Field(
        ..., description="The minimum allowed value (inclusive) for the specified columns", alias="min-value"
    )
    max_value: float | int | Decimal = Field(
        ..., description="The maximum allowed value (inclusive) for the specified columns", alias="max-value"
    )

    @model_validator(mode="after")
    def validate_between_values(self) -> "NumericBetweenCheckConfig":
        """
        Validate the logical consistency of the configured numeric range parameters.

        Ensures that the minimum and maximum numeric parameters form a valid range
        and that both values are properly ordered. This validation prevents
        configuration errors that would result in impossible validation conditions.

        Returns:
            NumericBetweenCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the numeric range parameters are
                logically inconsistent or contain invalid values.
        """
        if self.min_value > self.max_value:
            raise InvalidCheckConfigurationError(
                f"min_value ({self.min_value}) must not be greater than max_value ({self.max_value})"
            )
        return self
