from decimal import Decimal
from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMinCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.plugin.check_config_registry import register_check_config


class NumericMinCheck(BaseMinCheck):
    """
    Record-level validation check that enforces minimum numeric thresholds.

    Validates that numeric values in specified columns meet or exceed a configured
    minimum threshold. This check is essential for enforcing business rules such
    as minimum order amounts, positive balance requirements, or acceptable
    measurement ranges in data quality validation scenarios.

    The check supports both inclusive and exclusive boundary semantics, enabling
    precise control over numeric validation requirements.
    """

    pass  # implemented via BaseMinCheck


@register_check_config(check_name="numeric-min-check")
class NumericMinCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for minimum numeric threshold validation checks.

    Defines the parameters required for configuring checks that enforce minimum
    numeric boundaries. This configuration enables declarative check definition
    through external configuration sources while ensuring parameter validity.

    Attributes:
        columns (List[str]): Numeric column names that must meet minimum threshold requirements.
        min_value (float | int | Decimal): Minimum acceptable numeric value for validation.
        inclusive (bool): Whether the minimum threshold includes the boundary value itself.
    """

    check_class = NumericMinCheck
    columns: List[str] = Field(..., description="The list of numeric columns to check for minimum value")
    min_value: float | int | Decimal = Field(
        ..., description="The minimum allowed value (inclusive) for the specified columns", alias="min-value"
    )
    inclusive: bool = Field(False, description="Whether the minimum value is inclusive")
