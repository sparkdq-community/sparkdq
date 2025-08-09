from decimal import Decimal
from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMaxCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.plugin.check_config_registry import register_check_config


class NumericMaxCheck(BaseMaxCheck):
    """
    Record-level validation check that enforces maximum numeric thresholds.

    Validates that numeric values in specified columns remain within or below a
    configured maximum threshold. This check is essential for enforcing business
    rules such as maximum transaction limits, capacity constraints, or acceptable
    measurement ranges in data quality validation scenarios.

    The check supports both inclusive and exclusive boundary semantics, enabling
    precise control over numeric validation requirements.
    """

    pass  # implemented via BaseMaxCheck


@register_check_config(check_name="numeric-max-check")
class NumericMaxCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for maximum numeric threshold validation checks.

    Defines the parameters required for configuring checks that enforce maximum
    numeric boundaries. This configuration enables declarative check definition
    through external configuration sources while ensuring parameter validity.

    Attributes:
        columns (List[str]): Numeric column names that must remain within the maximum threshold.
        max_value (float | int | Decimal): Maximum acceptable numeric value for validation.
        inclusive (bool): Whether the maximum threshold includes the boundary value itself.
    """

    check_class = NumericMaxCheck
    columns: List[str] = Field(..., description="The list of numeric columns to check for maximum value")
    max_value: float | int | Decimal = Field(
        ..., description="The maximum allowed value (inclusive) for the specified columns", alias="max-value"
    )
    inclusive: bool = Field(False, description="Whether the maximum value is inclusive")
