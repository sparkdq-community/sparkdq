from decimal import Decimal
from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMaxCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.plugin.check_config_registry import register_check_config


class NumericMaxCheck(BaseMaxCheck):
    """
    Row-level data quality check that verifies that numeric values in the specified columns
    are less than or equal to a defined maximum threshold.

    A row fails the check if **any** of the target columns contain a value above `max_value`.
    """

    pass  # implemented via BaseMaxCheck


@register_check_config(check_name="numeric-max-check")
class NumericMaxCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the NumericMaxCheck.

    Attributes:
        columns (List[str]): The list of numeric columns to validate.
        max_value (float | int | Decimal): The maximum allowed value (inclusive).
        inclusive (bool): Whether to include the maximum value.
    """

    check_class = NumericMaxCheck
    columns: List[str] = Field(..., description="The list of numeric columns to check for maximum value")
    max_value: float | int | Decimal = Field(
        ..., description="The maximum allowed value (inclusive) for the specified columns"
    )
    inclusive: bool = Field(False, description="Whether the maximum value is inclusive")
