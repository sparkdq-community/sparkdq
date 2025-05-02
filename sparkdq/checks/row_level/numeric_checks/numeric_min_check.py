from decimal import Decimal
from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMinCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.plugin.check_config_registry import register_check_config


class NumericMinCheck(BaseMinCheck):
    """
    Row-level data quality check that verifies that numeric values in the specified columns
    are greater than or equal to a defined minimum threshold.

    A row fails the check if **any** of the target columns contain a value below `min_value`.
    """

    pass  # implemented via BaseMinCheck


@register_check_config(check_name="numeric-min-check")
class NumericMinCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the NumericMinCheck.

    Attributes:
        columns (List[str]): The list of numeric columns to validate.
        min_value (float): The minimum allowed value (inclusive).
        inclusive (bool): Whether to include the minimum value.
    """

    check_class = NumericMinCheck
    columns: List[str] = Field(..., description="The list of numeric columns to check for minimum value")
    min_value: float | int | Decimal = Field(
        ..., description="The minimum allowed value (inclusive) for the specified columns"
    )
    inclusive: bool = Field(False, description="Whether the minimum value is inclusive")
