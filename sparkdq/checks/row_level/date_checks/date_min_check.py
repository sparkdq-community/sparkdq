from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMinCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.plugin.check_config_registry import register_check_config


class DateMinCheck(BaseMinCheck):
    """
    Row-level data quality check that verifies that date values in the specified columns
    are greater than or equal to a defined minimum date.

    A row fails the check if **any** of the target columns contain a date before `min_value`.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: str,
        inclusive: bool,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new DateMinCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of date columns to check.
            min_value (str): The minimum allowed date (inclusive), in 'YYYY-MM-DD' format.
            inclusive (bool): Whether the minimum date is inclusive.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(
            check_id=check_id,
            columns=columns,
            severity=severity,
            inclusive=inclusive,
            min_value=min_value,
            cast_type="date",
        )


@register_check_config(check_name="date-min-check")
class DateMinCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the DateMinCheck.

    Attributes:
        columns (List[str]): The list of date columns to validate.
        min_value (str): The minimum allowed date (inclusive), in 'YYYY-MM-DD' format.
        inclusive (bool): Whether to include the minimum date.
    """

    check_class = DateMinCheck
    columns: List[str] = Field(..., description="The list of date columns to check for minimum date")
    min_value: str = Field(..., description="The minimum allowed date (inclusive) in 'YYYY-MM-DD' format")
    inclusive: bool = Field(False, description="Whether the minimum date is inclusive")
