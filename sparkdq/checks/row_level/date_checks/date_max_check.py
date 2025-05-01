from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMaxCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.plugin.check_config_registry import register_check_config


class DateMaxCheck(BaseMaxCheck):
    """
    Row-level data quality check that verifies date values are below a defined maximum date.

    A row fails if any of the target columns contains a date after (or equal to, depending on `inclusive`)
    `max_value`.

    Example use cases:
        - Ensure that no future dates are recorded.
        - Validate that invoice dates do not exceed the end of the reporting period.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        max_value: str,
        inclusive: bool,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new DateMaxCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of date columns to check.
            max_value (str): The maximum allowed date (inclusive), in 'YYYY-MM-DD' format.
            inclusive (bool): Whether the maximum date is inclusive.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(
            check_id=check_id,
            columns=columns,
            severity=severity,
            inclusive=inclusive,
            max_value=max_value,
            cast_type="date",
        )


@register_check_config(check_name="date-max-check")
class DateMaxCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for DateMaxCheck.

    Attributes:
        columns (List[str]): Date columns to validate.
        max_value (str): The maximum allowed date in ISO format.
        inclusive (bool): Whether to include the maximum date.
    """

    check_class = DateMaxCheck

    columns: List[str] = Field(..., description="Date columns to validate")
    max_value: str = Field(..., description="Maximum allowed date in YYYY-MM-DD format")
    inclusive: bool = Field(False, description="Whether the maximum date is inclusive")
