from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMaxCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.plugin.check_config_registry import register_check_config


class TimestampMaxCheck(BaseMaxCheck):
    """
    Row-level data quality check that verifies that timestamp values in the specified columns
    are below a defined maximum timestamp.

    A row fails the check if **any** of the target columns contain a timestamp greater than
    (or equal to, depending on `inclusive`) `max_value`.
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
        Initialize a new TimestampMaxCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of timestamp columns to check.
            max_value (str): The maximum allowed timestamp in ISO 8601 format.
            inclusive (bool): Whether the maximum value is inclusive.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(
            check_id=check_id,
            columns=columns,
            severity=severity,
            inclusive=inclusive,
            max_value=max_value,
            cast_type="timestamp",
        )


@register_check_config(check_name="timestamp-max-check")
class TimestampMaxCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for TimestampMaxCheck.

    Attributes:
        columns (List[str]): The timestamp columns to validate.
        max_value (str): The maximum allowed timestamp in ISO 8601 format.
        inclusive (bool): Whether to include the upper bound timestamp.
    """

    check_class = TimestampMaxCheck

    columns: List[str] = Field(..., description="The list of timestamp columns to check")
    max_value: str = Field(..., description="The maximum allowed timestamp (ISO 8601 format)")
    inclusive: bool = Field(False, description="Whether the maximum timestamp is inclusive")
