from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMinCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.plugin.check_config_registry import register_check_config


class TimestampMinCheck(BaseMinCheck):
    """
    Row-level data quality check that verifies that timestamp values in the specified columns
    are greater than a defined minimum timestamp.

    A row fails the check if **any** of the target columns contain a timestamp less than `min_value`.
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
        Initialize a new TimestampMinCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of timestamp columns to check.
            min_value (str): The minimum allowed timestamp in ISO format (e.g. '2023-01-01T00:00:00').
            inclusive (bool): Whether the minimum timestamp is inclusive.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(
            check_id=check_id,
            columns=columns,
            severity=severity,
            inclusive=inclusive,
            min_value=min_value,
            cast_type="timestamp",
        )


@register_check_config(check_name="timestamp-min-check")
class TimestampMinCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the TimestampMinCheck.

    Attributes:
        columns (List[str]): The list of timestamp columns to validate.
        min_value (str): The minimum allowed timestamp in ISO 8601 format (e.g. '2023-01-01T00:00:00').
        inclusive (bool): Whether the minimum value is inclusive.
    """

    check_class = TimestampMinCheck

    columns: List[str] = Field(..., description="The list of timestamp columns to check")
    min_value: str = Field(..., description="The minimum allowed timestamp (ISO 8601 format)")
    inclusive: bool = Field(False, description="Whether the minimum timestamp is inclusive")
