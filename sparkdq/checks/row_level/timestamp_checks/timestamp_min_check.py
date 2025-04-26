from typing import List

from pydantic import Field
from pyspark.sql import Column

from sparkdq.checks.base_classes.base_comparison_check import BaseComparisonCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.factory.check_config_registry import register_check_config


class TimestampMinCheck(BaseComparisonCheck):
    """
    Row-level data quality check that verifies that timestamp values in the specified columns
    are greater than or equal to a defined minimum timestamp.

    A row fails the check if **any** of the target columns contain a timestamp before `min_value`.

    Attributes:
        check_id (str): Unique identifier for the check instance.
        columns (List[str]): Names of the columns to validate.
        min_value (str): The minimum allowed timestamp (inclusive), in 'YYYY-MM-DD HH:MM:SS' format.
        severity (Severity): Severity level assigned if the check fails.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: str,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new TimestampMinCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of timestamp columns to check.
            min_value (str): The minimum allowed timestamp (inclusive), in 'YYYY-MM-DD HH:MM:SS' format.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type="timestamp")
        self.min_value = min_value

    def comparison_condition(self, column: Column) -> Column:
        """
        Defines the boolean condition that determines check failure for a single column.

        Args:
            column (Column): The Spark column expression to validate.

        Returns:
            Column: A boolean expression where `True` indicates a failed check
                    (value before `min_value`).
        """
        return column < self.min_value


@register_check_config(check_name="timestamp-min-check")
class TimestampMinCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the TimestampMinCheck.

    This configuration defines a minimum timestamp constraint on one or more timestamp columns.
    It ensures that all specified columns contain only timestamps on or after the configured `min_value`.
    Violations are flagged per row.

    Attributes:
        columns (List[str]): The list of timestamp columns to validate.
        min_value (str): The minimum allowed timestamp (inclusive), in 'YYYY-MM-DD HH:MM:SS' format.
    """

    check_class = TimestampMinCheck
    columns: List[str] = Field(
        ..., description="The list of timestamp columns to check for minimum timestamp"
    )
    min_value: str = Field(
        ..., description="The minimum allowed timestamp (inclusive) in 'YYYY-MM-DD HH:MM:SS' format"
    )
