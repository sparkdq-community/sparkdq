from typing import List

from pydantic import Field
from pyspark.sql import Column

from sparkdq.checks.base_classes.base_comparison_check import BaseComparisonCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.factory.check_config_registry import register_check_config


class TimestampMaxCheck(BaseComparisonCheck):
    """
    Row-level data quality check that verifies that timestamp values in the specified columns
    are less than or equal to a defined maximum timestamp.

    A row fails the check if **any** of the target columns contain a timestamp after `max_value`.

    Attributes:
        check_id (str): Unique identifier for the check instance.
        columns (List[str]): Names of the columns to validate.
        max_value (str): The maximum allowed timestamp (inclusive), in 'YYYY-MM-DD HH:MM:SS' format.
        severity (Severity): Severity level assigned if the check fails.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        max_value: str,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new TimestampMaxCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of timestamp columns to check.
            max_value (str): The maximum allowed timestamp (inclusive), in 'YYYY-MM-DD HH:MM:SS' format.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type="timestamp")
        self.max_value = max_value

    def comparison_condition(self, column: Column) -> Column:
        """
        Defines the boolean condition that determines check failure for a single column.

        Args:
            column (Column): The Spark column expression to validate.

        Returns:
            Column: A boolean expression where `True` indicates a failed check
                    (value after `max_value`).
        """
        return column > self.max_value


@register_check_config(check_name="timestamp-max-check")
class TimestampMaxCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the TimestampMaxCheck.

    This configuration defines a maximum timestamp constraint on one or more timestamp columns.
    It ensures that all specified columns contain only timestamps on or before the configured `max_value`.
    Violations are flagged per row.

    Attributes:
        columns (List[str]): The list of timestamp columns to validate.
        max_value (str): The maximum allowed timestamp (inclusive), in 'YYYY-MM-DD HH:MM:SS' format.
    """

    check_class = TimestampMaxCheck
    columns: List[str] = Field(
        ..., description="The list of timestamp columns to check for maximum timestamp"
    )
    max_value: str = Field(
        ..., description="The maximum allowed timestamp (inclusive) in 'YYYY-MM-DD HH:MM:SS' format"
    )
