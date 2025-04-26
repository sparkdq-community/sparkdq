from typing import List

from pydantic import Field
from pyspark.sql import Column

from sparkdq.checks.base_classes.base_comparison_check import BaseComparisonCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.factory.check_config_registry import register_check_config


class DateMaxCheck(BaseComparisonCheck):
    """
    Row-level data quality check that verifies that date values in the specified columns
    are less than or equal to a defined maximum date.

    A row fails the check if **any** of the target columns contain a date after `max_value`.

    Attributes:
        check_id (str): Unique identifier for the check instance.
        columns (List[str]): Names of the columns to validate.
        max_value (str): The maximum allowed date (inclusive), in 'YYYY-MM-DD' format.
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
        Initialize a new DateMaxCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of date columns to check.
            max_value (str): The maximum allowed date (inclusive), in 'YYYY-MM-DD' format.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type="date")
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


@register_check_config(check_name="date-max-check")
class DateMaxCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the DateMaxCheck.

    This configuration defines a maximum date constraint on one or more date columns.
    It ensures that all specified columns contain only dates on or before the configured `max_value`.
    Violations are flagged per row.

    Attributes:
        columns (List[str]): The list of date columns to validate.
        max_value (str): The maximum allowed date (inclusive), in 'YYYY-MM-DD' format.
    """

    check_class = DateMaxCheck
    columns: List[str] = Field(..., description="The list of date columns to check for maximum date")
    max_value: str = Field(..., description="The maximum allowed date (inclusive) in 'YYYY-MM-DD' format")
