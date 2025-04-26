from decimal import Decimal
from typing import List

from pydantic import Field
from pyspark.sql import Column

from sparkdq.checks.base_classes.base_comparison_check import BaseComparisonCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.factory.check_config_registry import register_check_config


class NumericMaxCheck(BaseComparisonCheck):
    """
    Row-level data quality check that verifies that numeric values in the specified columns
    are less than or equal to a defined maximum threshold.

    A row fails the check if **any** of the target columns contain a value above `max_value`.

    Attributes:
        check_id (str): Unique identifier for the check instance.
        columns (List[str]): Names of the columns to validate.
        max_value (float | int | Decimal): The maximum allowed value (inclusive).
        severity (Severity): Severity level assigned if the check fails.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        max_value: float | int | Decimal,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new NumericMaxCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of numeric columns to check.
            max_value (float | int | Decimal): The maximum allowed value (inclusive).
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type=None)
        self.max_value = max_value

    def comparison_condition(self, column: Column) -> Column:
        """
        Defines the boolean condition that determines check failure for a single column.

        Args:
            column (Column): The Spark column expression to validate.

        Returns:
            Column: A boolean expression where `True` indicates a failed check
                    (value above `max_value`).
        """
        return column > self.max_value


@register_check_config(check_name="numeric-max-check")
class NumericMaxCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the NumericMaxCheck.

    This configuration defines a maximum value constraint on one or more numeric columns.
    It ensures that all specified columns contain only values less than or equal to
    the configured `max_value`. Violations are flagged per row.

    Attributes:
        columns (List[str]): The list of numeric columns to validate.
        max_value (float | int | Decimal): The maximum allowed value (inclusive).
    """

    check_class = NumericMaxCheck
    columns: List[str] = Field(..., description="The list of numeric columns to check for maximum value")
    max_value: float | int | Decimal = Field(
        ..., description="The maximum allowed value (inclusive) for the specified columns"
    )
