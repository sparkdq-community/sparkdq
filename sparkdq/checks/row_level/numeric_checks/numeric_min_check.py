from decimal import Decimal
from typing import List

from pydantic import Field
from pyspark.sql import Column

from sparkdq.checks.base_classes.base_comparison_check import BaseComparisonCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.factory.check_config_registry import register_check_config


class NumericMinCheck(BaseComparisonCheck):
    """
    Row-level data quality check that verifies that numeric values in the specified columns
    are greater than or equal to a defined minimum threshold.

    A row fails the check if **any** of the target columns contain a value below `min_value`.

    Attributes:
        check_id (str): Unique identifier for the check instance.
        columns (List[str]): Names of the columns to validate.
        min_value (float | int | Decimal): The minimum allowed value (inclusive).
        severity (Severity): Severity level assigned if the check fails.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: float | int | Decimal,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new NumericMinCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of numeric columns to check.
            min_value (float | int | Decimal): The minimum allowed value (inclusive).
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type=None)
        self.min_value = min_value

    def comparison_condition(self, column: Column) -> Column:
        """
        Defines the boolean condition that determines check failure for a single column.

        Args:
            column (Column): The Spark column expression to validate.

        Returns:
            Column: A boolean expression where `True` indicates a failed check
                    (value below `min_value`).
        """
        return column < self.min_value


@register_check_config(check_name="numeric-min-check")
class NumericMinCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the NumericMinCheck.

    This configuration defines a minimum value constraint on one or more numeric columns.
    It ensures that all specified columns contain only values greater than or equal to
    the configured `min_value`. Violations are flagged per row.

    Attributes:
        columns (List[str]): The list of numeric columns to validate.
        min_value (float): The minimum allowed value (inclusive).
    """

    check_class = NumericMinCheck
    columns: List[str] = Field(..., description="The list of numeric columns to check for minimum value")
    min_value: float | int | Decimal = Field(
        ..., description="The minimum allowed value (inclusive) for the specified columns"
    )
