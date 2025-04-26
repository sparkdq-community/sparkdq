from typing import List

from pydantic import Field, model_validator
from pyspark.sql import Column

from sparkdq.checks.base_classes.base_comparison_check import BaseComparisonCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.factory.check_config_registry import register_check_config


class DateBetweenCheck(BaseComparisonCheck):
    """
    Row-level data quality check that verifies that date values in the specified columns
    lie between a defined minimum (`min_value`) and maximum (`max_value`) threshold.

    A row fails the check if **any** of the target columns contain a date before `min_value`
    or after `max_value`.

    Attributes:
        check_id (str): Unique identifier for the check instance.
        columns (List[str]): Names of the columns to validate.
        min_value (str): The minimum allowed date (inclusive), in 'YYYY-MM-DD' format.
        max_value (str): The maximum allowed date (inclusive), in 'YYYY-MM-DD' format.
        severity (Severity): Severity level assigned if the check fails.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: str,
        max_value: str,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new DateBetweenCheck.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): List of date columns to check.
            min_value (str): The minimum allowed date (inclusive), in 'YYYY-MM-DD' format.
            max_value (str): The maximum allowed date (inclusive), in 'YYYY-MM-DD' format.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type="date")
        self.min_value = min_value
        self.max_value = max_value

    def comparison_condition(self, column: Column) -> Column:
        """
        Defines the boolean condition that determines check failure for a single column.

        Args:
            column (Column): The Spark column expression to validate.

        Returns:
            Column: A boolean expression where `True` indicates a failed check
                    (value outside the [min_value, max_value] range).
        """
        return (column < self.min_value) | (column > self.max_value)


@register_check_config(check_name="date-between-check")
class DateBetweenCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the DateBetweenCheck.

    This configuration defines both a lower and upper date bound constraint
    on one or more date columns. It ensures that all specified columns contain
    only dates between the configured `min_value` and `max_value`.
    Violations are flagged per row.

    Attributes:
        columns (List[str]): The list of date columns to validate.
        min_value (str): The minimum allowed date (inclusive), in 'YYYY-MM-DD' format.
        max_value (str): The maximum allowed date (inclusive), in 'YYYY-MM-DD' format.
    """

    check_class = DateBetweenCheck
    columns: List[str] = Field(..., description="The list of date columns to check for date range")
    min_value: str = Field(..., description="The minimum allowed date (inclusive) in 'YYYY-MM-DD' format")
    max_value: str = Field(..., description="The maximum allowed date (inclusive) in 'YYYY-MM-DD' format")

    @model_validator(mode="after")
    def validate_between_values(self) -> "DateBetweenCheckConfig":
        """
        Validates that min_value and max_value are properly configured
        and that min_value is not greater than max_value.

        Returns:
            DateBetweenCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If min_value or max_value are not set or if min_value > max_value.
        """
        # Additional safety check if wanted (basic string comparison works for ISO dates)
        if self.min_value > self.max_value:
            raise InvalidCheckConfigurationError(
                f"min_value ({self.min_value}) must not be greater than max_value ({self.max_value})"
            )

        return self
