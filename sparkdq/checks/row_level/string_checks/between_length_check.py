from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class StringLengthBetweenCheck(BaseRowCheck):
    """
    Row-level check that flags rows where string length falls outside a given range.

    The check is applied only to non-null string values in the specified column.
    Null values are treated as valid.

    Attributes:
        column (str): The column to validate.
        min_length (int): Lower bound of string length (inclusive/exclusive).
        max_length (int): Upper bound of string length (inclusive/exclusive).
        inclusive (tuple[bool, bool]): Inclusiveness for (min, max).
    """

    def __init__(
        self,
        check_id: str,
        column: str,
        min_length: int,
        max_length: int,
        inclusive: tuple[bool, bool] = (True, True),
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize the StringLengthBetweenCheck instance.

        Args:
            check_id (str): Unique identifier for the check.
            column (str): Column to check.
            min_length (int): Minimum string length.
            max_length (int): Maximum string length.
            inclusive (Tuple[bool, bool], optional): Tuple of inclusivity for min and max bounds.
                Defaults to (True, True).
            severity (Severity, optional): Severity of failed rows. Defaults to CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.min_length = min_length
        self.max_length = max_length
        self.inclusive = inclusive

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Applies the length range check to the specified string column.

        A row fails if:
        - value is not null, and
        - its length is outside the configured range

        Args:
            df (DataFrame): Input DataFrame.

        Returns:
            DataFrame: DataFrame with additional column indicating failure (True/False).

        Raises:
            MissingColumnError: If the specified column is missing.
        """
        if self.column not in df.columns:
            raise MissingColumnError(self.column, df.columns)

        not_null_col = F.col(self.column).isNotNull()
        length_col = F.length(F.col(self.column))

        lower_op = length_col >= self.min_length if self.inclusive[0] else length_col > self.min_length
        upper_op = length_col <= self.max_length if self.inclusive[1] else length_col < self.max_length

        fail_expr = not_null_col & ~(lower_op & upper_op)

        return self.with_check_result_column(df, fail_expr)


@register_check_config(check_name="string-length-between-check")
class StringLengthBetweenCheckConfig(BaseRowCheckConfig):
    """
    Configuration for StringLengthBetweenCheck.

    Validates that string values in the given column fall between a minimum and maximum length.

    Attributes:
        column (str): The string column to validate.
        min_length (int): Minimum valid length (must be > 0).
        max_length (int): Maximum valid length (must be >= min_length).
        inclusive (tuple[bool, bool]): Tuple indicating inclusiveness of min and max bounds.
    """

    check_class = StringLengthBetweenCheck

    column: str = Field(..., description="The column to validate.")
    min_length: int = Field(..., description="Minimum allowed string length (must be > 0).")
    max_length: int = Field(..., description="Maximum allowed string length.")
    inclusive: tuple[bool, bool] = Field((True, True), description="Inclusiveness for (min, max) bounds.")

    @model_validator(mode="after")
    def validate_range(self) -> "StringLengthBetweenCheckConfig":
        """
        Validates that the min/max configuration is logically sound.

        Returns:
            StringLengthBetweenCheckConfig: Validated instance.

        Raises:
            InvalidCheckConfigurationError: If min_length > max_length or values are invalid.
        """
        if self.min_length <= 0:
            raise InvalidCheckConfigurationError(f"min_length ({self.min_length}) must be greater than 0")
        if self.max_length < self.min_length:
            raise InvalidCheckConfigurationError("max_length must be greater than or equal to min_length")
        return self
