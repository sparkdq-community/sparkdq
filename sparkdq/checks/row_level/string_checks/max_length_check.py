from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class StringMaxLengthCheck(BaseRowCheck):
    """
    Row-level check that flags rows where string values exceed the allowed maximum length.

    Non-null values in the specified column are validated against a maximum string length.
    If `inclusive=True`, length must be <= `max_length`; otherwise strictly < `max_length`.

    Null values are considered valid.

    Attributes:
        column (str): Column to validate.
        max_length (int): Maximum allowed string length.
        inclusive (bool): Whether equality (≤) is allowed. Defaults to True.
    """

    def __init__(
        self,
        check_id: str,
        column: str,
        max_length: int,
        inclusive: bool = True,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a StringMaxLengthCheck instance.

        Args:
            check_id (str): Unique identifier for the check.
            column (str): Name of the column to validate.
            max_length (int): Maximum allowed string length.
            inclusive (bool, optional): If True, allows equality (<=). If False, enforces strictly less (<).
            severity (Severity, optional): Severity level of the check. Defaults to CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.max_length = max_length
        self.inclusive = inclusive

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Applies the maximum string length check to the input DataFrame.

        A row fails if:
        - the value is not null, and
        - the length exceeds the configured maximum (depending on `inclusive`)

        Args:
            df (DataFrame): Input DataFrame.

        Returns:
            DataFrame: Output with boolean check result column.

        Raises:
            MissingColumnError: If the column is not present in the DataFrame.
        """
        if self.column not in df.columns:
            raise MissingColumnError(self.column, df.columns)

        not_null_col = F.col(self.column).isNotNull()
        length_col = F.length(F.col(self.column))
        length_condition = length_col > self.max_length if self.inclusive else length_col >= self.max_length

        fail_expr = not_null_col & length_condition

        return self.with_check_result_column(df, fail_expr)


@register_check_config(check_name="string-max-length-check")
class StringMaxLengthCheckConfig(BaseRowCheckConfig):
    """
    Configuration for StringMaxLengthCheck.

    Ensures that string values do not exceed the specified maximum length.

    Attributes:
        column (str): Column to validate.
        max_length (int): Maximum allowed length (must be > 0).
        inclusive (bool): If True, length must be <= max_length. If False, strictly < max_length.
    """

    check_class = StringMaxLengthCheck

    column: str = Field(..., description="The column to validate for maximum string length.")
    max_length: int = Field(..., description="Maximum allowed length of the string values.")
    inclusive: bool = Field(
        True, description="If True, allows equality (<=). If False, requires strictly less (<)."
    )

    @model_validator(mode="after")
    def validate_max_length(self) -> "StringMaxLengthCheckConfig":
        """
        Validate that max_length is greater than 0.

        Returns:
            StringMaxLengthCheckConfig: The validated object.

        Raises:
            InvalidCheckConfigurationError: If max_length <= 0.
        """
        if self.max_length <= 0:
            raise InvalidCheckConfigurationError(f"max_length ({self.max_length}) must be greater than 0")
        return self
