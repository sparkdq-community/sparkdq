from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class StringMinLengthCheck(BaseRowCheck):
    """
    Row-level check that flags rows where string values are shorter than the required length.

    This check ensures that non-null string values in the specified column are at least `min_length`
    characters long (or strictly longer, if `inclusive=False`). Null values are treated as valid.

    Attributes:
        column (str): Name of the string column to validate.
        min_length (int): Threshold for string length.
        inclusive (bool): If True, requires length >= min_length. If False, requires length > min_length.
    """

    def __init__(
        self,
        check_id: str,
        column: str,
        min_length: int,
        inclusive: bool = True,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a StringMinLengthCheck instance.

        Args:
            check_id (str): Unique identifier for the check.
            column (str): Name of the column to validate.
            min_length (int): Minimum required string length.
            inclusive (bool, optional): If True, allows equality. If False, requires strictly greater length.
                Defaults to True.
            severity (Severity, optional): Severity level of the check result. Defaults to CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.min_length = min_length
        self.inclusive = inclusive

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the min-length check on the specified column.

        A row fails if:

        - value is not null, AND
        - its length is < min_length (or <= min_length if inclusive=False)

        Args:
            df (DataFrame): Input DataFrame.

        Returns:
            DataFrame: DataFrame with an additional boolean column indicating check failures.

        Raises:
            MissingColumnError: If the column does not exist in the input DataFrame.
        """
        if self.column not in df.columns:
            raise MissingColumnError(self.column, df.columns)

        not_null_col = F.col(self.column).isNotNull()
        length_col = F.length(F.col(self.column))
        length_condition = length_col < self.min_length if self.inclusive else length_col <= self.min_length

        fail_expr = not_null_col & length_condition

        return self.with_check_result_column(df, fail_expr)


@register_check_config(check_name="string-min-length-check")
class StringMinLengthCheckConfig(BaseRowCheckConfig):
    """
    Configuration for StringMinLengthCheck.

    Ensures that all non-null values in the specified column have a minimum length.

    Attributes:
        column (str): Name of the string column to validate.
        min_length (int): Minimum allowed string length (must be > 0).
        inclusive (bool): If True, allows equality (>=). If False, requires strictly greater length (>).
    """

    check_class = StringMinLengthCheck

    column: str = Field(..., description="The column to validate for minimum string length.")
    min_length: int = Field(..., description="Minimum required length of string values (must be > 0).")
    inclusive: bool = Field(
        True, description="If True, enforces length >= min_length. If False, enforces > min_length."
    )

    @model_validator(mode="after")
    def validate_min_length(self) -> "StringMinLengthCheckConfig":
        """
        Validate that the configured `min_length` is greater than 0.

        Returns:
            StringMinLengthCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If `min_length` is not greater than 0.
        """
        if self.min_length <= 0:
            raise InvalidCheckConfigurationError(f"min_length ({self.min_length}) must be greater than 0")
        return self
