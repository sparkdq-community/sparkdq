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
    Record-level validation check that enforces maximum string length constraints.

    Validates that string values in the specified column remain within maximum
    length limits, ensuring data consistency and storage efficiency. This check
    is essential for validating field constraints such as database column limits,
    display requirements, or data processing boundaries.

    The check supports both inclusive and exclusive boundary semantics, enabling
    precise control over string length validation requirements. Null values are
    considered valid and do not trigger validation failures.

    Attributes:
        column (str): String column name that must remain within the maximum length limit.
        max_length (int): Maximum acceptable string length threshold.
        inclusive (bool): Whether the maximum length threshold includes the boundary value itself.
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
        Initialize the maximum string length validation check with threshold configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            column (str): String column name that must remain within the maximum length limit.
            max_length (int): Maximum acceptable string length threshold.
            inclusive (bool, optional): Whether the maximum length threshold includes the
                boundary value itself. Defaults to True for inclusive comparison.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.max_length = max_length
        self.inclusive = inclusive

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the maximum string length validation logic against the configured column.

        Performs schema validation to ensure the target column exists, then applies
        length validation logic to non-null string values. Records fail validation
        when string values exceed the configured maximum length threshold.

        Args:
            df (DataFrame): The dataset to validate for string length compliance.

        Returns:
            DataFrame: Original dataset augmented with a boolean result column where
                True indicates validation failure (excessive string length) and
                False indicates compliance with length requirements.

        Raises:
            MissingColumnError: When the configured column is not present in the
                dataset schema, indicating a configuration mismatch.
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
    Configuration schema for maximum string length validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce maximum string length constraints. The configuration includes
    logical validation to ensure length parameters are positive and meaningful.

    Attributes:
        column (str): String column name that must remain within the maximum length limit.
        max_length (int): Maximum acceptable string length threshold (must be positive).
        inclusive (bool): Whether the maximum length threshold includes the boundary value itself.
    """

    check_class = StringMaxLengthCheck

    column: str = Field(..., description="The column to validate for maximum string length.")
    max_length: int = Field(..., description="Maximum allowed string length.", alias="max-length")
    inclusive: bool = Field(
        True, description="If True, allows equality (<=). If False, requires strictly less (<)."
    )

    @model_validator(mode="after")
    def validate_max_length(self) -> "StringMaxLengthCheckConfig":
        """
        Validate the logical consistency of the configured maximum length parameter.

        Ensures that the maximum length parameter is positive and meaningful for
        string validation purposes. This validation prevents configuration errors
        that would result in nonsensical validation conditions.

        Returns:
            StringMaxLengthCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the maximum length parameter is
                zero or negative, indicating an invalid configuration.
        """
        if self.max_length <= 0:
            raise InvalidCheckConfigurationError(f"max_length ({self.max_length}) must be greater than 0")
        return self
