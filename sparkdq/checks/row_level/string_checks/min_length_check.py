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
    Record-level validation check that enforces minimum string length requirements.

    Validates that string values in the specified column meet minimum length
    criteria, ensuring data completeness and quality standards. This check is
    essential for validating required fields such as names, descriptions, or
    identifiers that must contain meaningful content.

    The check supports both inclusive and exclusive boundary semantics, enabling
    precise control over string length validation requirements. Null values are
    considered valid and do not trigger validation failures.

    Attributes:
        column (str): String column name that must meet minimum length requirements.
        min_length (int): Minimum acceptable string length threshold.
        inclusive (bool): Whether the minimum length threshold includes the boundary value itself.
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
        Initialize the minimum string length validation check with threshold configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            column (str): String column name that must meet minimum length requirements.
            min_length (int): Minimum acceptable string length threshold.
            inclusive (bool, optional): Whether the minimum length threshold includes the
                boundary value itself. Defaults to True for inclusive comparison.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.min_length = min_length
        self.inclusive = inclusive

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the minimum string length validation logic against the configured column.

        Performs schema validation to ensure the target column exists, then applies
        length validation logic to non-null string values. Records fail validation
        when string values fall below the configured minimum length threshold.

        Args:
            df (DataFrame): The dataset to validate for string length compliance.

        Returns:
            DataFrame: Original dataset augmented with a boolean result column where
                True indicates validation failure (insufficient string length) and
                False indicates compliance with length requirements.

        Raises:
            MissingColumnError: When the configured column is not present in the
                dataset schema, indicating a configuration mismatch.
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
    Configuration schema for minimum string length validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce minimum string length requirements. The configuration includes
    logical validation to ensure length parameters are positive and meaningful.

    Attributes:
        column (str): String column name that must meet minimum length requirements.
        min_length (int): Minimum acceptable string length threshold (must be positive).
        inclusive (bool): Whether the minimum length threshold includes the boundary value itself.
    """

    check_class = StringMinLengthCheck

    column: str = Field(..., description="The column to validate for minimum string length.")
    min_length: int = Field(
        ..., description="Minimum allowed string length (must be > 0).", alias="min-length"
    )
    inclusive: bool = Field(
        True, description="If True, enforces length >= min_length. If False, enforces > min_length."
    )

    @model_validator(mode="after")
    def validate_min_length(self) -> "StringMinLengthCheckConfig":
        """
        Validate the logical consistency of the configured minimum length parameter.

        Ensures that the minimum length parameter is positive and meaningful for
        string validation purposes. This validation prevents configuration errors
        that would result in nonsensical validation conditions.

        Returns:
            StringMinLengthCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the minimum length parameter is
                zero or negative, indicating an invalid configuration.
        """
        if self.min_length <= 0:
            raise InvalidCheckConfigurationError(f"min_length ({self.min_length}) must be greater than 0")
        return self
