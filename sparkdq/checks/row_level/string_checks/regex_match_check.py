from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class RegexMatchCheck(BaseRowCheck):
    """
    Record-level validation check that enforces regular expression pattern matching.

    Validates that string values in the specified column conform to a defined
    regular expression pattern, ensuring data format consistency and business
    rule compliance. This check is essential for validating structured data
    such as email addresses, phone numbers, identifiers, or custom format
    requirements.

    The check provides flexible null handling and case sensitivity options,
    enabling precise control over pattern matching requirements for different
    validation scenarios.

    Attributes:
        column (str): String column name that must conform to the pattern.
        pattern (str): Regular expression pattern for validation.
        ignore_case (bool): Whether pattern matching should be case-insensitive.
        treat_null_as_failure (bool): Whether null values should be considered validation failures.
    """

    def __init__(
        self,
        check_id: str,
        column: str,
        pattern: str,
        ignore_case: bool = False,
        treat_null_as_failure: bool = False,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize the regular expression pattern validation check with matching configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            column (str): String column name that must conform to the pattern.
            pattern (str): Regular expression pattern for validation.
            ignore_case (bool): Whether pattern matching should be case-insensitive.
                Defaults to False for case-sensitive matching.
            treat_null_as_failure (bool): Whether null values should be considered
                validation failures. Defaults to False to skip null values.
            severity (Severity): Classification level for validation failures.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.pattern = pattern
        self.ignore_case = ignore_case
        self.treat_null_as_failure = treat_null_as_failure

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the regular expression pattern validation logic against the configured column.

        Performs schema validation to ensure the target column exists, then applies
        pattern matching logic with configurable case sensitivity and null handling.
        Records fail validation when string values do not conform to the specified
        regular expression pattern.

        Args:
            df (DataFrame): The dataset to validate for pattern compliance.

        Returns:
            DataFrame: Original dataset augmented with a boolean result column where
                True indicates validation failure (pattern mismatch) and False
                indicates compliance with pattern requirements.

        Raises:
            MissingColumnError: When the configured column is not present in the
                dataset schema, indicating a configuration mismatch.
        """
        if self.column not in df.columns:
            raise MissingColumnError(self.column, df.columns)

        col = F.col(self.column)
        pattern_expr = f"(?i){self.pattern}" if self.ignore_case else self.pattern
        match_expr = col.rlike(pattern_expr)

        if self.treat_null_as_failure:
            fail_expr = col.isNull() | ~match_expr
        else:
            fail_expr = col.isNotNull() & ~match_expr

        return self.with_check_result_column(df, fail_expr)


@register_check_config(check_name="regex-match-check")
class RegexMatchCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for regular expression pattern validation checks.

    Defines the parameters required for configuring checks that enforce regular
    expression pattern matching. This configuration enables declarative check
    definition through external configuration sources while providing flexible
    matching options.

    Attributes:
        column (str): String column name that must conform to the pattern.
        pattern (str): Regular expression pattern for validation.
        ignore_case (bool): Whether pattern matching should be case-insensitive.
        treat_null_as_failure (bool): Whether null values should be considered validation failures.
    """

    check_class = RegexMatchCheck

    column: str = Field(..., description="The column to validate against the regex.")
    pattern: str = Field(..., description="The regex pattern to match.")
    ignore_case: bool = Field(
        False, description="Perform case-insensitive matching if True.", alias="ignore-case"
    )
    treat_null_as_failure: bool = Field(
        False, description="Mark nulls as failed if True.", alias="treat-null-as-failure"
    )
