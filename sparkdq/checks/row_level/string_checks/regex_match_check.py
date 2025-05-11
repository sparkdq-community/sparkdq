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
    Row-level check that flags rows where a string column does not match a given regular expression pattern.

    Null values are skipped by default. If `treat_null_as_failure` is True, nulls are treated as invalid.

    Attributes:
        column (str): Column to validate.
        pattern (str): Regex pattern to match.
        ignore_case (bool): Whether the match should ignore case (defaults to False).
        treat_null_as_failure (bool): Whether nulls should be treated as failed (defaults to False).
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
        Initialize a RegexMatchCheck instance.

        Args:
            check_id (str): Unique identifier of the check.
            column (str): Name of the column to validate.
            pattern (str): Regex pattern to apply.
            ignore_case (bool): If True, performs case-insensitive matching.
            treat_null_as_failure (bool): If True, nulls are treated as failed.
            severity (Severity): Severity level of the check.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.pattern = pattern
        self.ignore_case = ignore_case
        self.treat_null_as_failure = treat_null_as_failure

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Applies the regex check on the specified column.

        Rows are marked as failed if:
        - the value is not null and does not match the regex
        - or the value is null and `treat_null_as_failure` is True

        Args:
            df (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: DataFrame with additional result column indicating check failures.

        Raises:
            MissingColumnError: If the column does not exist in the DataFrame.
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
    Configuration for RegexMatchCheck.

    Validates that a string column matches a given regex pattern.

    Attributes:
        column (str): Column to validate.
        pattern (str): Regex pattern to use for matching.
        ignore_case (bool): If True, regex is case-insensitive (default: False).
        treat_null_as_failure (bool): If True, null values are marked as failed (default: False).
    """

    check_class = RegexMatchCheck

    column: str = Field(..., description="The column to validate against the regex.")
    pattern: str = Field(..., description="The regex pattern to match.")
    ignore_case: bool = Field(False, description="Perform case-insensitive matching if True.")
    treat_null_as_failure: bool = Field(False, description="Mark nulls as failed if True.")
