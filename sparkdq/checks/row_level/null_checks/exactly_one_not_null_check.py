from typing import List

from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class ExactlyOneNotNullCheck(BaseRowCheck):
    """
    Row-level data quality check that verifies that **exactly one** of the specified columns
    is non-null in each row.

    This check appends a boolean result column to the input DataFrame.
    A row is marked as failed (True) if none or more than one of the specified columns are non-null.

    Example use cases:
        - Ensure that a user has provided exactly one contact method: email, phone, or ID.
        - Validate exclusivity constraints among optional fields.

    Attributes:
        columns (List[str]): Names of the columns to inspect for non-null values.
    """

    def __init__(self, check_id: str, columns: List[str], severity: Severity = Severity.CRITICAL):
        """
        Initialize an ExactlyOneNotNullCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): Names of the columns to validate for exclusivity.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the check on the given DataFrame.

        This method appends a new boolean column (named after `check_id`) that indicates
        for each row whether **exactly one** of the target columns is non-null.

        Args:
            df (DataFrame): The input Spark DataFrame to validate.

        Returns:
            DataFrame: A new DataFrame with an additional boolean column where
                `True` indicates the row failed the check (not exactly one non-null), and `False` means valid.

        Raises:
            MissingColumnError: If any of the specified columns do not exist in the DataFrame.
        """
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        # Build an array of boolean expressions for each column being not null
        not_null_exprs = F.array(*[F.col(c).isNotNull().cast("int") for c in self.columns])

        # Sum up the number of non-null fields per row
        count_non_null = F.aggregate(not_null_exprs, F.lit(0), lambda acc, x: acc + x)

        # Fail if count is not exactly 1
        failed_condition = count_non_null != 1

        return self.with_check_result_column(df, failed_condition)


@register_check_config(check_name="exactly-one-not-null-check")
class ExactlyOneNotNullCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the ExactlyOneNotNullCheck.

    Attributes:
        columns (List[str]): The names of the columns where exactly one must be non-null per row.
    """

    check_class = ExactlyOneNotNullCheck
    columns: List[str] = Field(..., description="The list of columns where exactly one must be non-null")
