from typing import List

from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.factory.check_config_registry import register_check_config


class NotNullCheck(BaseRowCheck):
    """
    Row-level data quality check that flags rows where any of the specified columns is *not null*.

    This check appends a boolean result column to the input DataFrame.
    Each row will be marked as a failure (True) if any of the specified columns contain a value.
    This is useful in scenarios where columns are expected to remain empty (null) under normal conditions.

    If any of the target columns do not exist in the input DataFrame, a
    `MissingColumnError` is raised at runtime to signal a misconfiguration.

    Attributes:
        columns (List[str]): List of column names to validate for null values (i.e. should remain unset).
    """

    def __init__(self, check_id: str, columns: List[str], severity: Severity = Severity.CRITICAL):
        """
        Initialize a NotNullCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance. Used as the
                name of the result column in the output DataFrame.
            columns (List[str]): List of column names that are expected to be null.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the not-null check on the given DataFrame.

        This method appends a new boolean column (named after `check_id`) that indicates
        for each row whether any of the specified columns is *not* null.

        Args:
            df (DataFrame): The input Spark DataFrame to validate.

        Returns:
            DataFrame: A new DataFrame with an additional boolean column where
                `True` indicates a non-null value (i.e. check failed), and `False` means valid.

        Raises:
            MissingColumnError: If any of the specified columns do not exist in the DataFrame.
        """
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        # Build a Spark array where each element checks if the corresponding column is NOT NULL
        not_null_checks = F.array(*[F.col(c).isNotNull() for c in self.columns])

        # Reduce the array by OR-ing all not-null checks
        any_not_null_expr = F.reduce(not_null_checks, F.lit(False), lambda acc, x: acc | x)

        return self.with_check_result_column(df, any_not_null_expr)


@register_check_config(check_name="not-null-check")
class NotNullCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the NotNullCheck.

    This check fails rows where any of the given columns is not null.
    It is typically used to enforce that fields remain empty (null) under certain business rules.

    Attributes:
        columns (List[str]): The names of the columns that should remain null.
    """

    check_class = NotNullCheck
    columns: List[str] = Field(..., description="The list of columns that must remain null")
