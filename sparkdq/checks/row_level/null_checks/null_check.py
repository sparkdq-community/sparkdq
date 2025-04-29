from typing import List

from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.factory.check_config_registry import register_check_config


class NullCheck(BaseRowCheck):
    """
    Row-level data quality check that flags null values in one or more specified columns.

    This check appends a boolean result column to the input DataFrame.
    A row is marked as failed (True) if **any** of the target columns are null.

    Attributes:
        columns (List[str]): Names of the columns to inspect for null values.
    """

    def __init__(self, check_id: str, columns: List[str], severity: Severity = Severity.CRITICAL):
        """
        Initialize a NullCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): Names of the columns to check for null values.
            severity (Severity, optional): Severity level of the check result.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the null check on the given DataFrame.

        This method appends a new boolean column (named after `check_id`) that indicates
        for each row whether **any** of the target columns are null.

        Args:
            df (DataFrame): The input Spark DataFrame to validate.

        Returns:
            DataFrame: A new DataFrame with an additional boolean column where
                `True` indicates a null value (i.e. check failed), and `False` means valid.

        Raises:
            MissingColumnError: If any of the specified columns do not exist in the DataFrame.
        """
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        # Build a Spark array column where each element checks if the corresponding column is NULL
        null_checks = F.array(*[F.col(c).isNull() for c in self.columns])

        # Reduce the array by OR-ing all null checks
        any_null_expr = F.reduce(null_checks, F.lit(False), lambda acc, x: acc | x)

        return self.with_check_result_column(df, any_null_expr)


@register_check_config(check_name="null-check")
class NullCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the NullCheck.

    This configuration model is used to define and validate parameters for the
    NullCheck when instantiated via dictionary or structured config input.

    Attributes:
        columns (List[str]): The names of the columns to check for null values.
            This is a required field and must match existing columns in the DataFrame.
    """

    check_class = NullCheck
    columns: List[str] = Field(..., description="The list of columns to check for null values")
