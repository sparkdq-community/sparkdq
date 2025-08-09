from typing import List

from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class NullCheck(BaseRowCheck):
    """
    Record-level validation check that identifies rows containing null values.

    Validates that specified columns contain non-null values across all records,
    flagging any rows where these columns are null. This check is essential for
    enforcing data completeness requirements and identifying missing data issues
    that could impact downstream processing or analysis.

    The check uses OR-based failure logic, where any null value in the configured
    columns marks the entire record as invalid.

    Attributes:
        columns (List[str]): Column names that must contain non-null values.
    """

    def __init__(self, check_id: str, columns: List[str], severity: Severity = Severity.CRITICAL):
        """
        Initialize the null value validation check with target columns and configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Column names that must contain non-null values.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the null value validation logic against the configured columns.

        Performs schema validation to ensure all target columns exist, then applies
        null detection logic with OR-based failure semantics. Records fail validation
        when any configured column contains null values.

        Args:
            df (DataFrame): The dataset to validate for null value compliance.

        Returns:
            DataFrame: Original dataset augmented with a boolean result column where
                True indicates validation failure (null values detected) and False
                indicates compliance with non-null requirements.

        Raises:
            MissingColumnError: When any configured column is not present in the
                dataset schema, indicating a configuration mismatch.
        """
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        # Build a Spark array column where each element checks if the corresponding column is NULL
        null_checks = F.array(*[F.col(c).isNull() for c in self.columns])

        # Reduce the array by OR-ing all null checks
        any_null_expr = F.aggregate(null_checks, F.lit(False), lambda acc, x: acc | x)

        return self.with_check_result_column(df, any_null_expr)


@register_check_config(check_name="null-check")
class NullCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for null value validation checks.

    Defines the parameters required for configuring checks that enforce non-null
    value requirements. This configuration enables declarative check definition
    through external configuration sources while ensuring parameter validity.

    Attributes:
        columns (List[str]): Column names that must contain non-null values for
            records to pass validation.
    """

    check_class = NullCheck
    columns: List[str] = Field(..., description="The list of columns to check for null values")
