from typing import List

from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class NotNullCheck(BaseRowCheck):
    """
    Record-level validation check that identifies rows with unexpected non-null values.

    Validates that specified columns remain null across all records, flagging any
    rows where these columns contain values. This check is particularly useful for
    validating optional fields that should remain unset under normal business
    conditions or for identifying data quality issues in columns that should be
    consistently empty.

    The check performs runtime schema validation to ensure all specified columns
    exist in the target dataset, providing clear error messages for configuration
    mismatches.

    Attributes:
        columns (List[str]): Column names that should consistently contain null values.
    """

    def __init__(self, check_id: str, columns: List[str], severity: Severity = Severity.CRITICAL):
        """
        Initialize the not-null validation check with target columns and configuration.

        Args:
            check_id (str): Unique identifier for this check instance, used for
                result column naming and tracking.
            columns (List[str]): Column names that should consistently contain null values.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the validation logic against the provided dataset.

        Performs schema validation to ensure all target columns exist, then applies
        the not-null validation logic to identify records where any specified column
        contains a value. The results are appended as a boolean column indicating
        validation failures.

        Args:
            df (DataFrame): The dataset to validate against null value expectations.

        Returns:
            DataFrame: Original dataset augmented with a boolean result column where
                True indicates validation failure (unexpected non-null values) and
                False indicates compliance with null value requirements.

        Raises:
            MissingColumnError: When any specified column is not present in the
                dataset schema, indicating a configuration mismatch.
        """
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        # Build a Spark array where each element checks if the corresponding column is NOT NULL
        not_null_checks = F.array(*[F.col(c).isNotNull() for c in self.columns])

        # Reduce the array by OR-ing all not-null checks
        any_not_null_expr = F.aggregate(not_null_checks, F.lit(False), lambda acc, x: acc | x)

        return self.with_check_result_column(df, any_not_null_expr)


@register_check_config(check_name="not-null-check")
class NotNullCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for not-null validation checks.

    Defines the parameters required for configuring checks that validate columns
    expected to remain null. This configuration enables declarative check
    definition through external configuration sources.

    Attributes:
        columns (List[str]): Column names that should consistently contain null values.
    """

    check_class = NotNullCheck
    columns: List[str] = Field(..., description="The list of columns that must remain null")
