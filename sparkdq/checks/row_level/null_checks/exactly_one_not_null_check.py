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
    Record-level validation check that enforces mutual exclusivity constraints.

    Validates that exactly one column from a specified set contains a non-null
    value per record, ensuring mutual exclusivity among related fields. This
    check is essential for enforcing business rules where multiple options
    exist but only one should be selected, such as contact methods, payment
    types, or classification categories.

    The check fails when zero columns or multiple columns contain values,
    ensuring strict adherence to single-choice constraints.

    Attributes:
        columns (List[str]): Column names where exactly one must contain a non-null value.
    """

    def __init__(self, check_id: str, columns: List[str], severity: Severity = Severity.CRITICAL):
        """
        Initialize the mutual exclusivity validation check with target columns.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Column names where exactly one must contain a non-null value.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the mutual exclusivity validation logic against the configured columns.

        Performs schema validation to ensure all target columns exist, then applies
        counting logic to verify exactly one column contains a non-null value per
        record. Records fail validation when zero or multiple columns contain values.

        Args:
            df (DataFrame): The dataset to validate for mutual exclusivity compliance.

        Returns:
            DataFrame: Original dataset augmented with a boolean result column where
                True indicates validation failure (not exactly one non-null value) and
                False indicates compliance with exclusivity requirements.

        Raises:
            MissingColumnError: When any configured column is not present in the
                dataset schema, indicating a configuration mismatch.
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
    Configuration schema for mutual exclusivity validation checks.

    Defines the parameters required for configuring checks that enforce exactly-one
    constraints among related columns. This configuration enables declarative
    check definition through external configuration sources.

    Attributes:
        columns (List[str]): Column names where exactly one must contain a non-null value.
    """

    check_class = ExactlyOneNotNullCheck
    columns: List[str] = Field(..., description="The list of columns where exactly one must be non-null")
