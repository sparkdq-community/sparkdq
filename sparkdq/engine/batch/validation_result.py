from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col, expr, lit

from sparkdq.core.check_results import AggregateCheckResult
from sparkdq.core.severity import Severity

from ..validation_summary import ValidationSummary


@dataclass(frozen=True)
class BatchValidationResult:
    """
    Encapsulates the results of a batch data quality validation run.

    Includes:
    - The validated Spark DataFrame, annotated with validation metadata.
    - A list of results from aggregate-level checks.
    - The original input column names, used to restore the pre-validation structure.

    This class provides convenience methods to access only the passing or failing rows,
    making it easier to route or analyze validated data downstream.

    Attributes:
        df (DataFrame): The DataFrame after validation, including:

            - _dq_passed (bool): Row-level pass/fail status.
            - _dq_errors (array): Structured errors from failed checks.
            - _dq_aggregate_errors (array, optional): Errors from failed aggregates.

        aggregate_results (List[AggregateCheckResult]): Results of all aggregate checks.
        input_columns (List[str]): Names of the original input columns.
        timestamp (datetime): Timestamp of when the validation result was created.
    """

    df: DataFrame
    aggregate_results: List[AggregateCheckResult]
    input_columns: List[str]
    timestamp: datetime = field(default_factory=datetime.now)

    def pass_df(self) -> DataFrame:
        """
        Return only the rows that passed all critical checks.

        This method filters for rows where `_dq_passed` is true and restores
        the original column structure from the input DataFrame.

        Returns:
            DataFrame: DataFrame containing only valid rows, with original schema.
        """
        return self.df.filter("_dq_passed").select(*self.input_columns)

    def fail_df(self) -> DataFrame:
        """
        Return only the rows that failed one or more critical checks.

        This includes validation metadata such as ``_dq_errors``, ``_dq_passed``, and,
        if present, ``_dq_aggregate_errors``. Additionally, a ``_dq_validation_ts`` column
        is added for downstream auditing or tracking.

        Returns:
            DataFrame: DataFrame containing invalid rows and relevant error metadata.
        """
        cols = self.input_columns + ["_dq_errors", "_dq_passed"]
        if "_dq_aggregate_errors" in self.df.columns:
            cols.append("_dq_aggregate_errors")
        df = self.df.filter("NOT _dq_passed").select(*cols)
        df = df.withColumn("_dq_validation_ts", lit(self.timestamp))
        return df

    def warn_df(self) -> DataFrame:
        """
        Returns rows that passed all critical checks but contain warning-level violations.

        These are rows where ``_dq_passed`` is True, but the ``_dq_errors`` array contains
        at least one entry with severity == **WARNING**.

        Returns:
            DataFrame: Filtered DataFrame of rows with warnings.
        """
        df = self.df.filter(
            col("_dq_passed")
            & array_contains(expr("transform(_dq_errors, x -> x.severity)"), Severity.WARNING.value)
        ).select(*self.input_columns + ["_dq_errors"])
        df = df.withColumn("_dq_validation_ts", lit(self.timestamp))
        return df

    def summary(self) -> ValidationSummary:
        """
        Create a summary of the validation results.

        This includes total record count, pass/fail statistics,
        and number of rows with warning-level errors.

        Returns:
            ValidationSummary: Structured summary of the validation outcome.
        """
        total = self.df.count()
        passed = self.df.filter("_dq_passed").count()
        failed = total - passed
        warnings = self.df.filter(
            col("_dq_passed")
            & array_contains(expr("transform(_dq_errors, x -> x.severity)"), Severity.WARNING.value)
        ).count()
        rate = round(passed / total if total else 0.0, 2)
        return ValidationSummary(
            total_records=total,
            passed_records=passed,
            failed_records=failed,
            warning_records=warnings,
            pass_rate=rate,
            timestamp=self.timestamp,
        )
