from typing import List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import array, col, concat, lit, struct, when

from sparkdq.core.base_check import BaseAggregateCheck, BaseCheck, BaseRowCheck
from sparkdq.core.check_results import AggregateCheckResult
from sparkdq.core.severity import Severity


class BatchCheckRunner:
    """
    Executes row-level and aggregate-level data quality checks on a Spark DataFrame.

    This class is designed for use in batch validation engines. It handles both the
    transformation of the input DataFrame with row-level error annotations, and the
    evaluation of aggregate-level checks that do not modify the DataFrame.

    Attributes:
        fail_levels (List[Severity]): The severities that are considered critical
            for determining whether a row should be marked as failed.
    """

    def __init__(self, fail_levels: List[Severity]):
        """
        Initializes the runner with severity levels that count as failures.

        Args:
            fail_levels (List[Severity]): List of severities that should be treated
                as failures when computing the _dq_passed column.
        """
        self.fail_levels = fail_levels

    def run(self, df: DataFrame, checks: List[BaseCheck]) -> Tuple[DataFrame, List[AggregateCheckResult]]:
        """
        Runs all provided checks against the input DataFrame.

        Args:
            df (DataFrame): The Spark DataFrame to validate.
            checks (List[BaseCheck]): A list of BaseRowCheck or BaseAggregateCheck instances.

        Returns:
            Tuple[DataFrame, List[AggregateCheckResult]]: The annotated DataFrame and list of results.
        """
        # Split checks into row-level and aggregate-level
        row_checks = [c for c in checks if isinstance(c, BaseRowCheck)]
        agg_checks = [c for c in checks if isinstance(c, BaseAggregateCheck)]

        # Run row checks, collect error annotations and critical fail flags
        df, error_structs, fail_flags = self._run_row_checks(df, row_checks)

        # Compute _dq_passed column based on critical flags
        df = self._combine_failure_flags(df, fail_flags)

        # Combine all row-level error structs into _dq_errors array
        df = df.withColumn("_dq_errors", array(*[e for e in error_structs if e is not None]))

        # Run aggregate checks and collect results
        aggregate_results = self._run_aggregate_checks(df, agg_checks)
        failed_aggregates = [agg for agg in aggregate_results if not agg.passed]

        # Attach aggregate error information if any failed
        if failed_aggregates:
            df = self._attach_aggregate_errors(df, failed_aggregates)

            # If a failed aggregate has critical severity, mark all rows as failed
            if any(agg.severity in self.fail_levels for agg in failed_aggregates):
                df = df.withColumn("_dq_passed", lit(False))

        return df, aggregate_results

    def _run_row_checks(
        self, df: DataFrame, row_checks: List[BaseRowCheck]
    ) -> Tuple[DataFrame, List[Column], List[Column]]:
        """
        Applies all row-level checks and collects failure information.

        Args:
            df (DataFrame): The DataFrame to validate.
            row_checks (List[BaseRowCheck]): The row-level checks to apply.

        Returns:
            Tuple containing:
                - The transformed DataFrame with error indicator columns.
                - A list of error struct expressions (used to build _dq_errors).
                - A list of boolean expressions indicating critical check failures.
        """
        result_df = df
        error_structs: List[Column] = []
        fail_flags: List[Column] = []

        for check in row_checks:
            result_df = check.validate(result_df)

            # Construct a struct column if the check failed
            error_expr = when(
                col(check.check_id),
                struct(
                    lit(check.name).alias("check"),
                    lit(check.check_id).alias("check-id"),
                    lit(check.severity.value).alias("severity"),
                ),
            )
            error_structs.append(error_expr)

            # Collect critical fail flags
            if check.severity in self.fail_levels:
                fail_flags.append(col(check.check_id))

        return result_df, error_structs, fail_flags

    def _run_aggregate_checks(
        self, df: DataFrame, agg_checks: List[BaseAggregateCheck]
    ) -> List[AggregateCheckResult]:
        """
        Evaluates all aggregate-level checks on the given DataFrame.

        Args:
            df (DataFrame): The DataFrame to evaluate.
            agg_checks (List[BaseAggregateCheck]): The aggregate-level checks.

        Returns:
            List[AggregateCheckResult]: The result of each aggregate check.
        """
        return [check.evaluate(df) for check in agg_checks]

    def _combine_failure_flags(self, df: DataFrame, fail_flags: List[Column]) -> DataFrame:
        """
        Combines all failure flags into a single _dq_passed column.

        If no failure flags exist, all rows are marked as passed.

        Args:
            df (DataFrame): The input DataFrame.
            fail_flags (List[Column]): Boolean expressions representing failed checks.

        Returns:
            DataFrame: The DataFrame with the _dq_passed column added.
        """
        if not fail_flags:
            return df.withColumn("_dq_passed", lit(True))

        combined_flag = fail_flags[0]
        for cond in fail_flags[1:]:
            combined_flag = combined_flag | cond

        return df.withColumn("_dq_passed", ~combined_flag)

    def _attach_aggregate_errors(
        self, df: DataFrame, failed_aggregates: List[AggregateCheckResult]
    ) -> DataFrame:
        """
        Appends aggregate check failures to the _dq_errors column.

        Also adds an optional _dq_aggregate_errors column for visibility.

        Args:
            df (DataFrame): The DataFrame being validated.
            failed_aggregates (List[AggregateCheckResult]): Failed checks.

        Returns:
            DataFrame: The DataFrame with aggregate errors included.
        """
        aggregate_error_array = array(
            [
                struct(
                    lit(agg.check).alias("check"),
                    lit(agg.check_id).alias("check-id"),
                    lit(agg.severity.value).alias("severity"),
                )
                for agg in failed_aggregates
            ]
        )

        df = df.withColumn("_dq_aggregate_errors", aggregate_error_array)
        df = df.withColumn("_dq_errors", concat(col("_dq_errors"), col("_dq_aggregate_errors")))
        return df
