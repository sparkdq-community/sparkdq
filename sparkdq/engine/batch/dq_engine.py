from typing import Optional

from pyspark.sql import DataFrame

from sparkdq.core.base_check import ReferenceDatasetDict
from sparkdq.exceptions import MissingCheckSetError

from ..base_engine import BaseDQEngine
from .check_runner import BatchCheckRunner
from .validation_result import BatchValidationResult


class BatchDQEngine(BaseDQEngine):
    """
    Engine for executing data quality checks on Spark DataFrames in batch mode.

    This engine applies both row-level and aggregate-level checks using the
    ``BatchCheckRunner``, and annotates the DataFrame with error metadata.
    """

    def run_batch(
        self, df: DataFrame, reference_datasets: Optional[ReferenceDatasetDict] = None
    ) -> BatchValidationResult:
        """
        Run all registered checks against the given DataFrame.

        This method applies both row-level and aggregate-level checks and
        returns a validation result containing the annotated DataFrame and
        the aggregated check results.

        Args:
            df (DataFrame): The input Spark DataFrame to validate.
            reference_datasets (ReferenceDatasetDict, optional):
                A dictionary of named reference DataFrames used by integrity checks.
                Required for checks that compare values against external datasets
                (e.g., foreign key validation). Each key should match the
                `reference_dataset` name expected by the check.

        Returns:
            BatchValidationResult: Object containing the validated DataFrame,
            aggregate check results, and the original input schema.
        """
        if self.check_set is None:
            raise MissingCheckSetError()
        input_columns = df.columns
        runner = BatchCheckRunner(self.fail_levels)
        validated_df, aggregate_results = runner.run(df, self.check_set.get_all(), reference_datasets)

        return BatchValidationResult(validated_df, aggregate_results, input_columns)
