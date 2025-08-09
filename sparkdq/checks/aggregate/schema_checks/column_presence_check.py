from pydantic import Field
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.plugin.check_config_registry import register_check_config


class ColumnPresenceCheck(BaseAggregateCheck):
    """
    Dataset-level validation check that enforces required column presence.

    Validates that all specified columns are present in the dataset schema,
    ensuring structural integrity and compatibility with downstream processing
    requirements. This check is essential for detecting schema changes, missing
    data sources, or configuration mismatches that could impact data processing
    pipelines.

    The check focuses solely on column name presence and does not validate
    data types or column content, providing targeted schema structure validation.

    Attributes:
        required_columns (list[str]): Column names that must be present in the dataset schema.
    """

    def __init__(
        self,
        check_id: str,
        required_columns: list[str],
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize the column presence validation check with required column specifications.

        Args:
            check_id (str): Unique identifier for this check instance.
            required_columns (list[str]): Column names that must be present in the dataset schema.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.required_columns = required_columns

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Execute the column presence validation against the dataset schema.

        Compares the required column list against the actual dataset schema to
        identify any missing columns. The validation passes only when all required
        columns are present in the dataset.

        Args:
            df (DataFrame): The dataset to evaluate for column presence compliance.

        Returns:
            AggregateEvaluationResult: Validation outcome including pass/fail status
                and detailed metrics listing any missing columns.
        """
        actual_columns = set(df.columns)
        missing_columns = [col for col in self.required_columns if col not in actual_columns]
        passed = not missing_columns

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "missing_columns": missing_columns,
            },
        )


@register_check_config(check_name="column-presence-check")
class ColumnPresenceCheckConfig(BaseAggregateCheckConfig):
    """
    Configuration schema for column presence validation checks.

    Defines the parameters required for configuring checks that enforce required
    column presence in dataset schemas. This configuration enables declarative
    check definition through external configuration sources.

    Attributes:
        required_columns (list[str]): Column names that must be present in the dataset schema.
    """

    check_class = ColumnPresenceCheck

    required_columns: list[str] = Field(
        ...,
        description="List of required column names that must be present in the DataFrame",
        alias="required-columns",
    )
