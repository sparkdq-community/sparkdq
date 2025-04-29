from pydantic import Field
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.factory.check_config_registry import register_check_config


class ColumnPresenceCheck(BaseAggregateCheck):
    """
    Aggregate-level data quality check that verifies that all required columns are present in the DataFrame.

    This check does not validate data types, only the presence of specified column names.

    Attributes:
        required_columns (list[str]): List of column names that must be present.
    """

    def __init__(
        self,
        check_id: str,
        required_columns: list[str],
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new ColumnPresenceCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            required_columns (list[str]): List of required column names.
            severity (Severity, optional): The severity level to assign if the check fails.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.required_columns = required_columns

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate whether all required columns are present in the given DataFrame.

        Args:
            df (DataFrame): The Spark DataFrame to evaluate.

        Returns:
            AggregateEvaluationResult: An object indicating the outcome of the check,
            including a list of missing columns, if any.
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
    Declarative configuration model for the ColumnPresenceCheck.

    This config defines a set of required column names that must exist in the DataFrame.

    Attributes:
        required_columns (list[str]): The list of required column names.
    """

    check_class = ColumnPresenceCheck

    required_columns: list[str] = Field(
        ..., description="List of required column names that must be present in the DataFrame"
    )
