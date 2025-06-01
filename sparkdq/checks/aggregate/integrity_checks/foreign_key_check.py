from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseAggregateCheck, IntegrityCheckMixin
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class ForeignKeyCheck(BaseAggregateCheck, IntegrityCheckMixin):
    """
    Aggregate check to verify that all values in a given column exist in a reference dataset column.

    This check enforces referential integrity between datasets. It fails if any value in the source column
    does not have a match in the reference column.
    """

    def __init__(
        self,
        check_id: str,
        column: str,
        reference_dataset: str,
        reference_column: str,
        severity: Severity = Severity.CRITICAL,
    ) -> None:
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.reference_dataset = reference_dataset
        self.reference_column = reference_column

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Perform a left join to find unmatched values.

        The check fails if there is at least one row in `df` whose value in `column` is not present
        in the reference dataset's `reference_column`.
        """
        if self.column not in df.columns:
            raise MissingColumnError(self.column, df.columns)

        ref_df = self.get_reference_df(self.reference_dataset)
        if self.reference_column not in ref_df.columns:
            raise MissingColumnError(self.reference_column, ref_df.columns)

        ref_alias = "__ref_fk__"
        source_alias = "__src_fk__"

        df_aliased = df.selectExpr(f"{self.column} as join_col").alias(source_alias)
        df_ref_aliased = ref_df.selectExpr(f"{self.reference_column} as join_col").alias(ref_alias)

        joined = df_aliased.join(df_ref_aliased, on="join_col", how="left")

        missing_count = joined.filter(F.col(f"{ref_alias}.join_col").isNull()).count()
        total_count = df.count()

        passed = missing_count == 0

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "missing_foreign_keys": missing_count,
                "total_rows": total_count,
                "missing_ratio": missing_count / total_count if total_count > 0 else None,
            },
        )


@register_check_config(check_name="foreign-key-check")
class ForeignKeyCheckConfig(BaseAggregateCheckConfig):
    """
    Configuration model for ForeignKeyCheck.

    Validates referential integrity by ensuring that all values in `column` exist in a reference dataset.
    """

    check_class = ForeignKeyCheck

    column: str = Field(..., description="The column in the source DataFrame to validate")
    reference_dataset: str = Field(..., description="The name of the reference dataset to compare against")
    reference_column: str = Field(
        ..., description="The column in the reference dataset that contains valid values"
    )
