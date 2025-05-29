from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck, IntegrityCheckMixin
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class ForeignKeyCheck(BaseRowCheck, IntegrityCheckMixin):
    """
    A row-level integrity check that validates whether each value in a given column
    exists in a reference dataset's column.
    """

    def __init__(
        self,
        check_id: str,
        column: str,
        reference_dataset: str,
        reference_column: str,
    ) -> None:
        """
        Initializes a ForeignKeyCheck instance.

        Args:
            check_id (str): Unique identifier for the check.
            column (str): The name of the column in the source DataFrame to validate.
            reference_dataset (str): The name of the reference dataset (must be injected).
            reference_column (str): The name of the column in the reference dataset to check against.
        """
        self.check_id = check_id
        self.column = column
        self.reference_dataset = reference_dataset
        self.reference_column = reference_column

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Spark-native execution of the foreign key check using a left join.

        Appends a boolean result column indicating whether the value in `column`
        does NOT exist in the reference dataset.
        """
        if self.column not in df.columns:
            raise MissingColumnError(self.column, df.columns)

        ref_df = self.get_reference_df(self.reference_dataset)
        ref_values_df = ref_df.select(self.reference_column).distinct()

        # Perform left join with reference dataset
        joined_df = df.join(
            ref_values_df,
            on=df[self.column] == ref_values_df[self.reference_column],
            how="left",
        )

        # Check failed = reference value is null (i.e. no match found)
        failure_expr = F.col(self.reference_column).isNull()

        return self.with_check_result_column(joined_df, failure_expr)


@register_check_config(check_name="foreign-key-check")
class ForeignKeyCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the ForeignKeyCheck.

    This check validates that all values in a given source column exist
    in a reference dataset's column. It is typically used to ensure
    referential integrity between datasets.

    Attributes:
        column (str): The name of the column in the source DataFrame to validate.
        reference_dataset (str): The name of the reference dataset that must be injected
            into the validation engine before execution.
        reference_column (str): The name of the column in the reference dataset
            that defines the set of valid values.
    """

    check_class = ForeignKeyCheck

    column: str = Field(..., description="The column in the source DataFrame to validate")
    reference_dataset: str = Field(..., description="The name of the reference dataset to compare against")
    reference_column: str = Field(
        ..., description="The column in the reference dataset that contains valid values"
    )
