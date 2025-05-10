from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class ColumnLessThanCheck(BaseRowCheck):
    """
    Row-level data quality check that flags rows where a comparison between two columns fails.

    This check ensures that values in `smaller_column` are less than (or less than or equal to,
    if `inclusive=True`) the values in `greater_column`. It is designed to enforce business rules
    such as "start_time <= end_time" or "amount < limit".

    Rows with nulls in either column are considered invalid.

    Attributes:
        smaller_column (str): The column expected to contain smaller (or equal) values.
        greater_column (str): The column expected to contain greater values.
        inclusive (bool): Whether to allow equality (i.e. smaller_column <= greater_column).
            Defaults to False.
    """

    def __init__(
        self,
        check_id: str,
        smaller_column: str,
        greater_column: str,
        inclusive: bool = False,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a ColumnLessThanCheck instance.

        Args:
            check_id (str): Unique identifier for the check. Will be used as the result column name.
            smaller_column (str): Column expected to contain smaller (or equal) values.
            greater_column (str): Column expected to contain greater values.
            inclusive (bool, optional): If True, the check allows equal values (<=). Defaults to False.
            severity (Severity, optional): Severity level of failed rows. Defaults to CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.smaller_column = smaller_column
        self.greater_column = greater_column
        self.inclusive = inclusive

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Apply the comparison and flag violations.

        Rows are marked as failed if:
        - the comparison fails (`smaller_column >= greater_column` or `>`, depending on `inclusive`), OR
        - either column is null

        Args:
            df (DataFrame): Input Spark DataFrame to validate.

        Returns:
            DataFrame: A new DataFrame with an additional boolean column where True indicates failure.

        Raises:
            MissingColumnError: If either column is not found in the DataFrame.
        """
        for col in [self.smaller_column, self.greater_column]:
            if col not in df.columns:
                raise MissingColumnError(col, df.columns)

        smaller = F.col(self.smaller_column)
        greater = F.col(self.greater_column)

        if self.inclusive:
            fail_expr = (smaller > greater) | smaller.isNull() | greater.isNull()
        else:
            fail_expr = (smaller >= greater) | smaller.isNull() | greater.isNull()

        return self.with_check_result_column(df, fail_expr)


@register_check_config(check_name="column-less-than-check")
class ColumnLessThanCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the ColumnLessThanCheck.

    This config defines a row-level comparison between two columns, ensuring that
    values in `smaller_column` are strictly less than (or less than or equal to,
    if `inclusive=True`) the values in `greater_column`.

    Null values in either column are treated as invalid and will fail the check.

    Example:
        ColumnLessThanCheckConfig(
            check_id="start-before-end",
            smaller_column="start_time",
            greater_column="end_time",
            inclusive=True
        )

    Attributes:
        smaller_column (str): Column expected to contain smaller (or equal) values.
        greater_column (str): Column expected to contain greater values.
        inclusive (bool): If True, validates `smaller_column <= greater_column`.
                          If False, requires strict inequality (`<`). Defaults to False.
    """

    check_class = ColumnLessThanCheck

    smaller_column: str = Field(..., description="The column expected to contain smaller (or equal) values.")
    greater_column: str = Field(..., description="The column expected to contain greater values.")
    inclusive: bool = Field(
        False, description="If True, allows equality (<=). Otherwise requires strict inequality (<)."
    )
