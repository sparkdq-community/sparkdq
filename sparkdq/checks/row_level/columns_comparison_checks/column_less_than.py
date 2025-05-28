from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidSQLExpressionError, MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class ColumnLessThanCheck(BaseRowCheck):
    """
    Row-level data quality check that flags rows where a comparison between two columns fails.

    This check ensures that values in `column` are less than (or less than or equal to,
    if `inclusive=True`) the result of evaluating `limit`. It is designed to enforce
    business rules such as "start_time <= end_time" or "selling_price < cost_price * 1.1".

    The limit can be:
    - A simple column name: "end_time"
    - A mathematical expression: "cost_price * 1.1"
    - A complex expression: "CASE WHEN level='expert' THEN max_score ELSE max_score * 0.9 END"

    Rows with nulls in either the column or the limit result are considered invalid.

    Attributes:
        column (str): The column expected to contain smaller (or equal) values.
        limit (str): The column or a Spark SQL expression expected to contain greater values.
        inclusive (bool): Whether to allow equality (i.e. column <= limit).
            Defaults to False.
    """

    def __init__(
        self,
        check_id: str,
        column: str,
        limit: str,
        inclusive: bool = False,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a ColumnLessThanCheck instance.

        Args:
            check_id (str): Unique identifier for the check. Will be used as the result column name.
            column (str): Column expected to contain smaller (or equal) values.
            limit (str): The column or a Spark SQL expression expected to contain greater values.
            inclusive (bool, optional): If True, the check allows equal values (<=). Defaults to False.
            severity (Severity, optional): Severity level of failed rows. Defaults to CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.limit = limit
        self.inclusive = inclusive

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Apply the comparison and flag violations.

        Rows are marked as failed if:
        - the comparison fails (`column >= limit` or `>`, depending on `inclusive`), OR
        - either the column or the limit result is null

        Args:
            df (DataFrame): Input Spark DataFrame to validate.

        Returns:
            DataFrame: A new DataFrame with an additional boolean column where True indicates failure.

        Raises:
            MissingColumnError: If column is not found in the DataFrame.
            InvalidSQLExpressionError: If the limit expression is invalid or cannot be evaluated.
        """
        if self.column not in df.columns:
            raise MissingColumnError(self.column, df.columns)

        # Validate that limit is a valid expression
        try:
            df.limit(0).select(F.expr(self.limit).alias("_validation_limit"))
        except Exception as e:
            raise InvalidSQLExpressionError(self.limit, str(e))

        smaller = F.col(self.column)
        greater = F.expr(self.limit)

        if self.inclusive:
            fail_expr = (smaller > greater) | smaller.isNull() | greater.isNull()
        else:
            fail_expr = (smaller >= greater) | smaller.isNull() | greater.isNull()

        return self.with_check_result_column(df, fail_expr)


@register_check_config(check_name="column-less-than-check")
class ColumnLessThanCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the ColumnLessThanCheck.

    This config defines a row-level comparison between a column and a Spark SQL expression,
    ensuring that values in `column` are strictly less than (or less than or equal to,
    if `inclusive=True`) the result of evaluating `limit`.

    Null values in either column or the limit result are treated as invalid
    and will fail the check.

    Examples:
        >>> # Simple column comparison
        >>> cfg = ColumnLessThanCheckConfig(
        ...     check_id="start-before-end",
        ...     column="start_time",
        ...     limit="end_time",
        ...     inclusive=True
        ... )

        >>> # Expression with mathematical operation
        >>> cfg = ColumnLessThanCheckConfig(
        ...     check_id="price-with-margin",
        ...     column="cost_price",
        ...     limit="selling_price * 0.8",
        ...     inclusive=True
        ... )

        >>> # Complex conditional expression
        >>> cfg = ColumnLessThanCheckConfig(
        ...     check_id="score-validation",
        ...     column="user_score",
        ...     limit="CASE WHEN level='expert' THEN max_score ELSE max_score * 0.9 END",
        ...     inclusive=False
        ... )

    Attributes:
        column (str): Column expected to contain smaller (or equal) values.
        limit (str): The column or a Spark SQL expression expected to contain greater values.
        inclusive (bool): If True, validates `column <= limit`.
                          If False, requires strict inequality (`<`). Defaults to False.
    """

    check_class = ColumnLessThanCheck

    column: str = Field(
        ..., description="The column expected to contain smaller (or equal) values.", alias="column"
    )
    limit: str = Field(
        ...,
        description="The column or a Spark SQL expression expected to contain greater values.",
        alias="limit",
    )
    inclusive: bool = Field(
        False, description="If True, allows equality (<=). Otherwise requires strict inequality (<)."
    )
