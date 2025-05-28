from pydantic import Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidSQLExpressionError, MissingColumnError
from sparkdq.plugin.check_config_registry import register_check_config


class ColumnGreaterThanCheck(BaseRowCheck):
    """
    Row-level data quality check that flags rows where a comparison between a column and
    another column or a SQL expression.

    This check ensures that values in `column` are greater than (or greater than or equal to,
    if `inclusive=True`) the result of evaluating `limit`. It is designed to enforce
    business rules such as "end_time >= start_time" or "selling_price > cost_price * 1.1".

    The limit can be:
    - A simple column name: "start_time"
    - A mathematical expression: "cost_price * 1.1"
    - A complex expression: "CASE WHEN level='expert' THEN min_score ELSE min_score * 0.9 END"

    Rows with nulls in either the column or the limit result are considered invalid.

    Attributes:
        column (str): The column expected to contain greater (or equal) values.
        limit (str): The column or a Spark SQL expression expected to contain smaller values.
        inclusive (bool): Whether to allow equality (i.e. column >= limit).
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
        Initialize a ColumnGreaterThanCheck instance.

        Args:
            check_id (str): Unique identifier for the check. Will be used as the result column name.
            column (str): Column expected to contain greater (or equal) values.
            limit (str): The column or a Spark SQL expression expected to contain smaller values.
            inclusive (bool, optional): If True, the check allows equal values (>=). Defaults to False.
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
        - the comparison fails (`column <= limit` or `<`, depending on `inclusive`), OR
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

        greater = F.col(self.column)
        smaller = F.expr(self.limit)

        if self.inclusive:
            fail_expr = (greater < smaller) | greater.isNull() | smaller.isNull()
        else:
            fail_expr = (greater <= smaller) | greater.isNull() | smaller.isNull()

        return self.with_check_result_column(df, fail_expr)


@register_check_config(check_name="column-greater-than-check")
class ColumnGreaterThanCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the ColumnGreaterThanCheck.

    This config defines a row-level comparison between a column and a Spark SQL expression,
    ensuring that values in `column` are strictly greater than (or greater than or equal to,
    if `inclusive=True`) the result of evaluating `limit`.

    Null values in either column or the limit result are treated as invalid
    and will fail the check.

    Example:
        >>> # Simple column comparison
        >>> cfg = ColumnGreaterThanCheckConfig(
        ...     check_id="end-after-start",
        ...     column="end_time",
        ...     limit="start_time",
        ...     inclusive=True
        ... )

        >>> # Expression with mathematical operation
        >>> cfg = ColumnGreaterThanCheckConfig(
        ...     check_id="price-with-margin",
        ...     column="selling_price",
        ...     limit="cost_price * 1.2",
        ...     inclusive=False
        ... )

        >>> # Complex conditional expression
        >>> cfg = ColumnGreaterThanCheckConfig(
        ...     check_id="score-validation",
        ...     column="user_score",
        ...     limit="CASE WHEN level='beginner' THEN min_score ELSE min_score * 1.1 END",
        ...     inclusive=True
        ... )

    Attributes:
        column (str): Column expected to contain greater (or equal) values.
        limit (str): The column or a Spark SQL expression expected to contain smaller values.
        inclusive (bool): If True, validates `column >= limit`.
                          If False, requires strict inequality (`>`). Defaults to False.
    """

    check_class = ColumnGreaterThanCheck

    column: str = Field(
        ..., description="The column expected to contain greater (or equal) values.", alias="column"
    )
    limit: str = Field(
        ...,
        description="The column or a Spark SQL expression expected to contain smaller values.",
        alias="limit",
    )
    inclusive: bool = Field(
        False, description="If True, allows equality (>=). Otherwise requires strict inequality (>)."
    )
