from abc import ABC, abstractmethod
from decimal import Decimal
from typing import List, Optional, Tuple, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError

SupportsComparison = Union[int, float, Decimal, str]


class BaseComparisonCheck(BaseRowCheck, ABC):
    """
    Abstract base class for comparison-based row-level checks like Min, Max, Between, Exact.

    Subclasses must implement the `comparison_condition` method, which defines the
    boolean expression for a single column. Multiple columns are combined with OR logic:
    if any column fails the check, the row is considered invalid.

    Attributes:
        columns (List[str]): The list of columns to apply the comparison on.
        cast_type (Optional[str]): Optional Spark SQL type for casting columns (e.g., 'date', 'timestamp').
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        severity: Severity = Severity.CRITICAL,
        cast_type: Optional[str] = None,
    ):
        """
        Initialize the BaseComparisonCheck.

        Args:
            check_id (str): Unique identifier for the check.
            columns (List[str]): Column names to which the check is applied.
            severity (Severity, optional): Severity level of the check. Defaults to CRITICAL.
            cast_type (Optional[str], optional): Optional Spark SQL data type to cast the columns.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns
        self.cast_type = cast_type

    @abstractmethod
    def comparison_condition(self, column: Column) -> Column:
        """
        Define the condition that determines whether a value is invalid.

        This method must be implemented by all subclasses.

        Args:
            column (Column): The Spark column expression (casted if applicable).

        Returns:
            Column: A boolean expression where True indicates a failure.
        """
        ...

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Apply the comparison check to the specified columns.

        The check fails for a row if **any** of the columns violates the comparison condition.

        Args:
            df (DataFrame): The input DataFrame to validate.

        Returns:
            DataFrame: The DataFrame with an additional boolean column (named after `check_id`),
                       where True indicates a failed check.

        Raises:
            MissingColumnError: If any of the specified columns do not exist in the DataFrame.
        """
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        # For each specified column, cast the column if needed and apply the comparison condition.
        column_exprs = F.array(*[self.comparison_condition(self._apply_cast(F.col(c))) for c in self.columns])

        # Reduce (aggregate) the array of boolean expressions into a single boolean expression
        result_expr = F.reduce(column_exprs, F.lit(False), lambda acc, x: acc | x)

        return self.with_check_result_column(df, result_expr)

    def _apply_cast(self, column: Column) -> Column:
        """
        Cast the column to the specified type if `cast_type` is set.

        Args:
            column (Column): The original Spark column.

        Returns:
            Column: The (optionally) casted column.
        """
        return column.cast(self.cast_type) if self.cast_type else column


class BaseMinCheck(BaseComparisonCheck):
    """
    Abstract base class for all minimum bound checks.

    This check flags rows where **any** of the specified columns fall **below** a defined minimum value.
    The comparison condition can be inclusive (>=) or exclusive (>), depending on the configuration.

    Attributes:
        min_value (SupportsRichComparison): The lower bound for the comparison.
        inclusive (bool, optional): Whether the minimum is inclusive (>=) or exclusive (>).
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: SupportsComparison,
        inclusive: bool = False,
        severity: Severity = Severity.CRITICAL,
        cast_type: str | None = None,
    ):
        """
        Initialize the base minimum check.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): Columns to which the minimum condition applies.
            min_value (SupportsRichComparison): The lower bound for the comparison.
            inclusive (bool, optional): Whether the minimum is inclusive (>=) or exclusive (>).
            severity (Severity, optional): Severity level of the check result.
            cast_type (str | None, optional): Optional type to cast columns to before comparison.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type=cast_type)
        self.min_value = min_value
        self.inclusive = inclusive

    def comparison_condition(self, column: Column) -> Column:
        """
        Defines the comparison condition that flags values below the minimum.

        Args:
            column (Column): The Spark column to evaluate.

        Returns:
            Column: Boolean expression that returns True when the value fails the check.
        """
        if self.inclusive:
            return column < F.lit(self.min_value)
        else:
            return column <= F.lit(self.min_value)


class BaseMaxCheck(BaseComparisonCheck):
    """
    Abstract base class for all maximum bound checks.

    This check flags rows where **any** of the specified columns exceed a defined maximum value.
    The comparison condition can be inclusive (<=) or exclusive (<), depending on the configuration.

    Attributes:
        max_value (SupportsComparison): The upper bound for the comparison.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        max_value: SupportsComparison,
        inclusive: bool = False,
        severity: Severity = Severity.CRITICAL,
        cast_type: str | None = None,
    ):
        """
        Initialize the base maximum check.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): Columns to which the maximum condition applies.
            max_value (SupportsComparison): The upper bound for the comparison.
            inclusive (bool, optional): Whether the maximum is inclusive (<=) or exclusive (<).
            severity (Severity, optional): Severity level of the check result.
            cast_type (str | None, optional): Optional type to cast columns to before comparison.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type=cast_type)
        self.max_value = max_value
        self.inclusive = inclusive

    def comparison_condition(self, column: Column) -> Column:
        """
        Defines the comparison condition that flags values above the maximum.

        Args:
            column (Column): The Spark column to evaluate.

        Returns:
            Column: Boolean expression that returns True when the value fails the check.
        """
        if self.inclusive:
            return column > F.lit(self.max_value)
        else:
            return column >= F.lit(self.max_value)


class BaseBetweenCheck(BaseComparisonCheck):
    """
    Abstract base class for all range-bound checks.

    This check flags rows where **any** of the specified columns lie outside the defined range.
    The comparison condition supports control over inclusivity on both bounds.

    Attributes:
        min_value (SupportsComparison): The lower bound.
        max_value (SupportsComparison): The upper bound.
        inclusive (Tuple[bool, bool]): Whether to include min and/or max in the valid range.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: SupportsComparison,
        max_value: SupportsComparison,
        inclusive: Tuple[bool, bool] = (False, False),
        severity: Severity = Severity.CRITICAL,
        cast_type: str | None = None,
    ):
        """
        Initialize the base range check.

        Args:
            check_id (str): Unique identifier for the check instance.
            columns (List[str]): Columns to which the range condition applies.
            min_value (SupportsComparison): The lower bound of the valid range.
            max_value (SupportsComparison): The upper bound of the valid range.
            inclusive (Tuple[bool, bool], optional): Tuple controlling inclusivity for min and max bounds.
            severity (Severity, optional): Severity level of the check result.
            cast_type (str | None, optional): Optional type to cast columns to before comparison.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type=cast_type)
        self.min_value = min_value
        self.max_value = max_value
        self.inclusive = inclusive

    def comparison_condition(self, column: Column) -> Column:
        """
        Defines the comparison condition that flags values outside the valid range.

        Args:
            column (Column): The Spark column to evaluate.

        Returns:
            Column: Boolean expression that returns True when the value fails the check.
        """
        min_cmp = column < F.lit(self.min_value) if self.inclusive[0] else column <= F.lit(self.min_value)
        max_cmp = column > F.lit(self.max_value) if self.inclusive[1] else column >= F.lit(self.max_value)
        return min_cmp | max_cmp
