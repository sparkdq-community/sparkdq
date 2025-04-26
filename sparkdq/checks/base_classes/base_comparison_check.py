from abc import ABC, abstractmethod
from typing import List, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError


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

        # Build the condition across all columns (OR logic)
        first_col_expr = self._apply_cast(col(self.columns[0]))
        result_expr = self.comparison_condition(first_col_expr)

        for column in self.columns[1:]:
            current_expr = self.comparison_condition(self._apply_cast(col(column)))
            result_expr = result_expr | current_expr

        return df.withColumn(self.check_id, result_expr)

    def _apply_cast(self, column: Column) -> Column:
        """
        Cast the column to the specified type if `cast_type` is set.

        Args:
            column (Column): The original Spark column.

        Returns:
            Column: The (optionally) casted column.
        """
        return column.cast(self.cast_type) if self.cast_type else column
