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
    Abstract foundation for comparison-based record-level validation checks.

    Provides a unified framework for implementing checks that compare column values
    against thresholds, ranges, or other criteria. The design supports multiple
    column validation with OR-based failure logic, where any failing column marks
    the entire record as invalid.

    The class handles optional type casting to ensure proper comparison semantics
    across different data types, and provides a consistent interface for all
    comparison-based validation scenarios.

    Attributes:
        columns (List[str]): Column names to which the comparison logic will be applied.
        cast_type (Optional[str]): Optional Spark SQL type specification for column
            casting before comparison operations.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        severity: Severity = Severity.CRITICAL,
        cast_type: Optional[str] = None,
    ):
        """
        Initialize the comparison-based validation check with target columns and configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Column names that will be subject to comparison validation.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
            cast_type (Optional[str], optional): Spark SQL type for column casting before
                comparison operations, enabling proper type semantics.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.columns = columns
        self.cast_type = cast_type

    @abstractmethod
    def comparison_condition(self, column: Column) -> Column:
        """
        Define the comparison logic that identifies invalid values.

        Subclasses must implement this method to specify their particular comparison
        criteria. The returned expression should evaluate to True for values that
        fail the validation check.

        Args:
            column (Column): Spark column expression, potentially type-cast according
                to the check configuration.

        Returns:
            Column: Boolean expression where True indicates validation failure.
        """
        ...

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the comparison validation logic against the configured columns.

        Performs schema validation to ensure all target columns exist, then applies
        the comparison logic with OR-based failure semantics. Records fail validation
        when any configured column violates the comparison criteria.

        Args:
            df (DataFrame): The dataset to validate against comparison criteria.

        Returns:
            DataFrame: Original dataset augmented with a boolean result column where
                True indicates validation failure and False indicates compliance.

        Raises:
            MissingColumnError: When any configured column is not present in the
                dataset schema, indicating a configuration mismatch.
        """
        for column in self.columns:
            if column not in df.columns:
                raise MissingColumnError(column, df.columns)

        # For each specified column, cast the column if needed and apply the comparison condition.
        column_exprs = F.array(*[self.comparison_condition(self._apply_cast(F.col(c))) for c in self.columns])

        # Reduce (aggregate) the array of boolean expressions into a single boolean expression
        result_expr = F.aggregate(column_exprs, F.lit(False), lambda acc, x: acc | x)

        return self.with_check_result_column(df, result_expr)

    def _apply_cast(self, column: Column) -> Column:
        """
        Apply optional type casting to the column expression.

        Performs type casting when a cast_type is configured, ensuring proper
        type semantics for comparison operations. This is particularly important
        for date, timestamp, and numeric comparisons where type consistency
        is critical for accurate validation results.

        Args:
            column (Column): The original Spark column expression.

        Returns:
            Column: The column expression with optional type casting applied.
        """
        return column.cast(self.cast_type) if self.cast_type else column


class BaseMinCheck(BaseComparisonCheck):
    """
    Abstract foundation for minimum threshold validation checks.

    Provides specialized comparison logic for validating that column values meet
    or exceed minimum threshold requirements. The check supports both inclusive
    and exclusive boundary semantics, enabling precise control over validation
    criteria for different business requirements.

    Records fail validation when any configured column falls below the specified
    minimum threshold according to the configured inclusivity rules.

    Attributes:
        min_value (SupportsComparison): The minimum acceptable value for comparison operations.
        inclusive (bool): Whether the minimum threshold is inclusive (>=) or exclusive (>).
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
        Initialize the minimum threshold validation check with boundary configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Column names that must meet minimum threshold requirements.
            min_value (SupportsComparison): The minimum acceptable value for validation.
            inclusive (bool, optional): Whether the minimum threshold includes the boundary
                value itself. Defaults to False for exclusive comparison.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
            cast_type (str | None, optional): Optional Spark SQL type for column casting
                before comparison operations.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type=cast_type)
        self.min_value = min_value
        self.inclusive = inclusive

    def comparison_condition(self, column: Column) -> Column:
        """
        Generate the comparison expression for minimum threshold validation.

        Creates the appropriate comparison logic based on the configured inclusivity
        setting, ensuring values are properly validated against the minimum threshold.

        Args:
            column (Column): The Spark column expression to evaluate against the threshold.

        Returns:
            Column: Boolean expression that evaluates to True when values fail to
                meet the minimum threshold requirements.
        """
        if self.inclusive:
            return column < F.lit(self.min_value)
        else:
            return column <= F.lit(self.min_value)


class BaseMaxCheck(BaseComparisonCheck):
    """
    Abstract foundation for maximum threshold validation checks.

    Provides specialized comparison logic for validating that column values remain
    within or below maximum threshold requirements. The check supports both inclusive
    and exclusive boundary semantics, enabling precise control over validation
    criteria for different business requirements.

    Records fail validation when any configured column exceeds the specified
    maximum threshold according to the configured inclusivity rules.

    Attributes:
        max_value (SupportsComparison): The maximum acceptable value for comparison operations.
        inclusive (bool): Whether the maximum threshold is inclusive (<=) or exclusive (<).
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
        Initialize the maximum threshold validation check with boundary configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Column names that must remain within maximum threshold limits.
            max_value (SupportsComparison): The maximum acceptable value for validation.
            inclusive (bool, optional): Whether the maximum threshold includes the boundary
                value itself. Defaults to False for exclusive comparison.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
            cast_type (str | None, optional): Optional Spark SQL type for column casting
                before comparison operations.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type=cast_type)
        self.max_value = max_value
        self.inclusive = inclusive

    def comparison_condition(self, column: Column) -> Column:
        """
        Generate the comparison expression for maximum threshold validation.

        Creates the appropriate comparison logic based on the configured inclusivity
        setting, ensuring values are properly validated against the maximum threshold.

        Args:
            column (Column): The Spark column expression to evaluate against the threshold.

        Returns:
            Column: Boolean expression that evaluates to True when values exceed
                the maximum threshold requirements.
        """
        if self.inclusive:
            return column > F.lit(self.max_value)
        else:
            return column >= F.lit(self.max_value)


class BaseBetweenCheck(BaseComparisonCheck):
    """
    Abstract foundation for range-based validation checks.

    Provides specialized comparison logic for validating that column values fall
    within specified ranges. The check supports independent inclusivity control
    for both minimum and maximum boundaries, enabling precise validation criteria
    for complex business requirements.

    Records fail validation when any configured column falls outside the specified
    range according to the configured inclusivity rules for both boundaries.

    Attributes:
        min_value (SupportsComparison): The minimum acceptable value for the valid range.
        max_value (SupportsComparison): The maximum acceptable value for the valid range.
        inclusive (Tuple[bool, bool]): Inclusivity settings for minimum and maximum
            boundaries respectively.
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
        Initialize the range validation check with boundary configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Column names that must fall within the specified range.
            min_value (SupportsComparison): The minimum acceptable value for the valid range.
            max_value (SupportsComparison): The maximum acceptable value for the valid range.
            inclusive (Tuple[bool, bool], optional): Inclusivity settings for minimum and
                maximum boundaries respectively. Defaults to (False, False) for exclusive bounds.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
            cast_type (str | None, optional): Optional Spark SQL type for column casting
                before comparison operations.
        """
        super().__init__(check_id=check_id, columns=columns, severity=severity, cast_type=cast_type)
        self.min_value = min_value
        self.max_value = max_value
        self.inclusive = inclusive

    def comparison_condition(self, column: Column) -> Column:
        """
        Generate the comparison expression for range validation.

        Creates the appropriate comparison logic based on the configured inclusivity
        settings for both boundaries, ensuring values are properly validated against
        the specified range requirements.

        Args:
            column (Column): The Spark column expression to evaluate against the range.

        Returns:
            Column: Boolean expression that evaluates to True when values fall
                outside the acceptable range boundaries.
        """
        min_cmp = column < F.lit(self.min_value) if self.inclusive[0] else column <= F.lit(self.min_value)
        max_cmp = column > F.lit(self.max_value) if self.inclusive[1] else column >= F.lit(self.max_value)
        return min_cmp | max_cmp
