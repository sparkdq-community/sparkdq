"""
Defines abstract base classes for implementing data quality checks
within the sparkdq framework.

This module provides:

- `BaseCheck`: The common interface and metadata structure for all checks.
- `BaseRowCheck`: Specialization for row-level checks applied per record.
- `BaseAggregateCheck`: Specialization for aggregate-level checks that evaluate entire datasets.

All subclasses define the contract for validation logic and standardized reporting.
"""

import inspect
from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import Column, DataFrame

from .check_results import AggregateCheckResult, AggregateEvaluationResult
from .severity import Severity


class BaseCheck(ABC):
    """
    Abstract base class for data quality checks.

    Provides shared metadata, parameter introspection, and reporting logic
    for both row-level and aggregate-level checks.

    Attributes:
        check_id (str): Unique identifier for the check instance.
        severity (Severity): Severity level of the check result (e.g., CRITICAL, WARNING, INFO).
    """

    def __init__(self, check_id: str, severity: Severity = Severity.CRITICAL):
        """
        Initializes the base check with a check ID and severity level.

        Args:
            check_id (str): Unique identifier for the check instance.
            severity (Severity, optional): Severity level of the check.
                Defaults to Severity.CRITICAL. Determines how failed checks are categorized.
        """
        self.severity = severity
        self.check_id = check_id

    @property
    def name(self) -> str:
        """
        Returns the name of the check class.

        Returns:
            str: Check class name.
        """
        return self.__class__.__name__

    def _parameters(self) -> dict[str, Any]:
        """
        Extracts check parameters from constructor arguments and instance attributes.

        Used for consistent reporting, logging, and debugging.

        Returns:
            dict[str, Any]: Mapping of parameter names to their current values.
        """
        ignore_parameters = ["self", "check_id"]
        init_signature = inspect.signature(type(self).__init__)
        init_params = [p for p in init_signature.parameters if p not in ignore_parameters]

        return {
            p: getattr(self, p, None)
            for p in init_params
            if hasattr(self, p) and p not in {"columns", "severity"}
        }

    def description(self) -> dict[str, Any]:
        """
        Returns a standardized description of the check.

        Returns:
            dict[str, Any]: Check metadata including name, ID, severity, and parameters.
        """
        return {
            "check": self.name,
            "check-id": self.check_id,
            "severity": self.severity.value,
            "parameters": self._parameters(),
        }

    def __str__(self) -> str:
        return f"{self.name}(severity={self.severity.value})"


class BaseRowCheck(BaseCheck):
    """
    Abstract base class for row-level data quality checks.

    Row-level checks operate on individual records and identify invalid rows directly
    within the dataset. Subclasses must implement the `validate()` method.
    """

    @abstractmethod
    def validate(self, df: DataFrame) -> DataFrame:
        """
        Applies the check to the input DataFrame.

        Args:
            df (DataFrame): Input dataset to validate.

        Returns:
            DataFrame: DataFrame with additional markers or columns indicating validation failures.
        """
        ...

    def with_check_result_column(self, df: DataFrame, condition: Column) -> DataFrame:
        """
        Adds the check result as a new column to the DataFrame.

        The result column will be named according to the check ID and contain the boolean evaluation
        for each row.

        Args:
            df (DataFrame): The input Spark DataFrame.
            condition (Column): The boolean Spark expression representing pass/fail per row.

        Returns:
            DataFrame: The original DataFrame extended with the check result column.
        """
        return df.withColumn(self.check_id, condition)


class BaseAggregateCheck(BaseCheck):
    """
    Abstract base class for aggregate-level data quality checks.

    Aggregate checks evaluate entire datasets and typically calculate metrics,
    apply thresholds, or return dataset-level validation results.

    Subclasses must implement the `_evaluate_logic()` method.
    """

    @abstractmethod
    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Defines the core logic for evaluating the check.

        Args:
            df (DataFrame): The input dataset to analyze.

        Returns:
            AggregateEvaluationResult: The result of the check evaluation.
        """
        ...

    def evaluate(self, df: DataFrame) -> AggregateCheckResult:
        """
        Evaluates the check and returns a structured result with metadata.

        This method serves as the standard entry point for executing aggregate-level checks.
        It delegates to `_evaluate_logic()` and wraps the outcome in an `AggregateCheckResult`.

        Args:
            df (DataFrame): The dataset to validate.

        Returns:
            AggregateCheckResult: The complete check result including metadata and evaluation outcome.
        """
        result = self._evaluate_logic(df)
        return AggregateCheckResult(
            check=self.name,
            check_id=self.check_id,
            severity=self.severity,
            parameters=self._parameters(),
            result=result,
        )
