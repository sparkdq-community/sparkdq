"""
Abstract base classes for implementing data quality checks within the sparkdq framework.

This module establishes the foundational architecture for all data quality validations,
providing standardized interfaces for both row-level and aggregate-level checks. The
design ensures consistent behavior, metadata handling, and result reporting across
all check implementations while maintaining extensibility for custom validation logic.

The module defines three primary abstractions:
- BaseCheck: Core interface and metadata management for all validation checks
- BaseRowCheck: Specialized interface for record-level validation operations
- BaseAggregateCheck: Specialized interface for dataset-wide validation operations

Additionally, the IntegrityCheckMixin enables cross-dataset validation scenarios
by providing reference dataset injection and retrieval capabilities.
"""

import inspect
from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import Column, DataFrame

from sparkdq.exceptions import MissingReferenceDatasetError

from .check_results import AggregateCheckResult, AggregateEvaluationResult
from .severity import Severity


class BaseCheck(ABC):
    """
    Abstract foundation for all data quality validation checks.

    Establishes the core contract and shared functionality for data quality validations,
    including metadata management, parameter introspection, and standardized reporting.
    This class serves as the common ancestor for both row-level and aggregate-level
    checks, ensuring consistent behavior and interface across all validation types.

    The class automatically extracts constructor parameters for reporting purposes
    and provides standardized description formatting for debugging and audit trails.

    Attributes:
        check_id (str): Unique identifier for this check instance within a validation context.
        severity (Severity): Classification of check importance determining failure handling behavior.
    """

    def __init__(self, check_id: str, severity: Severity = Severity.CRITICAL):
        """
        Initialize the base check with essential metadata and configuration.

        Args:
            check_id (str): Unique identifier for this check instance. Must be unique within
                the validation context to ensure proper result tracking and reporting.
            severity (Severity, optional): Classification level determining how check failures
                are handled. Defaults to Severity.CRITICAL for blocking behavior.
        """
        self.severity = severity
        self.check_id = check_id

    @property
    def name(self) -> str:
        """
        Retrieve the canonical name of this check implementation.

        Returns:
            str: The class name of this check, used for identification and reporting.
        """
        return self.__class__.__name__

    def _parameters(self) -> dict[str, Any]:
        """
        Extract configuration parameters from the check instance for reporting purposes.

        Performs introspection on the constructor signature to identify and collect
        all configuration parameters, excluding internal framework attributes. This
        enables consistent parameter reporting across all check implementations
        without requiring manual maintenance of parameter lists.

        Returns:
            dict[str, Any]: Configuration parameters with their current values, suitable
                for serialization and audit logging.
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
        Generate a comprehensive description of this check instance.

        Produces a standardized metadata structure containing all essential information
        about the check configuration, suitable for serialization, logging, and
        audit trail generation.

        Returns:
            dict[str, Any]: Complete check metadata including implementation details,
                configuration parameters, and execution context.
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
    Abstract foundation for record-level data quality validation checks.

    Defines the interface for checks that evaluate individual records within a dataset,
    enabling fine-grained validation at the row level. These checks typically append
    boolean result columns to the input DataFrame, marking each record as valid or
    invalid based on the implemented validation logic.

    Row-level checks are particularly effective for identifying specific problematic
    records, enabling downstream filtering, correction, or detailed error reporting
    on a per-record basis.
    """

    @abstractmethod
    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the validation logic against the provided dataset.

        Implementations must apply their specific validation rules to the input DataFrame
        and return an augmented version containing validation results. The returned
        DataFrame should preserve all original data while adding validation markers.

        Args:
            df (DataFrame): The dataset to validate against this check's criteria.

        Returns:
            DataFrame: Enhanced dataset containing original data plus validation results,
                typically as additional boolean columns indicating pass/fail status per record.
        """
        ...

    def with_check_result_column(self, df: DataFrame, condition: Column) -> DataFrame:
        """
        Augment the DataFrame with a validation result column.

        Appends a boolean column containing the validation outcome for each record,
        using the check's unique identifier as the column name. This standardized
        approach ensures consistent result formatting across all row-level checks.

        Args:
            df (DataFrame): The dataset to augment with validation results.
            condition (Column): Boolean Spark expression evaluating to True for failed
                records and False for valid records.

        Returns:
            DataFrame: Original dataset enhanced with a named boolean column indicating
                validation status for each record.
        """
        return df.withColumn(self.check_id, condition)


class BaseAggregateCheck(BaseCheck):
    """
    Abstract foundation for dataset-level data quality validation checks.

    Defines the interface for checks that evaluate global properties of entire datasets,
    such as row counts, statistical distributions, or cross-dataset relationships.
    These checks produce single validation outcomes per dataset along with detailed
    metrics explaining the validation results.

    Aggregate checks are essential for validating dataset-wide constraints and
    business rules that cannot be evaluated at the individual record level.
    """

    @abstractmethod
    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Implement the core validation logic for this aggregate check.

        Contains the specific business logic and computational operations required
        to evaluate the dataset against this check's criteria. Implementations
        should perform necessary calculations and return both the validation
        outcome and supporting metrics.

        Args:
            df (DataFrame): The dataset to evaluate against this check's criteria.

        Returns:
            AggregateEvaluationResult: Validation outcome including pass/fail status
                and detailed metrics explaining the evaluation results.
        """
        ...

    def evaluate(self, df: DataFrame) -> AggregateCheckResult:
        """
        Execute the check and produce a comprehensive result with full metadata.

        Serves as the primary interface for aggregate check execution, orchestrating
        the validation logic and enriching the results with complete metadata for
        reporting and audit purposes. This method ensures consistent result formatting
        across all aggregate check implementations.

        Args:
            df (DataFrame): The dataset to validate against this check's criteria.

        Returns:
            AggregateCheckResult: Complete validation result including check metadata,
                configuration parameters, and detailed evaluation outcomes.
        """
        result = self._evaluate_logic(df)
        return AggregateCheckResult(
            check=self.name,
            check_id=self.check_id,
            severity=self.severity,
            parameters=self._parameters(),
            result=result,
        )


# Typalias for mapping reference dataset names to Spark DataFrames
ReferenceDatasetDict = dict[str, DataFrame]


class IntegrityCheckMixin:
    """
    Mixin enabling cross-dataset integrity validation capabilities.

    Provides infrastructure for checks that require validation against external
    reference datasets, such as foreign key constraints, referential integrity,
    or cross-dataset consistency validations. The mixin handles reference dataset
    lifecycle management, including injection, storage, and retrieval operations.

    This design enables complex validation scenarios while maintaining clean
    separation between the validation logic and reference data management,
    supporting both simple lookups and complex multi-dataset relationships.

    Attributes:
        _reference_datasets (ReferenceDatasetDict): Internal registry mapping
            reference dataset names to their corresponding Spark DataFrames.
    """

    _reference_datasets: ReferenceDatasetDict = {}

    def inject_reference_datasets(self, datasets: ReferenceDatasetDict) -> None:
        """
        Register reference datasets for use during validation execution.

        Establishes the reference dataset registry that will be available during
        check execution. This method is typically invoked by the validation engine
        as part of the check preparation phase, ensuring all required reference
        data is accessible when validation logic executes.

        Args:
            datasets (ReferenceDatasetDict): Registry mapping reference dataset
                identifiers to their corresponding Spark DataFrames.
        """
        self._reference_datasets = datasets

    def get_reference_df(self, name: str) -> DataFrame:
        """
        Retrieve a reference dataset by its registered identifier.

        Provides access to previously injected reference datasets during check
        execution. This method ensures type-safe access to reference data while
        providing clear error handling for missing or misconfigured references.

        Args:
            name (str): The identifier of the reference dataset to retrieve.

        Returns:
            DataFrame: The Spark DataFrame associated with the specified identifier.

        Raises:
            MissingReferenceDatasetError: When the requested reference dataset
                identifier is not found in the current registry.
        """
        try:
            return self._reference_datasets[name]
        except KeyError:
            raise MissingReferenceDatasetError(name)
