"""
Standardized result containers for aggregate-level data quality check outcomes.

This module provides immutable, serializable data structures for representing
the results of aggregate-level data quality validations. The design ensures
consistent result formatting across all check implementations while providing
rich metadata for debugging, reporting, and audit trail generation.

The result containers separate the core evaluation outcome from the surrounding
metadata, enabling flexible consumption patterns while maintaining complete
traceability of validation results and their context.
"""

from dataclasses import asdict, dataclass
from typing import Any, Dict

from .severity import Severity


@dataclass(frozen=True)
class AggregateEvaluationResult:
    """
    Immutable container for aggregate-level data quality check evaluation outcomes.

    Encapsulates both the binary validation result and the supporting metrics that
    explain the evaluation process. The metrics provide essential context for
    understanding validation outcomes, enabling detailed debugging and comprehensive
    reporting of data quality assessment results.

    The separation of validation outcome from supporting metrics enables flexible
    consumption patterns while ensuring complete traceability of the evaluation
    process and its underlying calculations.

    Attributes:
        passed (bool): Binary indicator of whether the validation criteria were satisfied.
        metrics (Dict[str, Any]): Comprehensive diagnostic information including
            computed values, thresholds, and contextual data that explain the
            validation outcome.
    """

    passed: bool
    metrics: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the evaluation result to a serializable dictionary format.

        Produces a clean dictionary representation suitable for JSON serialization,
        logging, or integration with external reporting systems.

        Returns:
            Dict[str, Any]: Complete evaluation result in dictionary format,
                preserving all validation outcomes and diagnostic metrics.
        """
        return asdict(self)


@dataclass(frozen=True)
class AggregateCheckResult:
    """
    Comprehensive container for complete aggregate-level data quality check results.

    Combines the core evaluation outcome with complete metadata including check
    identification, configuration parameters, and execution context. This design
    provides full traceability and context for each validation result, supporting
    detailed audit trails and comprehensive reporting requirements.

    The immutable structure ensures result integrity while providing convenient
    access to both high-level outcomes and detailed diagnostic information.

    Attributes:
        check (str): Canonical name of the check implementation.
        check_id (str): Unique identifier for this specific check instance.
        severity (Severity): Classification level determining failure handling behavior.
        parameters (Dict[str, Any]): Complete configuration parameters used during execution.
        result (AggregateEvaluationResult): Detailed evaluation outcome with supporting metrics.
    """

    check: str
    check_id: str
    severity: Severity
    parameters: Dict[str, Any]
    result: AggregateEvaluationResult

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the complete check result to a serializable dictionary format.

        Produces a comprehensive dictionary representation with proper serialization
        of complex types, suitable for JSON export, logging, or integration with
        external monitoring and reporting systems.

        Returns:
            Dict[str, Any]: Complete check result including metadata and evaluation
                outcome, with all complex types properly serialized.
        """
        return {
            "check": self.check,
            "check-id": self.check_id,
            "severity": str(self.severity),
            "parameters": self.parameters,
            "result": self.result.to_dict(),
        }

    @property
    def passed(self) -> bool:
        """
        Retrieve the binary validation outcome for this check.

        Provides convenient access to the core validation result without requiring
        navigation through the nested result structure.

        Returns:
            bool: True if the validation criteria were satisfied, False otherwise.
        """
        return self.result.passed

    @property
    def metrics(self) -> Dict[str, Any]:
        """
        Retrieve the diagnostic metrics from the evaluation process.

        Provides convenient access to the detailed metrics that explain the
        validation outcome without requiring navigation through the nested
        result structure.

        Returns:
            Dict[str, Any]: Comprehensive diagnostic metrics including computed
                values, thresholds, and contextual information.
        """
        return self.result.metrics
