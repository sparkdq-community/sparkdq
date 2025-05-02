"""
Defines standardized result containers for aggregate-level data quality checks
within the sparkdq framework.

Includes:

- `AggregateEvaluationResult`: Represents the outcome of a single check evaluation.
- `AggregateCheckResult`: Wraps the evaluation result together with metadata such as
  check name, severity level, and configuration parameters.

These dataclasses ensure consistency in how check results are represented,
serialized, and consumed across the framework.
"""

from dataclasses import asdict, dataclass
from typing import Any, Dict

from .severity import Severity


@dataclass(frozen=True)
class AggregateEvaluationResult:
    """
    Encapsulates the outcome of an aggregate-level data quality check.

    This class holds both the result (`passed`) and additional diagnostic `metrics`
    that were computed as part of the check evaluation. These metrics provide context
    for understanding why a check passed or failed, and are especially useful during
    debugging or reporting.

    For example, in a CountMinCheck, the metrics might include:

    - ``actual_count``: the number of rows actually found
    - ``expected_min_count``: the configured minimum count threshold

    Such context allows users to understand the degree of deviation from expectations
    when a check fails.

    Attributes:
        passed (bool): Indicates whether the check condition was satisfied.
        metrics (Dict[str, Any]): Additional computed values or diagnostic information
            relevant to the check (e.g., actual vs. expected counts, computed averages,
            standard deviations, etc.).

    Example:
        >>> result = AggregateEvaluationResult(
        ...     passed=False,
        ...     metrics={
        ...         "actual_count": 42,
        ...         "expected_min_count": 100
        ...     }
        ... )
        >>> result.passed
        False
        >>> result.metrics["actual_count"]
        42
    """

    passed: bool
    metrics: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the evaluation result to a dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of the evaluation result.
        """
        return asdict(self)


@dataclass(frozen=True)
class AggregateCheckResult:
    """
    Encapsulates the full result of an aggregate-level data quality check.

    Combines the evaluation outcome with metadata such as the check name, severity,
    configuration parameters, and the result of the check evaluation.

    Attributes:
        check (str): Name of the check (e.g., "row-count-between").
        check_id (str): Unique identifier for the check instance.
        severity (Severity): Severity level of the check result (e.g., "CRITICAL", "WARNING").
        parameters (Dict[str, Any]): Configuration parameters used for this check.
        result (AggregateEvaluationResult): The outcome of the check evaluation.
    """

    check: str
    check_id: str
    severity: Severity
    parameters: Dict[str, Any]
    result: AggregateEvaluationResult

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the complete check result to a dictionary.

        Converts enums (e.g., severity) to strings and includes the serialized evaluation result.

        Returns:
            Dict[str, Any]: Dictionary containing check metadata and evaluation outcome.
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
        Indicates whether the check passed.

        Shortcut to access the `passed` status from the embedded result.

        Returns:
            bool: True if the check passed, False otherwise.
        """
        return self.result.passed

    @property
    def metrics(self) -> Dict[str, Any]:
        """
        Provides access to the evaluation metrics.

        Shortcut to access the `metrics` from the embedded result.

        Returns:
            Dict[str, Any]: Metrics produced during the check evaluation.
        """
        return self.result.metrics
