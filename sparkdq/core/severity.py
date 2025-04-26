"""
Defines the `Severity` enum for classifying the importance of data quality check results,
along with a utility function for normalizing severity values.

Severity levels help downstream logic differentiate between critical (blocking)
and warning (non-blocking) validation outcomes, enabling flexible handling
of failed data quality checks.
"""

from enum import Enum

from sparkdq.exceptions import InvalidSeverityLevelError


class Severity(Enum):
    """
    Severity levels for data quality checks.

    The severity level determines how a failed check should be treated,
    particularly in automated data pipelines (e.g., fail the pipeline or log a warning).

    Levels:
        CRITICAL: The check is considered blocking if it fails.
        WARNING: The check is non-blocking but may trigger alerts or monitoring actions.
    """

    CRITICAL = "critical"
    WARNING = "warning"

    def __str__(self) -> str:
        """
        Returns the string representation of the severity level.

        Returns:
            str: The lowercase severity level (e.g., "critical", "warning").
        """
        return self.value


def normalize_severity(value: str) -> Severity:
    """
    Converts a severity string to a `Severity` enum instance.

    This function ensures that severity values provided as strings
    (e.g., from configuration files or user input) are consistently interpreted
    within the framework.

    Args:
        value (str): Severity level as a string (case-insensitive).

    Returns:
        Severity: The corresponding `Severity` enum instance.

    Raises:
        InvalidSeverityLevelError: If the input does not match a known severity level.
    """
    try:
        return Severity[value.upper()]
    except KeyError:
        raise InvalidSeverityLevelError(value)
