"""
Severity classification system for data quality check results.

This module provides a standardized severity classification framework that enables
sophisticated failure handling strategies in data quality validation pipelines.
The severity system allows teams to distinguish between blocking failures that
should halt processing and informational warnings that require attention but
permit continued execution.

The design supports flexible pipeline behavior where different severity levels
can trigger different response strategies, from immediate pipeline termination
to alert generation and monitoring notifications.
"""

from enum import Enum

from sparkdq.exceptions import InvalidSeverityLevelError


class Severity(Enum):
    """
    Enumeration of data quality check severity classifications.

    Provides standardized severity levels that determine how validation failures
    are handled within data processing pipelines. The severity classification
    enables sophisticated failure handling strategies, allowing teams to implement
    nuanced responses to different types of data quality issues.

    The design supports both blocking and non-blocking failure modes, enabling
    flexible pipeline behavior that can adapt to different operational requirements
    and business contexts.

    Levels:
        CRITICAL: Validation failures that should block pipeline execution and
            require immediate attention before processing can continue.
        WARNING: Validation failures that indicate potential issues but should
            not prevent pipeline execution, typically triggering monitoring
            alerts or logging for later investigation.
    """

    CRITICAL = "critical"
    WARNING = "warning"

    def __str__(self) -> str:
        """
        Provide the canonical string representation of this severity level.

        Returns:
            str: Lowercase severity identifier suitable for serialization,
                logging, and external system integration.
        """
        return self.value


def normalize_severity(value: str) -> Severity:
    """
    Convert string representations to validated Severity enum instances.

    Provides robust parsing of severity values from external sources such as
    configuration files, user input, or API requests. The function performs
    case-insensitive matching and provides clear error handling for invalid
    severity specifications.

    This normalization ensures consistent severity handling across all framework
    components while providing clear feedback for configuration errors.

    Args:
        value (str): String representation of the severity level, processed
            case-insensitively for flexible input handling.

    Returns:
        Severity: Validated severity enum instance corresponding to the input string.

    Raises:
        InvalidSeverityLevelError: When the input string does not match any
            recognized severity level, providing clear feedback for correction.
    """
    try:
        return Severity[value.upper()]
    except KeyError:
        raise InvalidSeverityLevelError(value)
