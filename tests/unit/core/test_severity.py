import pytest

from sparkdq.core.severity import Severity, normalize_severity
from sparkdq.exceptions import InvalidSeverityLevelError


def test_normalize_severity_from_valid_string() -> None:
    """
    Verify that normalize_severity correctly converts valid string inputs to Severity enums.

    The normalization should handle case-insensitive string inputs and produce
    the appropriate enum values, ensuring consistent severity handling across
    configuration-driven validation workflows.
    """
    # Arrange: Define input values and their expected normalized enum
    inputs = {
        "critical": Severity.CRITICAL,
        "CRITICAL": Severity.CRITICAL,
        "warning": Severity.WARNING,
        "WARNING": Severity.WARNING,
    }

    # Act & Assert: Normalize each string and verify the correct enum is returned
    for input_value, expected in inputs.items():
        result = normalize_severity(input_value)
        assert result == expected


def test_normalize_severity_invalid_string() -> None:
    """
    Verify that normalize_severity raises InvalidSeverityLevelError for unrecognized inputs.

    The normalization should perform input validation and provide clear error
    messages when encountering invalid severity specifications, enabling early
    detection of configuration errors.
    """
    # Arrange: Define an invalid input string
    invalid_input = "fatal"

    # Act & Assert: Expect the custom exception to be raised
    with pytest.raises(InvalidSeverityLevelError):
        normalize_severity(invalid_input)
