import pytest

from sparkdq.core.severity import Severity, normalize_severity
from sparkdq.exceptions import InvalidSeverityLevelError


def test_normalize_severity_from_valid_string() -> None:
    """
    Validates that string representations of severity levels are correctly converted to enum values.

    Given valid string inputs (case-insensitive) like "critical" or "WARNING",
    the function should normalize them to the corresponding Severity enum.

    This ensures consistent severity handling in config-driven workflows.
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
    Validates that an unknown severity string raises an InvalidSeverityLevelError.

    Given an invalid string such as "fatal",
    the function should raise an InvalidSeverityLevelError with a helpful message.

    This test ensures that incorrect user input is caught early and explicitly.
    """
    # Arrange: Define an invalid input string
    invalid_input = "fatal"

    # Act & Assert: Expect the custom exception to be raised
    with pytest.raises(InvalidSeverityLevelError):
        normalize_severity(invalid_input)
