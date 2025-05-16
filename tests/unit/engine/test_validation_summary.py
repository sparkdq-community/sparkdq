from datetime import datetime

from sparkdq.engine.validation_summary import ValidationSummary


def test_all_passed_property_true() -> None:
    """
    Validates that all_passed returns True when pass_rate is exactly 1.0.

    Given a ValidationSummary with pass_rate = 1.0, the all_passed property
    should return True.
    """
    # Arrange
    summary = ValidationSummary(
        total_records=100,
        passed_records=100,
        failed_records=0,
        warning_records=0,
        pass_rate=1.0,
        timestamp=datetime.now(),
    )

    # Act
    result = summary.all_passed

    # Assert
    assert result is True


def test_all_passed_property_false() -> None:
    """
    Validates that all_passed returns False when pass_rate is less than 1.0.

    Given a ValidationSummary with pass_rate = 0.95, the all_passed property
    should return False.
    """
    # Arrange
    summary = ValidationSummary(
        total_records=100,
        passed_records=95,
        failed_records=5,
        warning_records=2,
        pass_rate=0.95,
        timestamp=datetime.now(),
    )

    # Act
    result = summary.all_passed

    # Assert
    assert result is False
