"""
Tests for the validation logic built into BaseRowCheckConfig via `__init_subclass__`.

This module ensures that incorrect or incomplete subclass definitions
fail early with helpful error messages.

Why this is important:

- The framework relies on config classes to define a valid `check_class`.
- Each `check_class` must be of the correct type (e.g., BaseRowCheck or BaseAggregateCheck).
- These constraints are enforced via `__init_subclass__` at definition time, not runtime.
- Failing to validate this would cause subtle bugs when using `.to_check()` or auto-loading config.

The goal is to fail **loudly and early** when the config layer is misused or extended incorrectly.
"""

import pytest

from sparkdq.core.base_check import BaseCheck
from sparkdq.core.base_config import BaseRowCheckConfig


def test_check_config_without_check_class_raises() -> None:
    """
    Validates that a subclass of BaseRowCheckConfig raises a TypeError
    when it does not define a `check_class` attribute.

    This ensures that all configs explicitly link to a concrete check implementation
    and fail early if this contract is violated.
    """
    # Act & Assert: Defining an invalid config class should raise an error
    with pytest.raises(TypeError) as exc_info:

        class IncompleteConfig(BaseRowCheckConfig):
            column: str  # no check_class defined

    assert "must define a 'check_class'" in str(exc_info.value)


def test_check_config_with_invalid_check_class_type_raises() -> None:
    """
    Validates that a subclass of BaseRowCheckConfig raises a TypeError
    if its `check_class` does not inherit from BaseRowCheck.

    This protects against configuration mismatches at class definition time.
    """
    # Act & Assert: Defining a config class with the wrong check_class type should raise an error
    with pytest.raises(TypeError) as exc_info:

        class InvalidCheckConfig(BaseRowCheckConfig):
            check_class = BaseCheck  # wrong base class
            column: str

    assert "must be a subclass of BaseRowCheck" in str(exc_info.value)
