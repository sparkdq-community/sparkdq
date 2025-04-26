"""
Validation tests for BaseAggregateCheckConfig subclasses.

These tests ensure that the abstract base config enforces correct usage
at class definition time via `__init_subclass__()`.

They cover:
- Missing `check_class` attributes
- Incorrect base class usage for aggregate checks

Failing these rules should raise `TypeError` immediately to prevent misuse.
"""

import pytest

from sparkdq.core.base_check import BaseCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig


def test_aggregate_check_config_without_check_class_raises() -> None:
    """
    Validates that BaseAggregateCheckConfig subclasses must define `check_class`.

    If omitted, a TypeError should be raised immediately on class definition.
    """
    with pytest.raises(TypeError) as exc_info:

        class IncompleteAggregateConfig(BaseAggregateCheckConfig):
            threshold: int

    assert "must define a 'check_class'" in str(exc_info.value)


def test_aggregate_check_config_with_invalid_check_class_type_raises() -> None:
    """
    Validates that `check_class` must inherit from BaseAggregateCheck.

    A TypeError should be raised if an invalid class is assigned.
    """
    with pytest.raises(TypeError) as exc_info:

        class InvalidAggregateConfig(BaseAggregateCheckConfig):
            check_class = BaseCheck  # wrong type
            threshold: int

    assert "must be a subclass of BaseAggregateCheck" in str(exc_info.value)
