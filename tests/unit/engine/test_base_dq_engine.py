"""
Unit tests for BaseDQEngine functionality.

These tests ensure that:
- `get_checks()` returns an empty list by default
- A CheckManager (CheckSet) can be assigned using `set_check_manager()`
- Once assigned, the engine delegates check retrieval to the manager

BaseDQEngine serves as the shared logic foundation for batch and streaming engines,
so these behaviors must be consistent across all derived implementations.
"""

from typing import List

from sparkdq.core.base_check import BaseCheck
from sparkdq.core.severity import Severity
from sparkdq.engine.base_engine import BaseDQEngine
from sparkdq.management.check_set import CheckSet


class DummyCheck(BaseCheck):
    def __init__(self):
        super().__init__(check_id="test", severity=Severity.CRITICAL)


class DummyEngine(BaseDQEngine):
    """Concrete dummy implementation for testing abstract BaseDQEngine."""

    pass


class DummyCheckSet(CheckSet):
    """Minimal CheckSet with static checks for testing."""

    def get_all(self) -> List[BaseCheck]:
        return [DummyCheck(), DummyCheck()]


def test_get_checks_without_check_manager() -> None:
    """
    Validates that get_checks() returns an empty list
    if no CheckManager has been assigned.
    """
    # Arrange
    engine = DummyEngine()

    # Act
    checks = engine.get_checks()

    # Assert
    assert checks == []


def test_get_checks_with_check_manager() -> None:
    """
    Validates that get_checks() returns the checks provided by the assigned CheckManager.

    This confirms that set_check_manager() overrides the internal check list.
    """
    # Arrange
    engine = DummyEngine(DummyCheckSet())

    # Act
    checks = engine.get_checks()

    # Assert
    assert len(checks) == 2
    assert all(isinstance(c, DummyCheck) for c in checks)
