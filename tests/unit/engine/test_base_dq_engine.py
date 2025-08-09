"""
Unit tests for BaseDQEngine, the abstract foundation for data quality validation engines.

This test suite validates the core engine functionality including:
- Default behavior for check retrieval when no CheckSet is configured
- Proper delegation to CheckSet instances for check management
- Consistent behavior patterns that must be maintained across all engine implementations

BaseDQEngine provides the shared architectural foundation for both batch and streaming
validation engines, ensuring consistent check management behavior across execution paradigms.
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
    Verify that get_checks returns an empty list when no CheckSet is configured.

    The engine should provide predictable default behavior when operating
    without a configured check collection, enabling safe initialization patterns.
    """
    # Arrange
    engine = DummyEngine()

    # Act
    checks = engine.get_checks()

    # Assert
    assert checks == []


def test_get_checks_with_check_manager() -> None:
    """
    Verify that get_checks correctly delegates to the configured CheckSet instance.

    The engine should properly integrate with CheckSet instances, providing
    access to all registered checks through the standard retrieval interface.
    """
    # Arrange
    engine = DummyEngine(DummyCheckSet())

    # Act
    checks = engine.get_checks()

    # Assert
    assert len(checks) == 2
    assert all(isinstance(c, DummyCheck) for c in checks)
