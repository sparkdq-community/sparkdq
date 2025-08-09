"""
Unit tests for CheckSet, the centralized registry and lifecycle manager for data quality checks.

This test suite validates the complete check management functionality including:
- Check registration and instantiation from configuration objects
- Dynamic check loading from external configuration sources via CheckFactory integration
- Type-based filtering capabilities for row-level and aggregate-level checks
- Registry cleanup and reset operations
- Fluent API support for method chaining in configuration workflows

CheckSet serves as the foundational component for declarative validation pipeline
configuration, providing flexible and extensible check management capabilities.
"""

from typing import ClassVar, Type

from sparkdq.core.base_check import BaseAggregateCheck, BaseRowCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig, BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.management.check_set import CheckSet


class DummyRowCheck(BaseRowCheck):
    def __init__(self, check_id: str, column: str, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column

    def validate(self, df): ...


class DummyAggregateCheck(BaseAggregateCheck):
    def __init__(self, check_id: str, threshold: int, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.threshold = threshold

    def _evaluate_logic(self, df): ...


class DummyRowCheckConfig(BaseRowCheckConfig):
    check_class: ClassVar[Type[BaseRowCheck]] = DummyRowCheck
    column: str


class DummyAggregateCheckConfig(BaseAggregateCheckConfig):
    check_class: ClassVar[Type[BaseAggregateCheck]] = DummyAggregateCheck
    threshold: int


def test_add_check_adds_row_check_instance() -> None:
    """
    Verify that add_check correctly instantiates and registers row-level checks.

    The method should convert configuration objects to concrete check instances
    and maintain proper type categorization within the internal registry.
    """
    check_set = CheckSet()
    config = DummyRowCheckConfig(check_id="test", column="foo", severity=Severity.WARNING)

    result = check_set.add_check(config)

    checks = check_set.get_all()
    assert isinstance(result, CheckSet)
    assert len(checks) == 1
    assert isinstance(checks[0], DummyRowCheck)
    assert check_set.get_row_checks() == checks
    assert check_set.get_aggregate_checks() == []


def test_add_check_adds_aggregate_check_instance() -> None:
    """
    Verify that add_check correctly instantiates and registers aggregate-level checks.

    The method should convert configuration objects to concrete check instances
    and maintain proper type categorization within the internal registry.
    """
    check_set = CheckSet()
    config = DummyAggregateCheckConfig(check_id="test", threshold=10)

    result = check_set.add_check(config)

    checks = check_set.get_all()
    assert isinstance(result, CheckSet)
    assert len(checks) == 1
    assert isinstance(checks[0], DummyAggregateCheck)
    assert check_set.get_aggregate_checks() == checks
    assert check_set.get_row_checks() == []


def test_add_checks_from_dicts_creates_multiple_checks(monkeypatch) -> None:
    """
    Verify that add_checks_from_dicts correctly processes external configuration sources.

    The method should integrate with CheckFactory to convert raw configuration
    dictionaries into concrete check instances for bulk registration.
    """
    check_set = CheckSet()
    dummy_check = DummyRowCheck(check_id="test", column="test", severity=Severity.CRITICAL)

    def mock_from_list(configs):
        return [dummy_check, dummy_check]

    monkeypatch.setattr("sparkdq.management.check_set.CheckFactory.from_list", mock_from_list)

    check_set.add_checks_from_dicts([{"check": "dummy"}, {"check": "dummy"}])

    all_checks = check_set.get_all()
    assert len(all_checks) == 2
    assert all(isinstance(c, DummyRowCheck) for c in all_checks)


def test_clear_removes_all_checks() -> None:
    """
    Verify that clear correctly resets the CheckSet to its initial empty state.

    The operation should remove all registered checks across all categories,
    enabling clean reinitialization for subsequent validation configurations.
    """
    check_set = CheckSet()
    check_set.add_check(DummyRowCheckConfig(check_id="test1", column="foo"))
    check_set.add_check(DummyAggregateCheckConfig(check_id="test2", threshold=1))

    check_set.clear()

    assert check_set.get_all() == []
    assert check_set.get_row_checks() == []
    assert check_set.get_aggregate_checks() == []


def test_method_chaining_with_add_check() -> None:
    """
    Verify that add_check supports fluent API patterns through method chaining.

    The method should return the CheckSet instance to enable chained configuration
    calls, supporting concise and readable validation pipeline definitions.
    """
    check_set = (
        CheckSet()
        .add_check(DummyRowCheckConfig(check_id="row1", column="a"))
        .add_check(DummyAggregateCheckConfig(check_id="agg1", threshold=100))
    )

    all_checks = check_set.get_all()
    assert len(all_checks) == 2
    assert any(isinstance(c, DummyRowCheck) for c in all_checks)
    assert any(isinstance(c, DummyAggregateCheck) for c in all_checks)
