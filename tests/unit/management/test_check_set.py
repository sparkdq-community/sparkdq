"""
Unit tests for the CheckSet, which manages and organizes all checks in a validation session.

These tests verify:
- Correct registration and instantiation of row and aggregate checks via configuration classes
- Support for dynamic loading from configuration dictionaries via CheckFactory
- Filtering capabilities to separate row-level and aggregate-level checks
- Cleanup behavior using clear()
- Support for method chaining through the fluent API of add_check() and add_checks_from_dicts()

CheckSet serves as the centralized definition layer for validation pipelines and is
designed to be flexible, extendable, and chainable for fluent configuration.
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
    Validates that add_check() correctly instantiates and stores a row-level check.

    Given a DummyRowCheckConfig, the resulting CheckSet should contain a single DummyRowCheck.
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
    Validates that add_check() correctly instantiates and stores an aggregate-level check.

    Given a DummyAggregateCheckConfig, the resulting CheckSet should contain a single DummyAggregateCheck.
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
    Validates that add_checks_from_dicts() correctly builds check instances from config dicts.

    Given mocked CheckFactory output, the CheckSet should contain all returned checks.
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
    Validates that clear() removes all previously registered checks.

    After calling clear(), the CheckSet should be empty across all categories.
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
    Validates that add_check() supports method chaining via the fluent API.

    Given two configurations, the chained call should return a CheckSet containing both checks.
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
