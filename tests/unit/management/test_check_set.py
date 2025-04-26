"""
Unit tests for the CheckSet, which manages and organizes all checks in a validation session.

These tests verify:
- Correct registration and instantiation of row and aggregate checks
- Support for dynamic loading from configuration dictionaries
- Filtering capabilities for row and aggregate checks
- Cleanup behavior via clear()

CheckSet serves as the centralized definition layer for validation pipelines.
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
    """
    check_set = CheckSet()
    config = DummyRowCheckConfig(check_id="test", column="foo", severity=Severity.WARNING)

    check_set.add_check(config)
    checks = check_set.get_all()

    assert len(checks) == 1
    assert isinstance(checks[0], DummyRowCheck)
    assert check_set.get_row_checks() == checks
    assert check_set.get_aggregate_checks() == []


def test_add_check_adds_aggregate_check_instance() -> None:
    """
    Validates that add_check() correctly instantiates and stores an aggregate-level check.
    """
    check_set = CheckSet()
    config = DummyAggregateCheckConfig(check_id="test", threshold=10)

    check_set.add_check(config)
    checks = check_set.get_all()

    assert len(checks) == 1
    assert isinstance(checks[0], DummyAggregateCheck)
    assert check_set.get_aggregate_checks() == checks
    assert check_set.get_row_checks() == []


def test_add_checks_from_dicts_creates_multiple_checks(monkeypatch) -> None:
    """
    Validates that add_checks_from_dicts() correctly builds check instances from config dicts.
    """
    # Arrange
    check_set = CheckSet()

    dummy_check = DummyRowCheck(check_id="test", column="test", severity=Severity.CRITICAL)

    def mock_from_list(configs):
        return [dummy_check, dummy_check]

    monkeypatch.setattr("sparkdq.management.check_set.CheckFactory.from_list", mock_from_list)

    # Act
    check_set.add_checks_from_dicts([{"check": "dummy"}, {"check": "dummy"}])

    # Assert
    all_checks = check_set.get_all()
    assert len(all_checks) == 2
    assert all(isinstance(c, DummyRowCheck) for c in all_checks)


def test_clear_removes_all_checks() -> None:
    """
    Validates that clear() removes all previously registered checks.
    """
    check_set = CheckSet()
    check_set.add_check(DummyRowCheckConfig(check_id="test1", column="foo"))
    check_set.add_check(DummyAggregateCheckConfig(check_id="test2", threshold=1))

    check_set.clear()

    assert check_set.get_all() == []
    assert check_set.get_row_checks() == []
    assert check_set.get_aggregate_checks() == []
