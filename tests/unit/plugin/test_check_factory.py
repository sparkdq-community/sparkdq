"""
Unit tests for CheckFactory, which builds check instances from raw config dictionaries.

These tests verify:
- Correct creation of check objects using registered config classes
- Proper normalization of severity fields
- Robust error handling when required fields are missing
- Support for batch creation via from_list()

CheckFactory is key for YAML/JSON-driven pipelines and must behave predictably.
"""

from typing import ClassVar, Type

import pytest

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingCheckTypeError
from sparkdq.plugin.check_config_registry import CheckConfigRegistry
from sparkdq.plugin.check_factory import CheckFactory


class DummyRowCheck(BaseRowCheck):
    def __init__(self, check_id: str, column: str, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column

    def validate(self, df): ...


class DummyRowCheckConfig(BaseRowCheckConfig):
    check_class: ClassVar[Type[BaseRowCheck]] = DummyRowCheck
    column: str


def setup_module():
    CheckConfigRegistry._registry.clear()
    CheckConfigRegistry.register("dummy-check", DummyRowCheckConfig)


def test_from_dict_creates_check_instance() -> None:
    """
    Validates that from_dict() returns a valid check instance
    when provided with a properly structured config dictionary.
    """
    # Arrange
    config_data = {"check-id": "test", "check": "dummy-check", "column": "foo", "severity": "warning"}

    # Act
    check = CheckFactory._from_dict(config_data)

    # Assert
    assert isinstance(check, DummyRowCheck)
    assert check.column == "foo"
    assert check.severity == Severity.WARNING


def test_from_dict_raises_on_missing_check_field() -> None:
    """
    Validates that from_dict() raises MissingCheckTypeError
    when the 'check' field is missing from the dictionary.
    """
    with pytest.raises(MissingCheckTypeError):
        CheckFactory._from_dict({"column": "foo"})


def test_from_list_creates_multiple_checks() -> None:
    """
    Validates that from_list() creates multiple check instances
    from a list of configuration dictionaries.
    """
    configs = [
        {"check-id": "test1", "check": "dummy-check", "column": "a", "severity": "critical"},
        {"check-id": "test2", "check": "dummy-check", "column": "b", "severity": "warning"},
    ]

    checks = CheckFactory.from_list(configs)

    assert len(checks) == 2
    assert all(isinstance(c, DummyRowCheck) for c in checks)
    assert [c.column for c in checks] == ["a", "b"]
    assert [c.severity for c in checks] == [Severity.CRITICAL, Severity.WARNING]
