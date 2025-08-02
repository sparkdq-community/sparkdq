"""
Unit tests for CheckFactory, the dynamic check instantiation engine for configuration-driven workflows.

This test suite validates the complete factory functionality including:
- Dynamic check instantiation from raw configuration dictionaries
- Proper parameter normalization and validation during instantiation
- Robust error handling for malformed or incomplete configurations
- Batch processing capabilities for bulk check creation

CheckFactory enables declarative validation pipeline configuration by bridging
external configuration sources with concrete check implementations.
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
    Verify that from_dict correctly instantiates checks from well-formed configuration dictionaries.

    The factory should process configuration parameters, perform validation,
    and produce properly initialized check instances ready for execution.
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
    Verify that from_dict raises MissingCheckTypeError for incomplete configurations.

    The factory should validate required configuration fields and provide
    clear error messages when essential type identification is missing.
    """
    with pytest.raises(MissingCheckTypeError):
        CheckFactory._from_dict({"column": "foo"})


def test_from_list_creates_multiple_checks() -> None:
    """
    Verify that from_list correctly processes collections of configuration dictionaries.

    The factory should handle batch processing scenarios, creating multiple
    check instances while maintaining proper parameter validation for each.
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
