"""
Tests for BaseCheckConfig and its concrete subclass behavior (e.g., DummyRowCheckConfig).

This module focuses on verifying the positive behavior of the `to_check()` method,
which is responsible for instantiating a concrete check based on a configuration object.

These tests ensure:

- That a correctly defined config class instantiates the associated check class.
- That parameters like `column` and `severity` are passed properly.
- That this core mechanism works as expected and is safe to rely on in Engine pipelines.

This test operates on a minimal dummy row-level check to avoid coupling with real checks.
"""

from typing import ClassVar

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity


class DummyRowCheck(BaseRowCheck):
    def __init__(self, check_id: str, column: str, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column

    def validate(self, df): ...
    def error_column(self) -> str:
        return f"{self.column}_is_null"


class DummyRowCheckConfig(BaseRowCheckConfig):
    """
    Minimal example config for testing BaseRowCheckConfig behavior.
    """

    check_class: ClassVar[type] = DummyRowCheck
    column: str


def test_to_check_creates_check_instance() -> None:
    """
    Validates that `to_check()` correctly instantiates the associated check class
    with parameters from the config object.

    Given a DummyRowCheckConfig with required fields,
    calling `.to_check()` should return a DummyRowCheck instance
    with matching attributes.
    """
    # Arrange: Build a config instance
    check_id = "test"
    config = DummyRowCheckConfig(check_id=check_id, column="foo", severity=Severity.WARNING)

    # Act: Convert to check instance
    check = config.to_check()

    # Assert: The check is correctly instantiated
    assert isinstance(check, DummyRowCheck)
    assert check.column == "foo"
    assert check.check_id == check_id
    assert check.severity == Severity.WARNING
