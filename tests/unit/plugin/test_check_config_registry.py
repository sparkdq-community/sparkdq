"""
Unit tests for the CheckConfigRegistry and @register_check_config decorator.

These tests ensure:
- Correct registration and retrieval of check config classes
- Enforcement of uniqueness (no duplicate names allowed)
- Safe lookup and introspection
- Decorator-based registration works as expected

The registry enables dynamic configuration of checks via names and classes.
"""

from typing import ClassVar, Type

import pytest

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.plugin.check_config_registry import CheckConfigRegistry, register_check_config


class DummyRowCheck(BaseRowCheck):
    def validate(self, df): ...
    def error_column(self) -> str:
        return "dummy_error"


class DummyConfig(BaseRowCheckConfig):
    check_class: ClassVar[Type[BaseRowCheck]] = DummyRowCheck
    column: str


def test_register_and_get_config() -> None:
    """
    Validates that a config class can be registered and retrieved by name.
    """
    CheckConfigRegistry._registry.clear()

    CheckConfigRegistry.register("dummy-check", DummyConfig)
    retrieved = CheckConfigRegistry.get("dummy-check")

    assert retrieved is DummyConfig


def test_register_twice_raises() -> None:
    """
    Validates that registering the same name twice raises a ValueError.
    """
    CheckConfigRegistry._registry.clear()
    CheckConfigRegistry.register("duplicate-check", DummyConfig)

    with pytest.raises(ValueError, match="already registered"):
        CheckConfigRegistry.register("duplicate-check", DummyConfig)


def test_get_unregistered_name_raises() -> None:
    """
    Validates that accessing an unregistered check name raises a KeyError.
    """
    CheckConfigRegistry._registry.clear()

    with pytest.raises(KeyError, match="No check config registered"):
        CheckConfigRegistry.get("not-found")


def test_list_registered_returns_copy() -> None:
    """
    Validates that list_registered() returns a shallow copy of the registry.
    """
    CheckConfigRegistry._registry.clear()
    CheckConfigRegistry.register("dummy-check", DummyConfig)

    listed = CheckConfigRegistry.list_registered()

    assert "dummy-check" in listed
    assert listed["dummy-check"] is DummyConfig
    assert listed is not CheckConfigRegistry._registry  # ensure it's a copy


def test_register_check_config_decorator() -> None:
    """
    Validates that the @register_check_config decorator registers the class correctly.
    """
    CheckConfigRegistry._registry.clear()

    @register_check_config("decorated-check")
    class DecoratedConfig(BaseRowCheckConfig):
        check_class: ClassVar[Type[BaseRowCheck]] = DummyRowCheck
        column: str

    retrieved = CheckConfigRegistry.get("decorated-check")
    assert retrieved is DecoratedConfig
