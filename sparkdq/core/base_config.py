"""
Defines the base classes for check configurations within the sparkdq framework.

Check configurations are Pydantic-based and enable:

- Declarative check definitions (e.g., via JSON or YAML).
- Runtime validation of check parameters.
- Factory-based instantiation of concrete check classes.

Each specific check must provide its own configuration class by subclassing
either `BaseRowCheckConfig` or `BaseAggregateCheckConfig`.
"""

from abc import ABC
from typing import Any, ClassVar, Dict, Type

from pydantic import BaseModel, Field

from .base_check import BaseAggregateCheck, BaseCheck, BaseRowCheck
from .severity import Severity


class BaseCheckConfig(BaseModel):
    """
    Base class for check configurations.

    This class defines the interface for converting a configuration object
    into a concrete check instance. Subclasses specify the parameters required
    for their corresponding checks and the associated check class.

    Attributes:
        check_class (ClassVar[Type[BaseCheck]]): The concrete check class to instantiate.
        check_id (str): Unique identifier for the check instance.
        severity (Severity): Severity level used when the check fails.
    """

    check_class: ClassVar[Type[BaseCheck]]
    check_id: str = Field(..., description="Unique identifier for this check instance", alias="check-id")
    severity: Severity = Field(default=Severity.CRITICAL, description="Severity level of the check")

    model_config = {
        "populate_by_name": True,
    }

    def parameters(self) -> Dict[str, Any]:
        """
        Returns the parameters required to instantiate the corresponding check.

        Meta-fields like 'check' are excluded from the result.

        Returns:
            Dict[str, Any]: Parameter names and values for check instantiation.
        """
        params = self.model_dump(exclude={"check"})
        return params

    def to_check(self) -> BaseCheck:
        """
        Instantiates the configured check.

        Returns:
            BaseCheck: The instantiated check object.
        """
        return self.check_class(**self.parameters())


class BaseRowCheckConfig(ABC, BaseCheckConfig):
    """
    Base class for row-level check configurations.

    Row checks operate on individual records and typically require parameters
    such as column names. Subclasses must define a `check_class` that inherits
    from `BaseRowCheck`.

    Raises:
        TypeError: If `check_class` is missing or not a subclass of `BaseRowCheck`.
    """

    def __init_subclass__(cls) -> None:
        """
        Validates that the subclass defines a compatible `check_class`.
        """
        super().__init_subclass__()
        if not hasattr(cls, "check_class") or cls.check_class is None:
            raise TypeError(f"{cls.__name__} must define a 'check_class'.")
        if not issubclass(cls.check_class, BaseRowCheck):
            raise TypeError(f"{cls.__name__}.check_class must be a subclass of BaseRowCheck.")


class BaseAggregateCheckConfig(ABC, BaseCheckConfig):
    """
    Base class for aggregate-level check configurations.

    Aggregate checks evaluate global dataset properties, such as row counts
    or min/max thresholds. Subclasses must define a `check_class` that inherits
    from `BaseAggregateCheck`.

    Raises:
        TypeError: If `check_class` is missing or not a subclass of `BaseAggregateCheck`.
    """

    def __init_subclass__(cls) -> None:
        """
        Validates that the subclass defines a compatible `check_class`.
        """
        super().__init_subclass__()
        if not hasattr(cls, "check_class") or cls.check_class is None:
            raise TypeError(f"{cls.__name__} must define a 'check_class'.")
        if not issubclass(cls.check_class, BaseAggregateCheck):
            raise TypeError(f"{cls.__name__}.check_class must be a subclass of BaseAggregateCheck.")
