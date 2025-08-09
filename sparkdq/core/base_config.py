"""
Base configuration classes for declarative data quality check definitions.

This module provides the foundational infrastructure for defining data quality checks
through configuration objects rather than direct instantiation. The Pydantic-based
approach enables robust parameter validation, serialization support, and seamless
integration with external configuration sources such as JSON, YAML, or database records.

The configuration system supports both programmatic and declarative workflows,
allowing teams to define validation rules in configuration files while maintaining
type safety and parameter validation. Each configuration class encapsulates the
parameters required for its corresponding check implementation and provides
factory methods for instantiation.
"""

from abc import ABC
from typing import Any, ClassVar, Dict, Type

from pydantic import BaseModel, Field

from .base_check import BaseAggregateCheck, BaseCheck, BaseRowCheck
from .severity import Severity


class BaseCheckConfig(BaseModel):
    """
    Abstract foundation for all data quality check configurations.

    Establishes the core contract for configuration-driven check instantiation,
    providing standardized parameter validation and factory methods. This class
    enables declarative check definitions while ensuring type safety and proper
    parameter validation through Pydantic's validation framework.

    The configuration approach decouples check definition from implementation,
    enabling flexible deployment scenarios where validation rules can be
    externally managed and dynamically loaded.

    Attributes:
        check_class (ClassVar[Type[BaseCheck]]): The concrete check implementation
            that this configuration will instantiate.
        check_id (str): Unique identifier for the check instance within the validation context.
        severity (Severity): Classification level determining failure handling behavior.
    """

    check_class: ClassVar[Type[BaseCheck]]
    check_id: str = Field(..., description="Unique identifier for this check instance", alias="check-id")
    severity: Severity = Field(default=Severity.CRITICAL, description="Severity level of the check")

    model_config = {
        "populate_by_name": True,
    }

    def parameters(self) -> Dict[str, Any]:
        """
        Extract configuration parameters for check instantiation.

        Produces a clean parameter dictionary suitable for passing to the check
        constructor, automatically excluding framework-internal fields that are
        not part of the check's interface.

        Returns:
            Dict[str, Any]: Configuration parameters ready for check instantiation,
                with framework metadata filtered out.
        """
        params = self.model_dump(exclude={"check"})
        return params

    def to_check(self) -> BaseCheck:
        """
        Create a check instance from this configuration.

        Applies the factory pattern to instantiate the appropriate check class
        with the validated configuration parameters, ensuring type safety and
        proper initialization.

        Returns:
            BaseCheck: Fully configured and validated check instance ready for execution.
        """
        return self.check_class(**self.parameters())


class BaseRowCheckConfig(ABC, BaseCheckConfig):
    """
    Abstract configuration foundation for record-level data quality checks.

    Specializes the base configuration interface for checks that operate on individual
    records within a dataset. This class enforces that subclasses are properly
    associated with row-level check implementations and provides the appropriate
    validation and instantiation logic.

    Row-level configurations typically include parameters such as column specifications,
    validation thresholds, and record-specific business rules.

    Raises:
        TypeError: When the subclass fails to define a valid check_class or associates
            with a non-row-level check implementation.
    """

    def __init_subclass__(cls) -> None:
        """
        Enforce proper check class association during subclass definition.

        Validates that the configuration subclass correctly associates with a
        row-level check implementation, preventing runtime errors and ensuring
        type safety across the configuration system.
        """
        super().__init_subclass__()
        if not hasattr(cls, "check_class") or cls.check_class is None:
            raise TypeError(f"{cls.__name__} must define a 'check_class'.")
        if not issubclass(cls.check_class, BaseRowCheck):
            raise TypeError(f"{cls.__name__}.check_class must be a subclass of BaseRowCheck.")


class BaseAggregateCheckConfig(ABC, BaseCheckConfig):
    """
    Abstract configuration foundation for dataset-level data quality checks.

    Specializes the base configuration interface for checks that evaluate global
    properties of entire datasets. This class enforces proper association with
    aggregate-level check implementations and provides appropriate validation
    and instantiation capabilities.

    Aggregate-level configurations typically include parameters such as statistical
    thresholds, count boundaries, and dataset-wide business rules.

    Raises:
        TypeError: When the subclass fails to define a valid check_class or associates
            with a non-aggregate-level check implementation.
    """

    def __init_subclass__(cls) -> None:
        """
        Enforce proper check class association during subclass definition.

        Validates that the configuration subclass correctly associates with an
        aggregate-level check implementation, preventing runtime errors and
        ensuring type safety across the configuration system.
        """
        super().__init_subclass__()
        if not hasattr(cls, "check_class") or cls.check_class is None:
            raise TypeError(f"{cls.__name__} must define a 'check_class'.")
        if not issubclass(cls.check_class, BaseAggregateCheck):
            raise TypeError(f"{cls.__name__}.check_class must be a subclass of BaseAggregateCheck.")
