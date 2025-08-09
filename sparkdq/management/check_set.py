from typing import Any, Dict, List

from sparkdq.core.base_check import BaseAggregateCheck, BaseCheck, BaseRowCheck
from sparkdq.core.base_config import BaseCheckConfig
from sparkdq.plugin.check_factory import CheckFactory


class CheckSet:
    """
    Centralized registry and lifecycle manager for data quality checks.

    Orchestrates the complete lifecycle of data quality checks from configuration
    to execution readiness, providing a unified interface for check registration,
    organization, and retrieval. The CheckSet abstracts the complexity of check
    instantiation and type management, enabling clean separation between check
    definition and execution logic.

    This design supports both programmatic and declarative check registration
    patterns while maintaining type safety and providing convenient filtering
    capabilities for different execution contexts.
    """

    def __init__(self) -> None:
        """Initialize an empty CheckSet ready for check registration."""
        self._checks: List[BaseCheck] = []

    def add_check(self, config: BaseCheckConfig) -> "CheckSet":
        """
        Register a single check from a validated configuration object.

        Instantiates the check from the provided configuration and adds it to
        the internal registry. The fluent interface enables method chaining
        for convenient multi-check registration.

        Args:
            config (BaseCheckConfig): Validated configuration object containing
                all parameters required for check instantiation.

        Returns:
            CheckSet: This instance to enable fluent method chaining.
        """
        self._checks.append(config.to_check())
        return self

    def add_checks_from_dicts(self, configs: List[Dict[str, Any]]) -> None:
        """
        Register multiple checks from raw configuration dictionaries.

        Processes a collection of configuration dictionaries through the CheckFactory,
        performing validation and instantiation for each check definition. This method
        enables bulk registration from external configuration sources such as JSON,
        YAML, or database records.

        Args:
            configs (List[Dict[str, Any]]): Collection of configuration dictionaries,
                each containing the parameters required for a specific check type.
        """
        self._checks.extend(CheckFactory.from_list(configs))

    def get_all(self) -> List[BaseCheck]:
        """
        Retrieve the complete collection of registered checks.

        Returns:
            List[BaseCheck]: All checks currently managed by this CheckSet,
                regardless of their specific type or implementation.
        """
        return self._checks

    def get_row_checks(self) -> List[BaseRowCheck]:
        """
        Retrieve only the record-level validation checks.

        Filters the registered checks to return only those that operate on
        individual records, enabling targeted execution for row-level
        validation scenarios.

        Returns:
            List[BaseRowCheck]: Collection of checks that validate individual
                records within the dataset.
        """
        return [check for check in self._checks if isinstance(check, BaseRowCheck)]

    def get_aggregate_checks(self) -> List[BaseAggregateCheck]:
        """
        Retrieve only the dataset-level validation checks.

        Filters the registered checks to return only those that evaluate
        global dataset properties, enabling targeted execution for
        aggregate-level validation scenarios.

        Returns:
            List[BaseAggregateCheck]: Collection of checks that validate
                dataset-wide properties and constraints.
        """
        return [check for check in self._checks if isinstance(check, BaseAggregateCheck)]

    def clear(self) -> None:
        """
        Remove all registered checks from this CheckSet.

        Clears the internal check registry, returning the CheckSet to its
        initial empty state. This operation is useful for resetting between
        validation runs or when reconfiguring the check collection.
        """
        self._checks.clear()

    def __repr__(self) -> str:
        """
        Generate a concise developer-oriented representation of this CheckSet.

        Returns:
            str: Summary representation indicating the total number of
                registered checks for debugging and logging purposes.
        """
        return f"<CheckSet (total checks: {len(self._checks)})>"

    def __str__(self) -> str:
        """
        Generate a detailed human-readable listing of all registered checks.

        Produces a formatted string containing the check identifiers and types
        for all registered checks, providing comprehensive visibility into the
        current CheckSet configuration.

        Returns:
            str: Formatted listing of all registered checks with their
                identifiers and implementation types.
        """
        if not self._checks:
            return "CheckSet: No checks registered."

        lines = ["CheckSet:"]
        for check in self._checks:
            check_id = getattr(check, "check_id", "unknown-id")
            check_type = type(check).__name__
            lines.append(f"  - {check_id} ({check_type})")
        return "\n".join(lines)
