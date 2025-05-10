from typing import Any, Dict, List

from sparkdq.core.base_check import BaseAggregateCheck, BaseCheck, BaseRowCheck
from sparkdq.core.base_config import BaseCheckConfig
from sparkdq.plugin.check_factory import CheckFactory


class CheckSet:
    """
    Manages a collection of data quality checks.

    The CheckSet handles the lifecycle of data quality checks within the framework.
    It converts configurations into concrete check instances, keeps track of all
    registered checks, and provides filtered access to row-level or aggregate-level
    checks.

    This component decouples check definition from check execution.
    """

    def __init__(self) -> None:
        """Initializes an empty CheckSet."""
        self._checks: List[BaseCheck] = []

    def add_check(self, config: BaseCheckConfig) -> "CheckSet":
        """
        Adds a single check from a validated configuration object and returns self for fluent chaining.

        Args:
            config (BaseCheckConfig): The configuration object defining the check.

        Returns:
            CheckSet: The current instance with the added check.
        """
        self._checks.append(config.to_check())
        return self

    def add_checks_from_dicts(self, configs: List[Dict[str, Any]]) -> None:
        """
        Adds multiple checks from raw configuration dictionaries using the CheckFactory.

        Note:
            The `sparkdq.checks` module is imported here to ensure that all available
            checks are registered in the CheckFactory before instantiation.

        Args:
            configs (List[Dict[str, Any]]): A list of configuration dictionaries defining the checks.
        """
        self._checks.extend(CheckFactory.from_list(configs))

    def get_all(self) -> List[BaseCheck]:
        """
        Returns all registered checks.

        Returns:
            List[BaseCheck]: All checks currently managed by this CheckSet.
        """
        return self._checks

    def get_row_checks(self) -> List[BaseRowCheck]:
        """
        Returns only the row-level checks.

        Returns:
            List[BaseRowCheck]: Checks that operate on individual rows.
        """
        return [check for check in self._checks if isinstance(check, BaseRowCheck)]

    def get_aggregate_checks(self) -> List[BaseAggregateCheck]:
        """
        Returns only the aggregate-level checks.

        Returns:
            List[BaseAggregateCheck]: Checks that operate on DataFrame aggregates.
        """
        return [check for check in self._checks if isinstance(check, BaseAggregateCheck)]

    def clear(self) -> None:
        """
        Removes all currently registered checks.

        Useful for resetting the CheckSet between validation runs.
        """
        self._checks.clear()

    def __repr__(self) -> str:
        """
        Returns a developer-friendly string representation of the CheckSet.

        Returns:
            str: Representation showing the number of registered checks.
        """
        return f"<CheckSet (total checks: {len(self._checks)})>"

    def __str__(self) -> str:
        """
        Returns a human-readable string listing the registered checks.

        Returns:
            str: String listing all check IDs and their types.
        """
        if not self._checks:
            return "CheckSet: No checks registered."

        lines = ["CheckSet:"]
        for check in self._checks:
            check_id = getattr(check, "check_id", "unknown-id")
            check_type = type(check).__name__
            lines.append(f"  - {check_id} ({check_type})")
        return "\n".join(lines)
