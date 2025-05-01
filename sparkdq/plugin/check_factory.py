from typing import Any, Dict, List

from sparkdq.core.base_check import BaseCheck
from sparkdq.core.severity import normalize_severity
from sparkdq.exceptions import MissingCheckTypeError
from sparkdq.plugin.check_config_registry import CheckConfigRegistry, load_config_module


class CheckFactory:
    """
    Factory for creating check instances from configuration dictionaries.

    The CheckFactory uses the CheckConfigRegistry to resolve check names
    to their corresponding configuration classes. It validates input dictionaries
    against the expected schema and creates check instances via the configuration's
    `to_check()` method.

    This allows checks to be created dynamically from external sources
    such as JSON, YAML, or API calls.
    """

    @staticmethod
    def _from_dict(config_data: Dict[str, Any]) -> BaseCheck:
        """
        Creates a check instance from a configuration dictionary.

        The configuration must contain a "check" key that specifies the
        registered check type in the CheckConfigRegistry. If a "severity"
        field is provided, it will be normalized to a valid Severity enum.

        Args:
            config_data (Dict[str, Any]): The check configuration.

        Returns:
            BaseCheck: The instantiated and validated check object.

        Raises:
            MissingCheckTypeError: If the "check" field is missing.
            ValidationError: If the configuration data is invalid for the resolved config class.
        """
        check_name = config_data.get("check")
        if not check_name:
            raise MissingCheckTypeError()

        # Normalize severity if present
        if "severity" in config_data:
            config_data["severity"] = normalize_severity(config_data["severity"])

        config_cls = CheckConfigRegistry.get(check_name)
        config = config_cls.model_validate(config_data)
        return config.to_check()

    @staticmethod
    def from_list(config_list: List[Dict[str, Any]]) -> List[BaseCheck]:
        """
        Creates multiple check instances from a list of configuration dictionaries.

        This method calls `from_dict()` for each configuration entry and is typically
        used for bulk configuration loading (e.g., from a YAML or JSON array).

        Args:
            config_list (List[Dict[str, Any]]): A list of check configuration dictionaries.

        Returns:
            List[BaseCheck]: A list of instantiated and validated check objects.
        """
        # Ensure that all check classes are registered in the CheckFactory
        load_config_module("sparkdq.checks")
        return [CheckFactory._from_dict(cfg) for cfg in config_list]
