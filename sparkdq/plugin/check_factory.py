from typing import Any, Dict, List

from sparkdq.core.base_check import BaseCheck
from sparkdq.core.severity import normalize_severity
from sparkdq.exceptions import MissingCheckTypeError
from sparkdq.plugin.check_config_registry import CheckConfigRegistry, load_config_module


class CheckFactory:
    """
    Factory for dynamic check instantiation from configuration dictionaries.

    Orchestrates the complete process of transforming raw configuration data into
    executable check instances, including type resolution, parameter validation,
    and proper instantiation. The factory leverages the CheckConfigRegistry to
    maintain loose coupling between check implementations and their configurations.

    This design enables flexible deployment scenarios where validation rules can
    be externally defined and dynamically loaded from various sources including
    configuration files, databases, or API endpoints.
    """

    @staticmethod
    def _from_dict(config_data: Dict[str, Any]) -> BaseCheck:
        """
        Instantiate a check from a raw configuration dictionary.

        Processes a configuration dictionary through the complete validation and
        instantiation pipeline, including check type resolution, parameter validation,
        and severity normalization. This method serves as the core transformation
        point between external configuration sources and executable check instances.

        Args:
            config_data (Dict[str, Any]): Raw configuration dictionary containing
                check type specification and all required parameters.

        Returns:
            BaseCheck: Fully validated and configured check instance ready for execution.

        Raises:
            MissingCheckTypeError: When the configuration lacks the required
                'check' field for type identification.
            ValidationError: When the configuration parameters fail validation
                against the resolved check configuration schema.
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
        Instantiate multiple checks from a collection of configuration dictionaries.

        Processes a collection of configuration dictionaries through the complete
        validation and instantiation pipeline, enabling efficient bulk loading
        from external configuration sources. This method ensures all check
        implementations are properly registered before processing begins.

        Args:
            config_list (List[Dict[str, Any]]): Collection of configuration
                dictionaries, each defining a complete check specification.

        Returns:
            List[BaseCheck]: Collection of fully validated and configured check
                instances ready for execution.
        """
        # Ensure that all check classes are registered in the CheckFactory
        load_config_module("sparkdq.checks")
        return [CheckFactory._from_dict(cfg) for cfg in config_list]
