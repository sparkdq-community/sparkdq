from typing import Callable, Dict, Type

from sparkdq.core.base_config import BaseCheckConfig


class CheckConfigRegistry:
    """
    Registry for check configuration classes.

    Maps unique check names (e.g., 'null-check') to their corresponding configuration classes.
    Used to resolve configuration classes dynamically during check instantiation.
    """

    _registry: Dict[str, Type[BaseCheckConfig]] = {}

    @classmethod
    def register(cls, name: str, config_cls: Type[BaseCheckConfig]) -> None:
        """
        Registers a configuration class under a given name.

        Args:
            name (str): Unique name for the check configuration (e.g., 'null-check').
            config_cls (Type[BaseCheckConfig]): The configuration class to register.

        Raises:
            ValueError: If the given name is already registered.
        """
        if name in cls._registry:
            raise ValueError(f"Check config '{name}' is already registered.")
        cls._registry[name] = config_cls

    @classmethod
    def get(cls, name: str) -> Type[BaseCheckConfig]:
        """
        Retrieves a registered configuration class by its check name.

        Args:
            name (str): The name of the check configuration.

        Returns:
            Type[BaseCheckConfig]: The corresponding configuration class.

        Raises:
            KeyError: If no configuration class is registered under the given name.
        """
        if name not in cls._registry:
            raise KeyError(f"No check config registered under name '{name}'.")
        return cls._registry[name]

    @classmethod
    def list_registered(cls) -> Dict[str, Type[BaseCheckConfig]]:
        """
        Returns all registered check configurations.

        Returns:
            Dict[str, Type[BaseCheckConfig]]: Mapping of check names to configuration classes.
        """
        return cls._registry.copy()


def register_check_config(check_name: str) -> Callable[[Type[BaseCheckConfig]], Type[BaseCheckConfig]]:
    """
    Class decorator to register a check configuration class.

    This decorator registers the given configuration class in the CheckConfigRegistry
    under the specified name and returns the class unchanged.

    Args:
        check_name (str): The name to register the configuration class under.

    Returns:
        Callable[[Type[BaseCheckConfig]], Type[BaseCheckConfig]]: The original configuration class.
    """

    def decorator(cls: Type[BaseCheckConfig]) -> Type[BaseCheckConfig]:
        CheckConfigRegistry.register(check_name, cls)
        return cls

    return decorator
