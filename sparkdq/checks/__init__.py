"""
This module serves as the dynamic entry point for the sparkdq.checks subpackage.

It recursively traverses all submodules within the sparkdq.checks package to identify and register
all classes that inherit from either `BaseRowCheckConfig` or `BaseAggregateCheckConfig`.

Each valid check configuration class is added to the module’s global namespace and included in
`__all__`, ensuring it becomes part of the public API.

This approach eliminates the need for manual imports and `__all__` maintenance, making the system
easy to extend as new check classes are added. It also allows developers and users to import any
check configuration directly from the top-level `sparkdq` namespace.

This mechanism incurs a small runtime overhead during the initial import of `sparkdq.checks`,
but improves maintainability and scalability significantly in large modular frameworks.
"""

import importlib
import inspect
import pkgutil

from sparkdq.core.base_config import BaseAggregateCheckConfig, BaseRowCheckConfig

__all__ = []

base_classes = (BaseRowCheckConfig, BaseAggregateCheckConfig)


def is_valid_check_class(obj: object) -> bool:
    """
    Checks whether obj is a class that inherits from one of the base config classes,
    excluding the base classes themselves.
    """
    return inspect.isclass(obj) and issubclass(obj, base_classes) and obj not in base_classes


# Recursively scan all submodules and collect valid check config classes
for _, module_name, _ in pkgutil.walk_packages(__path__, prefix=f"{__name__}."):  # type: ignore
    module = importlib.import_module(module_name)
    for name, obj in inspect.getmembers(module, is_valid_check_class):
        globals()[name] = obj
        __all__.append(name)
