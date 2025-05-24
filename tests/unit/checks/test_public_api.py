import importlib
import inspect
import pkgutil

from sparkdq import checks
from sparkdq.core.base_config import BaseAggregateCheckConfig, BaseRowCheckConfig


def get_all_check_config_subclasses(package) -> set[str]:
    """
    Scans the entire sparkdq package for subclasses of BaseRowCheckConfig or BaseAggregateCheckConfig.
    Returns the set of class names that should be publicly exposed.
    """
    base_classes = (BaseRowCheckConfig, BaseAggregateCheckConfig)
    subclasses = set()

    for _, module_name, _ in pkgutil.walk_packages(package.__path__, prefix=package.__name__ + "."):
        module = importlib.import_module(module_name)
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, base_classes) and obj not in base_classes:
                subclasses.add(name)
    return subclasses


def test_all_check_configs_are_exposed_in_public_api():
    """
    Validates that all subclasses of BaseRowCheckConfig or BaseAggregateCheckConfig
    are included in sparkdq.__all__.
    """
    # Arrange
    expected_exports = get_all_check_config_subclasses(checks)
    actual_exports = set(checks.__all__)

    # Assert
    missing = expected_exports - actual_exports
    assert not missing, f"Missing public exports in __all__: {missing}"
