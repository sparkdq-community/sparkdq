from abc import ABC
from typing import List, Optional

from sparkdq.core.base_check import BaseCheck
from sparkdq.core.severity import Severity
from sparkdq.management.check_set import CheckSet


class BaseDQEngine(ABC):
    """
    Abstract base class for data quality validation engines.

    The BaseDQEngine provides shared functionality for both batch and streaming
    validation engines. It manages the registration of checks via a CheckSet and
    defines common behaviors such as determining which checks should be executed.

    This class does not implement the actual execution logic but serves as
    the foundation for concrete engine implementations.
    """

    def __init__(
        self, check_set: Optional[CheckSet] = None, fail_levels: List[Severity] = [Severity.CRITICAL]
    ):
        """
        Initializes the data quality engine.

        Args:
            check_set (Optional[CheckSet]): The set of data quality checks to validate.
            fail_levels (List[Severity]): List of severity levels considered as failure.
                                           Defaults to [Severity.CRITICAL].
        """
        self.fail_levels = fail_levels
        self.check_set = check_set

    def get_checks(self) -> List[BaseCheck]:
        """
        Retrieves the list of checks to be executed by the engine.

        If a CheckSet is defined, its registered checks are returned.
        If no CheckSet is configured, an empty list is returned.

        Returns:
            List[BaseCheck]: The list of checks to be executed.
        """
        if self.check_set is not None:
            return self.check_set.get_all()
        return []
