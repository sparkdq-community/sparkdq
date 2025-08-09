from abc import ABC
from typing import List, Optional

from sparkdq.core.base_check import BaseCheck
from sparkdq.core.severity import Severity
from sparkdq.management.check_set import CheckSet


class BaseDQEngine(ABC):
    """
    Abstract foundation for data quality validation execution engines.

    Establishes the core architecture for validation engines that orchestrate
    the execution of data quality checks across different processing paradigms.
    This class provides shared functionality for check management, severity-based
    filtering, and execution coordination while remaining agnostic to the specific
    execution model (batch, streaming, or real-time).

    The design enables consistent behavior across different engine implementations
    while providing the flexibility needed to optimize for specific execution
    contexts and performance requirements.
    """

    def __init__(
        self, check_set: Optional[CheckSet] = None, fail_levels: List[Severity] = [Severity.CRITICAL]
    ):
        """
        Initialize the validation engine with check configuration and failure criteria.

        Args:
            check_set (Optional[CheckSet]): Collection of data quality checks to execute.
                Can be None for engines that will have checks assigned later.
            fail_levels (List[Severity]): Severity levels that should be treated as
                validation failures. Defaults to [Severity.CRITICAL] for blocking
                behavior on critical issues only.
        """
        self.fail_levels = fail_levels
        self.check_set = check_set

    def get_checks(self) -> List[BaseCheck]:
        """
        Retrieve the complete list of checks configured for execution.

        Provides access to all registered checks within the current CheckSet,
        enabling engine implementations to iterate over and execute the
        configured validations. Returns an empty list when no CheckSet
        has been configured.

        Returns:
            List[BaseCheck]: Complete collection of checks ready for execution,
                or empty list if no CheckSet is configured.
        """
        if self.check_set is not None:
            return self.check_set.get_all()
        return []
