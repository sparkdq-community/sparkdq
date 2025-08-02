from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMaxCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.plugin.check_config_registry import register_check_config


class DateMaxCheck(BaseMaxCheck):
    """
    Record-level validation check that enforces maximum date boundaries.

    Validates that date values in specified columns remain within or before a
    configured maximum date threshold. This check is essential for preventing
    future date entries, enforcing reporting period boundaries, and maintaining
    temporal data integrity across business processes.

    The check supports both inclusive and exclusive boundary semantics, enabling
    precise control over date validation requirements for different business contexts.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        max_value: str,
        inclusive: bool,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize the maximum date validation check with boundary configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Date column names that must remain within the maximum threshold.
            max_value (str): Maximum acceptable date in ISO format (YYYY-MM-DD).
            inclusive (bool): Whether the maximum date threshold includes the boundary
                date itself for validation purposes.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(
            check_id=check_id,
            columns=columns,
            severity=severity,
            inclusive=inclusive,
            max_value=max_value,
            cast_type="date",
        )


@register_check_config(check_name="date-max-check")
class DateMaxCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for maximum date validation checks.

    Defines the parameters required for configuring checks that enforce maximum
    date boundaries. This configuration enables declarative check definition
    through external configuration sources while ensuring parameter validity.

    Attributes:
        columns (List[str]): Date column names that must remain within the maximum threshold.
        max_value (str): Maximum acceptable date in ISO format (YYYY-MM-DD).
        inclusive (bool): Whether the maximum date threshold includes the boundary date itself.
    """

    check_class = DateMaxCheck

    columns: List[str] = Field(..., description="Date columns to validate")
    max_value: str = Field(..., description="Maximum allowed date in YYYY-MM-DD format", alias="max-value")
    inclusive: bool = Field(False, description="Whether the maximum date is inclusive")
    model_config = {
        "populate_by_name": True,
    }
