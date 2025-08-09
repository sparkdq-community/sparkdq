from typing import List

from pydantic import Field

from sparkdq.checks.utils.base_comparison_check import BaseMinCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.plugin.check_config_registry import register_check_config


class DateMinCheck(BaseMinCheck):
    """
    Record-level validation check that enforces minimum date boundaries.

    Validates that date values in specified columns meet or exceed a configured
    minimum date threshold. This check is essential for preventing historical
    date entries beyond acceptable limits, enforcing data retention policies,
    and maintaining temporal data integrity across business processes.

    The check supports both inclusive and exclusive boundary semantics, enabling
    precise control over date validation requirements for different business contexts.
    """

    def __init__(
        self,
        check_id: str,
        columns: List[str],
        min_value: str,
        inclusive: bool,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize the minimum date validation check with boundary configuration.

        Args:
            check_id (str): Unique identifier for this check instance.
            columns (List[str]): Date column names that must meet minimum threshold requirements.
            min_value (str): Minimum acceptable date in ISO format (YYYY-MM-DD).
            inclusive (bool): Whether the minimum date threshold includes the boundary
                date itself for validation purposes.
            severity (Severity, optional): Classification level for validation failures.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(
            check_id=check_id,
            columns=columns,
            severity=severity,
            inclusive=inclusive,
            min_value=min_value,
            cast_type="date",
        )


@register_check_config(check_name="date-min-check")
class DateMinCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for minimum date validation checks.

    Defines the parameters required for configuring checks that enforce minimum
    date boundaries. This configuration enables declarative check definition
    through external configuration sources while ensuring parameter validity.

    Attributes:
        columns (List[str]): Date column names that must meet minimum threshold requirements.
        min_value (str): Minimum acceptable date in ISO format (YYYY-MM-DD).
        inclusive (bool): Whether the minimum date threshold includes the boundary date itself.
    """

    check_class = DateMinCheck
    columns: List[str] = Field(..., description="The list of date columns to check for minimum date")
    min_value: str = Field(..., description="Minimum allowed date in YYYY-MM-DD format", alias="min-value")
    inclusive: bool = Field(False, description="Whether the minimum date is inclusive")
    model_config = {
        "populate_by_name": True,
    }
