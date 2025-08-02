from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config


class IsContainedInCheck(BaseRowCheck):
    """
    Record-level validation check that enforces value whitelist constraints.

    Validates that values in specified columns are contained within predefined
    allowed value lists, ensuring data integrity and business rule compliance.
    This check is essential for validating categorical data, status codes,
    enumerated values, or any scenario where only specific values are acceptable.

    The check supports multiple columns with independent allowed value lists,
    enabling comprehensive validation of related categorical fields.

    Attributes:
        allowed_values (dict[str, list[object]]): Mapping of column names to their
            corresponding lists of acceptable values.
    """

    def __init__(
        self,
        check_id: str,
        allowed_values: dict[str, list[object]],
        severity: Severity = Severity.CRITICAL,
    ):
        super().__init__(check_id=check_id, severity=severity)
        self.allowed_values = allowed_values

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Execute the value whitelist validation logic against the configured columns.

        Performs validation to ensure all configured columns contain only values
        from their respective allowed value lists. Records fail validation when
        any configured column contains values outside the permitted set.

        Args:
            df (DataFrame): The dataset to validate for value containment compliance.

        Returns:
            DataFrame: Original dataset augmented with a boolean result column where
                True indicates validation failure (disallowed values detected) and
                False indicates compliance with whitelist requirements.
        """
        conditions = [F.col(column).isin(values) for column, values in self.allowed_values.items()]

        overall_condition = F.aggregate(F.array(*conditions), F.lit(True), lambda acc, x: acc & x)

        return self.with_check_result_column(df, overall_condition)


@register_check_config(check_name="is-contained-in-check")
class IsContainedInCheckConfig(BaseRowCheckConfig):
    """
    Configuration schema for value whitelist validation checks.

    Defines the parameters and validation rules for configuring checks that
    enforce value containment constraints. The configuration includes logical
    validation to ensure allowed value specifications are complete and meaningful.

    Attributes:
        allowed_values (dict[str, list[object]]): Mapping of column names to their
            corresponding lists of acceptable values.
    """

    check_class = IsContainedInCheck

    allowed_values: dict[str, list[object]] = Field(
        ..., description="Mapping of column names to lists of allowed values.", alias="allowed-values"
    )

    @model_validator(mode="after")
    def validate_allowed_values(self) -> "IsContainedInCheckConfig":
        """
        Validate the logical consistency and completeness of the allowed values configuration.

        Ensures that the allowed values mapping is properly structured with non-empty
        value lists for each configured column. This validation prevents configuration
        errors that would result in impossible or meaningless validation conditions.

        Returns:
            IsContainedInCheckConfig: The validated configuration instance.

        Raises:
            InvalidCheckConfigurationError: When the allowed values configuration is
                empty, malformed, or contains invalid value specifications.
        """
        if not self.allowed_values:
            raise InvalidCheckConfigurationError("allowed_values must not be empty.")

        for col, values in self.allowed_values.items():
            if not isinstance(values, list) or not values:
                raise InvalidCheckConfigurationError(
                    f"allowed_values for column '{col}' must be a non-empty list."
                )

        return self
