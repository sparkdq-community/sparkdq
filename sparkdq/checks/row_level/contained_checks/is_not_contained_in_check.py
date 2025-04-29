from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.factory.check_config_registry import register_check_config


class IsNotContainedInCheck(BaseRowCheck):
    """
    Row-level check that verifies that column values are NOT contained in a forbidden list.

    Attributes:
        forbidden_values (dict[str, list[object]]): Dictionary mapping column names to forbidden values.
    """

    def __init__(
        self,
        check_id: str,
        forbidden_values: dict[str, list[object]],
        severity: Severity = Severity.CRITICAL,
    ):
        super().__init__(check_id=check_id, severity=severity)
        self.forbidden_values = forbidden_values

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Evaluate whether all specified columns do NOT contain forbidden values.

        Args:
            df (DataFrame): The Spark DataFrame to validate.

        Returns:
            DataFrame: The input DataFrame extended with a column indicating pass/fail per row.
        """
        conditions = [~F.col(column).isin(values) for column, values in self.forbidden_values.items()]

        overall_condition = F.reduce(F.array(*conditions), F.lit(True), lambda acc, x: acc & x)

        return self.with_check_result_column(df, overall_condition)


@register_check_config(check_name="is-not-contained-in-check")
class IsNotContainedInCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the IsNotContainedInCheck.

    This config allows validation that specified columns do NOT contain forbidden values.

    Attributes:
        forbidden_values (dict[str, list[object]]): Mapping of column names to forbidden values.
    """

    check_class = IsNotContainedInCheck

    forbidden_values: dict[str, list[object]] = Field(
        ..., description="Mapping of column names to lists of forbidden values."
    )

    @model_validator(mode="after")
    def validate_forbidden_values(self) -> "IsNotContainedInCheckConfig":
        """
        Validate that forbidden_values is not empty and properly formed.

        Returns:
            IsNotContainedInCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If forbidden_values is missing or invalid.
        """
        if not self.forbidden_values:
            raise InvalidCheckConfigurationError("forbidden_values must not be empty.")

        for col, values in self.forbidden_values.items():
            if not isinstance(values, list) or not values:
                raise InvalidCheckConfigurationError(
                    f"forbidden_values for column '{col}' must be a non-empty list."
                )

        return self
