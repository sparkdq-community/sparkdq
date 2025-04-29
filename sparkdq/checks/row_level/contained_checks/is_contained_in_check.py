from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.base_config import BaseRowCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.factory.check_config_registry import register_check_config


class IsContainedInCheck(BaseRowCheck):
    """
    Row-level check that verifies whether column values are contained in a predefined list of allowed values.

    Each specified column must only contain values listed in its associated value list.

    Attributes:
        allowed_values (dict[str, list[object]]): Dictionary mapping column names to their allowed values.
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
        Evaluate whether all specified columns contain only allowed values.

        Args:
            df (DataFrame): The Spark DataFrame to validate.

        Returns:
            DataFrame: The input DataFrame extended with a column indicating pass/fail per row.
        """
        conditions = [F.col(column).isin(values) for column, values in self.allowed_values.items()]

        overall_condition = F.reduce(F.array(*conditions), F.lit(True), lambda acc, x: acc & x)

        return self.with_check_result_column(df, overall_condition)


@register_check_config(check_name="is-contained-in-check")
class IsContainedInCheckConfig(BaseRowCheckConfig):
    """
    Declarative configuration model for the IsContainedInCheck.

    This config allows validation that specified columns contain only predefined values.

    Attributes:
        allowed_values (dict[str, list[object]]): Mapping of column names to allowed values.
    """

    check_class = IsContainedInCheck

    allowed_values: dict[str, list[object]] = Field(
        ..., description="Mapping of column names to lists of allowed values."
    )

    @model_validator(mode="after")
    def validate_allowed_values(self) -> "IsContainedInCheckConfig":
        """
        Validate that allowed_values is not empty and properly formed.

        Returns:
            IsContainedInCheckConfig: The validated configuration object.

        Raises:
            InvalidCheckConfigurationError: If allowed_values is invalid.
        """
        if not self.allowed_values:
            raise InvalidCheckConfigurationError("allowed_values must not be empty.")

        for col, values in self.allowed_values.items():
            if not isinstance(values, list) or not values:
                raise InvalidCheckConfigurationError(
                    f"allowed_values for column '{col}' must be a non-empty list."
                )

        return self
