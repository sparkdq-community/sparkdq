import re
from typing import ClassVar

from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.factory.check_config_registry import register_check_config


class SchemaCheck(BaseAggregateCheck):
    """
    Aggregate-level data quality check that verifies whether a DataFrame matches an expected schema.

    This check compares the DataFrame's schema (column names and types) against a provided
    expected schema definition. It optionally enforces strict schema matching, rejecting
    any additional unexpected columns not listed in the expected schema.

    Attributes:
        expected_schema (dict[str, str]): A mapping of column names to expected Spark data types.
        strict (bool): Whether to fail the check if the DataFrame contains columns not listed
            in the expected schema.
    """

    def __init__(
        self,
        check_id: str,
        expected_schema: dict[str, str],
        strict: bool = True,
        severity: Severity = Severity.CRITICAL,
    ):
        """
        Initialize a new ExpectedSchemaCheck instance.

        Args:
            check_id (str): Unique identifier for the check instance.
            expected_schema (dict[str, str]): A dictionary defining expected column names
                and their corresponding Spark data types (as strings).
            strict (bool, optional): If True, the presence of any additional columns
                not listed in the expected schema will cause the check to fail.
                Defaults to True.
            severity (Severity, optional): The severity level to assign if the check fails.
                Defaults to Severity.CRITICAL.
        """
        super().__init__(check_id=check_id, severity=severity)
        self.expected_schema = expected_schema
        self.strict = strict

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluate the schema of the given DataFrame against the expected schema definition.

        The check passes if:
        - all expected columns are present,
        - all expected types match,
        - (if strict) no unexpected columns are found.

        Args:
            df (DataFrame): The Spark DataFrame to evaluate.

        Returns:
            AggregateEvaluationResult: An object indicating the outcome of the check,
            including metrics for missing columns, type mismatches, and unexpected columns.
        """
        actual_schema = {field.name: field.dataType.simpleString() for field in df.schema}

        missing_columns = [col for col in self.expected_schema if col not in actual_schema]

        unexpected_columns = (
            [col for col in actual_schema if col not in self.expected_schema] if self.strict else []
        )

        type_mismatches = {
            col: {
                "expected": self.expected_schema[col],
                "actual": actual_schema[col],
            }
            for col in self.expected_schema
            if col in actual_schema and actual_schema[col] != self.expected_schema[col]
        }

        passed = not missing_columns and not type_mismatches and not unexpected_columns

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "missing_columns": missing_columns,
                "type_mismatches": type_mismatches,
                "unexpected_columns": unexpected_columns,
            },
        )


@register_check_config(check_name="schema-check")
class SchemaCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for the ExpectedSchemaCheck.

    Ensures the DataFrame matches the expected schema, with optional strict mode.
    Validates all specified types, including support for decimal(p,s) types.

    Attributes:
        expected_schema (dict[str, str]): Required column names and Spark types.
        strict (bool): Whether to disallow unexpected columns.
    """

    check_class = SchemaCheck

    expected_schema: dict[str, str] = Field(
        ..., description="Expected schema mapping of column names to Spark data types"
    )
    strict: bool = Field(
        default=True,
        description="Whether to disallow extra columns not listed in the expected schema",
    )

    _valid_spark_types: ClassVar[set[str]] = {
        "string",
        "boolean",
        "int",
        "bigint",
        "float",
        "double",
        "date",
        "timestamp",
        "binary",
        "array",
        "map",
        "struct",
        "decimal",
    }
    _decimal_pattern: ClassVar[re.Pattern[str]] = re.compile(r"^decimal\(\d+,\s*\d+\)$")

    @classmethod
    def is_valid_type(cls, _type: str) -> bool:
        return _type in cls._valid_spark_types or (
            _type.startswith("decimal") and bool(cls._decimal_pattern.match(_type))
        )

    @model_validator(mode="after")
    def validate_schema(self) -> "SchemaCheckConfig":
        """
        Validates that expected_schema is not empty and all types are valid.

        Raises:
            InvalidCheckConfigurationError: If any type is invalid.
        """
        if not self.expected_schema:
            raise InvalidCheckConfigurationError("expected_schema must not be empty.")

        invalid_types = [
            (col, typ) for col, typ in self.expected_schema.items() if not self.is_valid_type(typ)
        ]
        if invalid_types:
            raise InvalidCheckConfigurationError(
                f"Invalid Spark types in expected_schema: {invalid_types}. "
                f"Allowed types include: {sorted(self._valid_spark_types)} and decimal(p,s)"
            )

        return self
