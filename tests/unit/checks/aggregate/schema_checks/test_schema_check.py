import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

from sparkdq.checks.aggregate.schema_checks.schema_check import SchemaCheck, SchemaCheckConfig
from sparkdq.exceptions import InvalidCheckConfigurationError


def test_schema_check_passes_with_exact_match(spark: SparkSession) -> None:
    """
    Validates that SchemaCheck passes when the DataFrame matches the expected schema exactly.
    """
    df = spark.createDataFrame([(100.0, "Alice")], ["costs", "name"])
    check = SchemaCheck(check_id="test", expected_schema={"costs": "double", "name": "string"})
    result = check._evaluate_logic(df)
    assert result.metrics["missing_columns"] == []
    assert result.metrics["type_mismatches"] == {}
    assert result.metrics["unexpected_columns"] == []
    assert result.passed is True


def test_schema_check_fails_with_missing_column(spark: SparkSession) -> None:
    """
    Validates that SchemaCheck fails when the DataFrame is missing a required column.
    """
    df = spark.createDataFrame([(100.0,)], ["costs"])
    check = SchemaCheck(check_id="test", expected_schema={"costs": "double", "name": "string"})
    result = check._evaluate_logic(df)
    assert result.passed is False
    assert result.metrics["missing_columns"] == ["name"]
    assert result.metrics["type_mismatches"] == {}
    assert result.metrics["unexpected_columns"] == []


def test_schema_check_fails_with_type_mismatch(spark: SparkSession) -> None:
    """
    Validates that SchemaCheck fails when a column's type does not match the expected type.
    """
    df = spark.createDataFrame([(1, True)], ["id", "active"])
    check = SchemaCheck(check_id="test", expected_schema={"id": "bigint", "active": "string"})
    result = check._evaluate_logic(df)
    assert result.passed is False
    assert result.metrics["missing_columns"] == []
    assert result.metrics["unexpected_columns"] == []
    assert result.metrics["type_mismatches"] == {"active": {"expected": "string", "actual": "boolean"}}


def test_schema_check_fails_with_unexpected_column_in_strict_mode(spark: SparkSession) -> None:
    """
    Validates that SchemaCheck fails in strict mode when the DataFrame contains unexpected columns.
    """
    df = spark.createDataFrame([(1, "x")], ["id", "extra"])
    check = SchemaCheck(check_id="test", expected_schema={"id": "bigint"}, strict=True)
    result = check._evaluate_logic(df)
    assert result.passed is False
    assert result.metrics["unexpected_columns"] == ["extra"]


def test_schema_check_allows_extra_columns_in_non_strict_mode(spark: SparkSession) -> None:
    """
    Validates that SchemaCheck passes in non-strict mode when the DataFrame contains extra columns.
    """
    df = spark.createDataFrame([(1, "x")], ["id", "extra"])
    check = SchemaCheck(check_id="test", expected_schema={"id": "bigint"}, strict=False)
    result = check._evaluate_logic(df)
    assert result.passed is True
    assert result.metrics["unexpected_columns"] == []


def test_schema_check_accepts_decimal_type(spark: SparkSession) -> None:
    """
    Validates that SchemaCheck correctly matches decimal column types like decimal(10,2).
    """
    df = spark.createDataFrame([(1,)], schema=["amount"])
    df = df.withColumn("amount", df["amount"].cast(DecimalType(10, 2)))
    check = SchemaCheck(check_id="test", expected_schema={"amount": "decimal(10,2)"})
    result = check._evaluate_logic(df)
    assert result.passed is True


@pytest.mark.parametrize(
    "expected_type",
    ["int", "string", "boolean", "decimal(10,2)", "timestamp", "float"],
)
def test_schema_check_config_accepts_valid_types(expected_type: str) -> None:
    """
    Validates that SchemaCheckConfig accepts all supported and properly formatted Spark types.
    """
    config = SchemaCheckConfig(check_id="test", expected_schema={"col": expected_type})
    assert config.expected_schema["col"] == expected_type


@pytest.mark.parametrize(
    "invalid_type",
    ["sting", "decimal(10.2)", "decimal()", "decimal(abc,2)"],
)
def test_schema_check_config_rejects_invalid_types(invalid_type: str) -> None:
    """
    Validates that SchemaCheckConfig raises an error for invalid or malformed type strings.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        SchemaCheckConfig(check_id="test", expected_schema={"col": invalid_type})


def test_schema_check_config_raises_for_empty_schema() -> None:
    """
    Validates that SchemaCheckConfig raises an error when expected_schema is empty.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        SchemaCheckConfig(check_id="test", expected_schema={})
