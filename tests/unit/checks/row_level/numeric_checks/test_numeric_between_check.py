from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.numeric_checks.numeric_between_check import (
    NumericBetweenCheck,
    NumericBetweenCheckConfig,
)
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError


def test_numeric_between_check_config_valid() -> None:
    """
    Validates that NumericBetweenCheckConfig correctly instantiates a NumericBetweenCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, min_value, and max_value.
    """
    config = NumericBetweenCheckConfig(
        check_id="check_value_range",
        columns=["price", "discount"],
        min_value=0.0,
        max_value=100.0,
    )
    check = config.to_check()

    assert isinstance(check, NumericBetweenCheck)
    assert check.check_id == "check_value_range"
    assert check.columns == ["price", "discount"]
    assert check.min_value == 0.0
    assert check.max_value == 100.0


def test_numeric_between_check_config_invalid_range() -> None:
    """
    Validates that NumericBetweenCheckConfig raises an InvalidCheckConfigurationError
    when min_value is greater than max_value.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        NumericBetweenCheckConfig(
            check_id="check_invalid_range",
            columns=["price"],
            min_value=10,
            max_value=5,
        )


def test_numeric_between_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that NumericBetweenCheck flags rows where the numeric column value is outside the defined range.

    A row is marked as failed if the 'price' column is below 0.0 or above 100.0.
    """
    df = spark.createDataFrame([Row(price=0.0), Row(price=5.0), Row(price=100.0)])

    config = NumericBetweenCheckConfig(
        check_id="between_check",
        columns=["price"],
        min_value=0.0,
        max_value=100.0,
    )
    result_df = config.to_check().validate(df)

    expected_df = spark.createDataFrame(
        [(0.0, True), (5.0, False), (100.0, True)],
        ["price", "between_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_numeric_between_check_validate_inclusive_bounds(spark: SparkSession) -> None:
    """
    Verifies that inclusive=True in NumericBetweenCheck allows boundary values to pass.

    A row fails if the value is < min_value or > max_value.
    """
    df = spark.createDataFrame(
        [Row(value=0.0), Row(value=50.0), Row(value=100.0), Row(value=150.0), Row(value=-1.0)]
    )

    config = NumericBetweenCheckConfig(
        check_id="inclusive_between",
        columns=["value"],
        min_value=0.0,
        max_value=100.0,
        inclusive=(True, True),
    )
    result_df = config.to_check().validate(df)

    expected_df = spark.createDataFrame(
        [(0.0, False), (50.0, False), (100.0, False), (150.0, True), (-1.0, True)],
        ["value", "inclusive_between"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_numeric_between_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that NumericBetweenCheck raises MissingColumnError if a specified column does not exist.
    """
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = NumericBetweenCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        min_value=0.0,
        max_value=100.0,
    )
    with pytest.raises(MissingColumnError):
        config.to_check().validate(df)


@pytest.mark.parametrize(
    "range_type,min_value,max_value",
    [
        ("int", 0, 100),
        ("float", 0.0, 100.0),
        ("decimal", Decimal("0.0"), Decimal("100.0")),
    ],
)
def test_numeric_between_check_with_different_numeric_types(
    spark: SparkSession, range_type: str, min_value, max_value
) -> None:
    """
    Validates that NumericBetweenCheck correctly handles different numeric types
    (int, float, Decimal) for min_value and max_value.
    """
    df = spark.createDataFrame([Row(price=50.0), Row(price=150.0)])

    config = NumericBetweenCheckConfig(
        check_id=f"between_check_{range_type}",
        columns=["price"],
        min_value=min_value,
        max_value=max_value,
    )
    result_df = config.to_check().validate(df)
    result = result_df.select(f"between_check_{range_type}").rdd.flatMap(lambda x: x).collect()

    assert result == [False, True], f"Failed for range type: {range_type}"
