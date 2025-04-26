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
    # Arrange: Create a config with valid parameters
    config = NumericBetweenCheckConfig(
        check_id="check_value_range",
        columns=["price", "discount"],
        min_value=0.0,
        max_value=100.0,
    )

    # Act: Convert config to check instance
    check = config.to_check()

    # Assert: Check instance has correct attributes
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
    # Arrange & Act & Assert: Expect error on invalid range (min > max)
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
    # Arrange: Create a DataFrame with prices inside and outside the allowed range
    data = [Row(price=50.0), Row(price=-5.0), Row(price=150.0)]  # -5.0 and 150.0 should fail
    df = spark.createDataFrame(data)

    config = NumericBetweenCheckConfig(
        check_id="between_check",
        columns=["price"],
        min_value=0.0,
        max_value=100.0,
    )
    check = config.to_check()

    # Act: Apply the NumericBetweenCheck
    result_df = check.validate(df)

    # Assert: Expect True where 'price' is outside [0.0, 100.0], otherwise False
    expected_df = spark.createDataFrame(
        [(50.0, False), (-5.0, True), (150.0, True)],
        ["price", "between_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_numeric_between_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that NumericBetweenCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    # Arrange: DataFrame does not contain the required 'missing' column
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = NumericBetweenCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        min_value=0.0,
        max_value=100.0,
    )
    check = config.to_check()

    # Act & Assert: Expect MissingColumnError when the column is not present
    with pytest.raises(MissingColumnError):
        check.validate(df)


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

    Ensures consistent behavior across type variations that may occur through YAML configuration.
    """
    # Arrange: Create test DataFrame
    data = [Row(price=50.0), Row(price=150.0)]  # 150.0 should fail
    df = spark.createDataFrame(data)

    config = NumericBetweenCheckConfig(
        check_id=f"between_check_{range_type}",
        columns=["price"],
        min_value=min_value,
        max_value=max_value,
    )
    check = config.to_check()

    # Act: Apply the NumericBetweenCheck
    result_df = check.validate(df)
    result = result_df.select(f"between_check_{range_type}").rdd.flatMap(lambda x: x).collect()

    # Assert: Expect the second row to fail regardless of min/max type
    assert result == [False, True], f"Failed for range type: {range_type}"
