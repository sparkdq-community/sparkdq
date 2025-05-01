from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.numeric_checks.numeric_min_check import NumericMinCheck, NumericMinCheckConfig
from sparkdq.exceptions import MissingColumnError


def test_numeric_min_check_config_valid() -> None:
    """
    Validates that NumericMinCheckConfig correctly instantiates a NumericMinCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, and min_value.
    """
    # Arrange: Create a config with valid parameters
    config = NumericMinCheckConfig(
        check_id="check_min_value",
        columns=["price", "discount"],
        min_value=0.0,
    )

    # Act: Convert config to check instance
    check = config.to_check()

    # Assert: Check instance has correct attributes
    assert isinstance(check, NumericMinCheck)
    assert check.check_id == "check_min_value"
    assert check.columns == ["price", "discount"]
    assert check.min_value == 0.0


def test_numeric_min_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that NumericMinCheck flags rows where the numeric column value is below min_value.

    A row is marked as failed if the 'price' column is less than 0.0.
    """
    # Arrange: Create a DataFrame with some prices below and above 0.0
    data = [Row(price=10.0), Row(price=5.0), Row(price=-1.0)]
    df = spark.createDataFrame(data)

    config = NumericMinCheckConfig(
        check_id="min_check",
        columns=["price"],
        min_value=0.0,
    )
    check = config.to_check()

    # Act: Apply the NumericMinCheck
    result_df = check.validate(df)

    # Assert: Expect True where 'price' is below 0.0, otherwise False
    expected_df = spark.createDataFrame(
        [(10.0, False), (5.0, False), (-1.0, True)],
        ["price", "min_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_numeric_min_check_inclusive(spark: SparkSession) -> None:
    """
    Verifies that NumericMinCheck flags rows where the numeric value is strictly less than min_value,
    when inclusive is set to True.
    """
    # Arrange
    data = [Row(value=10), Row(value=5), Row(value=1)]
    df = spark.createDataFrame(data)

    config = NumericMinCheckConfig(check_id="min_inclusive", columns=["value"], min_value=5, inclusive=True)
    check = config.to_check()

    # Act
    result_df = check.validate(df)

    # Assert
    expected_df = spark.createDataFrame(
        [
            (10, False),
            (5, False),
            (1, True),
        ],
        ["value", "min_inclusive"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_numeric_min_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that NumericMinCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    # Arrange: DataFrame does not contain the required 'missing' column
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = NumericMinCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        min_value=0.0,
    )
    check = config.to_check()

    # Act & Assert: Expect MissingColumnError when the column is not present
    with pytest.raises(MissingColumnError):
        check.validate(df)


@pytest.mark.parametrize(
    "min_value_type,min_value",
    [
        ("int", 0),
        ("float", 0.0),
        ("decimal", Decimal("0.0")),
    ],
)
def test_numeric_min_check_with_different_numeric_types(
    spark: SparkSession, min_value_type: str, min_value
) -> None:
    """
    Validates that NumericMinCheck correctly handles different numeric types
    (int, float, Decimal) as min_value.

    Ensures consistent behavior across type variations that may occur through YAML configuration.
    """
    # Arrange: Create test DataFrame
    data = [Row(price=5.0), Row(price=-1.0)]  # -1.0 should fail
    df = spark.createDataFrame(data)

    config = NumericMinCheckConfig(
        check_id=f"min_check_{min_value_type}",
        columns=["price"],
        min_value=min_value,
    )
    check = config.to_check()

    # Act: Apply the NumericMinCheck
    result_df = check.validate(df)
    result = result_df.select(f"min_check_{min_value_type}").rdd.flatMap(lambda x: x).collect()

    # Assert: Expect the second row to fail regardless of min_value type
    assert result == [False, True], f"Failed for min_value type: {min_value_type}"
