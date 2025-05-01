from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.numeric_checks.numeric_max_check import NumericMaxCheck, NumericMaxCheckConfig
from sparkdq.exceptions import MissingColumnError


def test_numeric_max_check_config_valid() -> None:
    """
    Validates that NumericMaxCheckConfig correctly instantiates a NumericMaxCheck
    when provided with valid configuration parameters.

    The created check should match the configured check_id, columns, and max_value.
    """
    # Arrange: Create a config with valid parameters
    config = NumericMaxCheckConfig(
        check_id="check_max_value",
        columns=["price", "discount"],
        max_value=100.0,
    )

    # Act: Convert config to check instance
    check = config.to_check()

    # Assert: Check instance has correct attributes
    assert isinstance(check, NumericMaxCheck)
    assert check.check_id == "check_max_value"
    assert check.columns == ["price", "discount"]
    assert check.max_value == 100.0


def test_numeric_max_check_validate_correctly_flags_rows(spark: SparkSession) -> None:
    """
    Verifies that NumericMaxCheck flags rows where the numeric column value exceeds max_value.

    A row is marked as failed if the 'price' column is greater than 100.0.
    """
    # Arrange: Create a DataFrame with some prices below, equal to, and above 100.0
    data = [Row(price=80.0), Row(price=100.0), Row(price=150.0)]  # 150.0 should fail
    df = spark.createDataFrame(data)

    config = NumericMaxCheckConfig(
        check_id="max_check",
        columns=["price"],
        max_value=100.0,
    )
    check = config.to_check()

    # Act: Apply the NumericMaxCheck
    result_df = check.validate(df)

    # Assert: Expect True where 'price' exceeds 100.0, otherwise False
    expected_df = spark.createDataFrame(
        [(80.0, False), (100.0, True), (150.0, True)],
        ["price", "max_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_numeric_max_check_inclusive(spark: SparkSession) -> None:
    """
    Verifies that NumericMaxCheck flags rows where the numeric value is strictly greater than max_value,
    when inclusive is set to True.
    """
    # Arrange
    data = [Row(value=1), Row(value=5), Row(value=10)]
    df = spark.createDataFrame(data)

    config = NumericMaxCheckConfig(check_id="max_inclusive", columns=["value"], max_value=5, inclusive=True)
    check = config.to_check()

    # Act
    result_df = check.validate(df)

    # Assert
    expected_df = spark.createDataFrame(
        [
            (1, False),
            (5, False),
            (10, True),
        ],
        ["value", "max_inclusive"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_numeric_max_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that NumericMaxCheck raises MissingColumnError if a specified column does not exist.

    The check should immediately fail at runtime when accessing a missing column.
    """
    # Arrange: DataFrame does not contain the required 'missing' column
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    config = NumericMaxCheckConfig(
        check_id="check_missing_column",
        columns=["missing"],
        max_value=100.0,
    )
    check = config.to_check()

    # Act & Assert: Expect MissingColumnError when the column is not present
    with pytest.raises(MissingColumnError):
        check.validate(df)


@pytest.mark.parametrize(
    "max_value_type,max_value",
    [
        ("int", 100),
        ("float", 100.0),
        ("decimal", Decimal("100.0")),
    ],
)
def test_numeric_max_check_with_different_numeric_types(
    spark: SparkSession, max_value_type: str, max_value
) -> None:
    """
    Validates that NumericMaxCheck correctly handles different numeric types
    (int, float, Decimal) as max_value.

    Ensures consistent behavior across type variations that may occur through YAML configuration.
    """
    # Arrange: Create test DataFrame
    data = [Row(price=80.0), Row(price=150.0)]  # 150.0 should fail
    df = spark.createDataFrame(data)

    config = NumericMaxCheckConfig(
        check_id=f"max_check_{max_value_type}",
        columns=["price"],
        max_value=max_value,
    )
    check = config.to_check()

    # Act: Apply the NumericMaxCheck
    result_df = check.validate(df)
    result = result_df.select(f"max_check_{max_value_type}").rdd.flatMap(lambda x: x).collect()

    # Assert: Expect the second row to fail regardless of max_value type
    assert result == [False, True], f"Failed for max_value type: {max_value_type}"
