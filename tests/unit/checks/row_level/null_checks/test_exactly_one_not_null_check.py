import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.null_checks.exactly_one_not_null_check import (
    ExactlyOneNotNullCheck,
    ExactlyOneNotNullCheckConfig,
)
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError


def test_exactly_one_not_null_check_valid_rows(spark: SparkSession) -> None:
    """
    Validates that ExactlyOneNotNullCheck passes rows where exactly one value is non-null.

    Given multiple columns, only one of them should be non-null per row for the check to pass.
    """
    # Arrange
    df = spark.createDataFrame(
        [
            (None, "user@example.com", None),
            ("0151", None, None),
            (None, None, 1234),
        ],
        ["phone", "email", "user_id"],
    )
    check = ExactlyOneNotNullCheck(check_id="exactly_one", columns=["phone", "email", "user_id"])

    # Act
    result_df = check.validate(df)

    # Assert
    expected_df = df.withColumn("exactly_one", F.lit(False))
    assertDataFrameEqual(result_df, expected_df)


def test_exactly_one_not_null_check_invalid_rows(spark: SparkSession) -> None:
    """
    Validates that ExactlyOneNotNullCheck flags rows with zero or more than one non-null values.

    A row fails the check if none or more than one of the specified columns are non-null.
    """
    # Arrange
    df = spark.createDataFrame(
        [
            (None, None, None),  # 0 non-null
            ("0151", "user@example.com", None),  # 2 non-null
            ("0151", "user@example.com", 1234),  # 3 non-null
        ],
        ["phone", "email", "user_id"],
    )
    check = ExactlyOneNotNullCheck(check_id="exactly_one", columns=["phone", "email", "user_id"])

    # Act
    result_df = check.validate(df)

    # Assert
    expected_flags = [True, True, True]
    actual_flags = [row["exactly_one"] for row in result_df.collect()]
    assert actual_flags == expected_flags


def test_exactly_one_not_null_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that ExactlyOneNotNullCheck raises MissingColumnError if any column is not present.

    This protects against configuration errors referencing invalid columns.
    """
    # Arrange
    df = spark.createDataFrame([("abc",)], ["only_col"])
    check = ExactlyOneNotNullCheck(check_id="check_missing", columns=["only_col", "missing_col"])

    # Act & Assert
    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_exactly_one_not_null_check_config_instantiation() -> None:
    """
    Validates that ExactlyOneNotNullCheckConfig correctly creates an ExactlyOneNotNullCheck instance.
    """
    # Arrange
    config = ExactlyOneNotNullCheckConfig(
        check_id="config_test", columns=["email", "phone"], severity=Severity.WARNING
    )

    # Act
    check = config.to_check()

    # Assert
    assert isinstance(check, ExactlyOneNotNullCheck)
    assert check.columns == ["email", "phone"]
    assert check.check_id == "config_test"
    assert check.severity == Severity.WARNING
