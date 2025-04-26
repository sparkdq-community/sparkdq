import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.null_checks.not_null_check import NotNullCheck, NotNullCheckConfig
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError


def test_not_null_check_single_column(spark: SparkSession) -> None:
    """
    Validates that NotNullCheck flags rows where a single column contains a value.

    A row is marked as failed if 'deactivated_at' is NOT null.
    """
    # Arrange: DataFrame with null and non-null values in 'deactivated_at'
    df = spark.createDataFrame([(1, None), (2, "2023-01-01")], ["id", "deactivated_at"])
    check = NotNullCheck(check_id="not_null_check", columns=["deactivated_at"])

    # Act: Apply the check
    result_df = check.validate(df)

    # Assert: Only rows with values in 'deactivated_at' should fail
    expected_df = spark.createDataFrame(
        [(1, None, False), (2, "2023-01-01", True)], ["id", "deactivated_at", "not_null_check"]
    )
    assertDataFrameEqual(result_df, expected_df)


def test_not_null_check_multiple_columns(spark: SparkSession) -> None:
    """
    Validates that NotNullCheck fails if any of the given columns contain a value.

    A row is marked as failed if at least one of the specified columns is not null.
    """
    # Arrange: DataFrame with multiple control columns
    df = spark.createDataFrame(
        [
            (1, None, None),
            (2, "X", None),
            (3, None, "Y"),
            (4, "X", "Y"),
        ],
        ["id", "flag_a", "flag_b"],
    )
    check = NotNullCheck(check_id="control_flags", columns=["flag_a", "flag_b"])

    # Act: Apply the check
    result_df = check.validate(df)

    # Assert: Any non-null value in flag_a or flag_b should fail the check
    expected_df = spark.createDataFrame(
        [
            (1, None, None, False),
            (2, "X", None, True),
            (3, None, "Y", True),
            (4, "X", "Y", True),
        ],
        ["id", "flag_a", "flag_b", "control_flags"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_not_null_check_missing_column(spark: SparkSession) -> None:
    """
    Validates that NotNullCheck raises MissingColumnError if a specified column is missing.

    This ensures misconfiguration is caught early and explicitly.
    """
    # Arrange: DataFrame missing one of the target columns
    df = spark.createDataFrame([(1, 2)], ["id", "a"])
    check = NotNullCheck(check_id="check", columns=["a", "b"])

    # Act & Assert: MissingColumnError should be raised
    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_not_null_check_config_to_check_instantiates_correct_check() -> None:
    """
    Validates that NotNullCheckConfig creates a correct NotNullCheck instance via to_check().
    """
    # Arrange: Config with multiple columns
    config = NotNullCheckConfig(
        check_id="not_null_test", columns=["temp_field", "test_flag"], severity=Severity.WARNING
    )

    # Act: Convert config to check
    check = config.to_check()

    # Assert: Check is correctly instantiated
    assert isinstance(check, NotNullCheck)
    assert check.check_id == "not_null_test"
    assert check.columns == ["temp_field", "test_flag"]
    assert check.severity == Severity.WARNING
