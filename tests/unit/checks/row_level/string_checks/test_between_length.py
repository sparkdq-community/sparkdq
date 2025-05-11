import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.string_checks.between_length_check import (
    StringLengthBetweenCheck,
    StringLengthBetweenCheckConfig,
)
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError


def test_string_length_between_inclusive_bounds(spark: SparkSession) -> None:
    """
    Validates that values with length between min and max (inclusive) pass.
    """
    df = spark.createDataFrame(
        [
            (1, "ABC"),  # len=3, valid
            (2, "AB"),  # len=2, too short
            (3, "ABCDE"),  # len=5, valid
            (4, "ABCDEF"),  # len=6, too long
            (5, None),  # null, should pass
        ],
        ["id", "text"],
    )

    check = StringLengthBetweenCheck(
        check_id="between_check", column="text", min_length=3, max_length=5, inclusive=(True, True)
    )
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "ABC", False),
            (2, "AB", True),
            (3, "ABCDE", False),
            (4, "ABCDEF", True),
            (5, None, False),
        ],
        ["id", "text", "between_check"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_string_length_between_exclusive_bounds(spark: SparkSession) -> None:
    """
    Validates that exclusive bounds reject values equal to min or max length.
    """
    df = spark.createDataFrame(
        [
            (1, "ABC"),  # len=3, too short
            (2, "ABCD"),  # len=4, valid
            (3, "ABCDE"),  # len=5, too long
        ],
        ["id", "value"],
    )

    check = StringLengthBetweenCheck(
        check_id="between_strict", column="value", min_length=3, max_length=5, inclusive=(False, False)
    )
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "ABC", True),
            (2, "ABCD", False),
            (3, "ABCDE", True),
        ],
        ["id", "value", "between_strict"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_string_length_between_missing_column(spark: SparkSession) -> None:
    """
    Validates that MissingColumnError is raised when column is not in the DataFrame.
    """
    df = spark.createDataFrame([(1, "ABC")], ["id", "x"])
    check = StringLengthBetweenCheck(check_id="missing", column="y", min_length=1, max_length=5)
    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_string_length_between_config_to_check() -> None:
    """
    Validates that config correctly instantiates the check class.
    """
    config = StringLengthBetweenCheckConfig(
        check_id="config_test",
        column="my_col",
        min_length=2,
        max_length=10,
        inclusive=(False, True),
        severity=Severity.WARNING,
    )
    check = config.to_check()

    assert isinstance(check, StringLengthBetweenCheck)
    assert check.column == "my_col"
    assert check.min_length == 2
    assert check.max_length == 10
    assert check.inclusive == (False, True)
    assert check.severity == Severity.WARNING


def test_string_length_between_invalid_range() -> None:
    """
    Validates that invalid min > max raises configuration error.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        StringLengthBetweenCheckConfig(check_id="invalid_config", column="test", min_length=5, max_length=3)


def test_string_length_between_invalid_min_length() -> None:
    """
    Validates that min_length <= 0 raises configuration error.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        StringLengthBetweenCheckConfig(check_id="invalid_min", column="test", min_length=0, max_length=10)
