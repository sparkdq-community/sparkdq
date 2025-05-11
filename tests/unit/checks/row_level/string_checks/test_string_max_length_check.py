import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.string_checks.max_length_check import (
    StringMaxLengthCheck,
    StringMaxLengthCheckConfig,
)
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError


def test_string_max_length_inclusive(spark: SparkSession) -> None:
    """
    Validates that StringMaxLengthCheck (inclusive=True) flags strings longer than the threshold.
    """
    df = spark.createDataFrame(
        [
            (1, "ABC"),  # pass
            (2, "ABCD"),  # fail
            (3, None),  # pass (null)
        ],
        ["id", "text"],
    )

    check = StringMaxLengthCheck(check_id="max_len_check", column="text", max_length=3, inclusive=True)
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "ABC", False),
            (2, "ABCD", True),
            (3, None, False),
        ],
        ["id", "text", "max_len_check"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_string_max_length_exclusive(spark: SparkSession) -> None:
    """
    Validates that StringMaxLengthCheck (inclusive=False) flags strings equal to or longer than the threshold.
    """
    df = spark.createDataFrame(
        [
            (1, "AB"),  # pass
            (2, "ABC"),  # fail
            (3, "ABCD"),  # fail
        ],
        ["id", "text"],
    )

    check = StringMaxLengthCheck(check_id="max_len_strict", column="text", max_length=3, inclusive=False)
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "AB", False),
            (2, "ABC", True),
            (3, "ABCD", True),
        ],
        ["id", "text", "max_len_strict"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_string_max_length_nulls_pass(spark: SparkSession) -> None:
    """
    Validates that null values are not considered failures in the max length check.
    """
    df = spark.createDataFrame(
        [
            (1, None),
            (2, "ABCD"),
            (3, "A"),
        ],
        ["id", "field"],
    )

    check = StringMaxLengthCheck(check_id="nulls_pass", column="field", max_length=3)
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, None, False),
            (2, "ABCD", True),
            (3, "A", False),
        ],
        ["id", "field", "nulls_pass"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_string_max_length_missing_column(spark: SparkSession) -> None:
    """
    Validates that a MissingColumnError is raised if the target column does not exist.
    """
    df = spark.createDataFrame([(1, "A")], ["id", "x"])
    check = StringMaxLengthCheck(check_id="missing_col", column="y", max_length=2)

    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_string_max_length_config_to_check() -> None:
    """
    Validates that the config class properly instantiates a StringMaxLengthCheck instance.
    """
    config = StringMaxLengthCheckConfig(
        check_id="config_test",
        column="description",
        max_length=10,
        inclusive=False,
        severity=Severity.WARNING,
    )
    check = config.to_check()

    assert isinstance(check, StringMaxLengthCheck)
    assert check.check_id == "config_test"
    assert check.column == "description"
    assert check.max_length == 10
    assert check.inclusive is False
    assert check.severity == Severity.WARNING


def test_string_max_length_config_invalid_threshold() -> None:
    """
    Validates that an error is raised when the configured max_length is <= 0.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        StringMaxLengthCheckConfig(check_id="fail_cfg", column="field", max_length=0)
