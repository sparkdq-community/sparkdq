import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.string_checks.min_length_check import (
    StringMinLengthCheck,
    StringMinLengthCheckConfig,
)
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError, MissingColumnError


def test_string_min_length_inclusive(spark: SparkSession) -> None:
    """
    Validates that StringMinLengthCheck (inclusive=True) flags strings shorter than the threshold.
    """
    df = spark.createDataFrame(
        [
            (1, "ABC"),  # pass
            (2, "AB"),  # fail
            (3, None),  # pass (nulls allowed)
        ],
        ["id", "text"],
    )

    check = StringMinLengthCheck(check_id="min_len_check", column="text", min_length=3, inclusive=True)
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "ABC", False),
            (2, "AB", True),
            (3, None, False),
        ],
        ["id", "text", "min_len_check"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_string_min_length_exclusive(spark: SparkSession) -> None:
    """
    Validates that StringMinLengthCheck (inclusive=False) requires strictly greater length.
    """
    df = spark.createDataFrame(
        [
            (1, "ABCD"),  # pass
            (2, "ABC"),  # fail (equal to threshold)
            (3, "AB"),  # fail
        ],
        ["id", "text"],
    )

    check = StringMinLengthCheck(check_id="min_len_strict", column="text", min_length=3, inclusive=False)
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "ABCD", False),
            (2, "ABC", True),
            (3, "AB", True),
        ],
        ["id", "text", "min_len_strict"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_string_min_length_nulls_pass(spark: SparkSession) -> None:
    """
    Validates that null values are skipped and treated as valid.
    """
    df = spark.createDataFrame(
        [
            (1, None),
            (2, "ABC"),
            (3, "A"),
        ],
        ["id", "field"],
    )

    check = StringMinLengthCheck(check_id="nulls_pass", column="field", min_length=2)
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, None, False),
            (2, "ABC", False),
            (3, "A", True),
        ],
        ["id", "field", "nulls_pass"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_string_min_length_missing_column(spark: SparkSession) -> None:
    """
    Validates that MissingColumnError is raised when the column does not exist.
    """
    df = spark.createDataFrame([(1, "A")], ["id", "x"])
    check = StringMinLengthCheck(check_id="missing_col", column="y", min_length=2)

    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_string_min_length_config_to_check() -> None:
    """
    Validates that the config correctly instantiates the check with all parameters.
    """
    config = StringMinLengthCheckConfig(
        check_id="config_test", column="description", min_length=5, inclusive=False, severity=Severity.WARNING
    )

    check = config.to_check()
    assert isinstance(check, StringMinLengthCheck)
    assert check.check_id == "config_test"
    assert check.column == "description"
    assert check.min_length == 5
    assert check.inclusive is False
    assert check.severity == Severity.WARNING


def test_string_min_length_config_invalid_threshold() -> None:
    """
    Validates that config raises an error when min_length is <= 0.
    """
    with pytest.raises(InvalidCheckConfigurationError):
        StringMinLengthCheckConfig(check_id="fail_cfg", column="field", min_length=0)
