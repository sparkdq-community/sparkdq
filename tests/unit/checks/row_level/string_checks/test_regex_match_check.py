import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.string_checks.regex_match_check import (
    RegexMatchCheck,
    RegexMatchCheckConfig,
)
from sparkdq.core.severity import Severity
from sparkdq.exceptions import MissingColumnError


def test_regex_match_valid_and_invalid(spark: SparkSession) -> None:
    """
    Validates that strings matching the pattern pass and non-matching ones fail.
    """
    df = spark.createDataFrame(
        [
            (1, "test@example.com"),
            (2, "invalid_email"),
            (3, None),
        ],
        ["id", "email"],
    )

    check = RegexMatchCheck(
        check_id="email_check",
        column="email",
        pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
        ignore_case=False,
        treat_null_as_failure=False,
    )

    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "test@example.com", False),
            (2, "invalid_email", True),
            (3, None, False),
        ],
        ["id", "email", "email_check"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_regex_match_with_null_failure(spark: SparkSession) -> None:
    """
    Validates that nulls are treated as failures when treat_null_as_failure=True.
    """
    df = spark.createDataFrame(
        [
            (1, "abc"),
            (2, None),
        ],
        ["id", "col"],
    )

    check = RegexMatchCheck(
        check_id="match_null_fail", column="col", pattern=r"abc", treat_null_as_failure=True
    )

    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "abc", False),
            (2, None, True),
        ],
        ["id", "col", "match_null_fail"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_regex_match_ignore_case(spark: SparkSession) -> None:
    """
    Validates that case-insensitive matching works when ignore_case=True.
    """
    df = spark.createDataFrame(
        [
            (1, "Hello123"),
            (2, "hello123"),
            (3, "HELLO123"),
            (4, "no_match"),
        ],
        ["id", "val"],
    )

    check = RegexMatchCheck(check_id="case_check", column="val", pattern=r"hello123", ignore_case=True)

    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, "Hello123", False),
            (2, "hello123", False),
            (3, "HELLO123", False),
            (4, "no_match", True),
        ],
        ["id", "val", "case_check"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_regex_match_missing_column(spark: SparkSession) -> None:
    """
    Validates that a MissingColumnError is raised if the column does not exist.
    """
    df = spark.createDataFrame([(1, "abc")], ["id", "x"])
    check = RegexMatchCheck(check_id="missing_column", column="y", pattern=r".*")
    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_regex_match_config_to_check() -> None:
    """
    Validates that the RegexMatchCheckConfig correctly instantiates a RegexMatchCheck.
    """
    config = RegexMatchCheckConfig(
        check_id="email_test",
        column="email",
        pattern=r"^.+@.+\..+$",
        ignore_case=True,
        treat_null_as_failure=True,
        severity=Severity.WARNING,
    )

    check = config.to_check()

    assert isinstance(check, RegexMatchCheck)
    assert check.check_id == "email_test"
    assert check.column == "email"
    assert check.pattern == r"^.+@.+\..+$"
    assert check.ignore_case is True
    assert check.treat_null_as_failure is True
    assert check.severity == Severity.WARNING
