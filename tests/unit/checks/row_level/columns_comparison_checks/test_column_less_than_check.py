import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.columns_comparison_checks.column_less_than import (
    ColumnLessThanCheck,
    ColumnLessThanCheckConfig,
)
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidSQLExpressionError, MissingColumnError


def test_column_less_than_strict(spark: SparkSession) -> None:
    """
    Validates that ColumnLessThanCheck flags rows where column >= limit (strict mode).
    A row is marked as failed if start >= end.
    """
    df = spark.createDataFrame(
        [
            (1, 10, 20),
            (2, 30, 30),
            (3, 50, 40),
        ],
        ["id", "start", "end"],
    )
    check = ColumnLessThanCheck(check_id="lt_check", column="start", limit="end")

    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, 10, 20, False),
            (2, 30, 30, True),
            (3, 50, 40, True),
        ],
        ["id", "start", "end", "lt_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_column_less_than_inclusive(spark: SparkSession) -> None:
    """
    Validates that ColumnLessThanCheck with inclusive=True allows equal values.
    A row is marked as failed if start > end.
    """
    df = spark.createDataFrame(
        [
            (1, 10, 20),
            (2, 30, 30),
            (3, 50, 40),
        ],
        ["id", "start", "end"],
    )
    check = ColumnLessThanCheck(check_id="lte_check", column="start", limit="end", inclusive=True)
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, 10, 20, False),
            (2, 30, 30, False),
            (3, 50, 40, True),
        ],
        ["id", "start", "end", "lte_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_column_less_than_null_values(spark: SparkSession) -> None:
    """
    Validates that ColumnLessThanCheck treats nulls in either column as failures.
    """
    df = spark.createDataFrame(
        [
            (1, None, 10),
            (2, 5, None),
            (3, None, None),
            (4, 1, 2),
        ],
        ["id", "start", "end"],
    )
    check = ColumnLessThanCheck(check_id="lt_null_check", column="start", limit="end")
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, None, 10, True),
            (2, 5, None, True),
            (3, None, None, True),
            (4, 1, 2, False),
        ],
        ["id", "start", "end", "lt_null_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_column_less_than_missing_column(spark: SparkSession) -> None:
    """
    Validates that ColumnLessThanCheck raises MissingColumnError if a required column is missing.
    """
    df = spark.createDataFrame(
        [(1, 10)],
        ["id", "end"],
    )
    check = ColumnLessThanCheck(check_id="missing_col", column="start", limit="end")
    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_column_less_than_check_config_to_check() -> None:
    """
    Validates that ColumnLessThanCheckConfig creates a correct ColumnLessThanCheck instance via to_check().
    """
    config = ColumnLessThanCheckConfig(
        check_id="config_check",
        column="a",
        limit="b",
        inclusive=True,
        severity=Severity.WARNING,
    )
    check = config.to_check()

    assert isinstance(check, ColumnLessThanCheck)
    assert check.check_id == "config_check"
    assert check.column == "a"
    assert check.limit == "b"
    assert check.inclusive is True
    assert check.severity == Severity.WARNING


def test_column_less_than_check_with_date_columns(spark: SparkSession) -> None:
    """
    Validates that ColumnLessThanCheck works correctly with DateType columns.
    Uses inclusive=True to allow equal dates.
    """
    df = (
        spark.createDataFrame(
            [
                (1, "2024-01-01", "2024-01-02"),
                (2, "2024-01-02", "2024-01-02"),
                (3, "2024-01-03", "2024-01-01"),
            ],
            ["id", "start_date", "end_date"],
        )
        .withColumn("start_date", F.col("start_date").cast("date"))
        .withColumn("end_date", F.col("end_date").cast("date"))
    )

    check = ColumnLessThanCheck(check_id="date_check", column="start_date", limit="end_date", inclusive=True)
    result_df = check.validate(df)

    expected_df = (
        spark.createDataFrame(
            [
                (1, "2024-01-01", "2024-01-02", False),
                (2, "2024-01-02", "2024-01-02", False),
                (3, "2024-01-03", "2024-01-01", True),
            ],
            ["id", "start_date", "end_date", "date_check"],
        )
        .withColumn("start_date", F.col("start_date").cast("date"))
        .withColumn("end_date", F.col("end_date").cast("date"))
    )

    assertDataFrameEqual(result_df, expected_df)


def test_column_less_than_check_with_timestamp_columns(spark: SparkSession) -> None:
    """
    Validates that ColumnLessThanCheck works correctly with TimestampType columns.
    Uses inclusive=False to enforce strict ordering.
    """
    df = (
        spark.createDataFrame(
            [
                (1, "2024-01-01 08:00:00", "2024-01-01 09:00:00"),
                (2, "2024-01-01 09:00:00", "2024-01-01 09:00:00"),
                (3, "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            ],
            ["id", "pickup", "dropoff"],
        )
        .withColumn("pickup", F.col("pickup").cast("timestamp"))
        .withColumn("dropoff", F.col("dropoff").cast("timestamp"))
    )

    check = ColumnLessThanCheck(check_id="ts_check", column="pickup", limit="dropoff", inclusive=False)
    result_df = check.validate(df)

    expected_df = (
        spark.createDataFrame(
            [
                (1, "2024-01-01 08:00:00", "2024-01-01 09:00:00", False),
                (2, "2024-01-01 09:00:00", "2024-01-01 09:00:00", True),
                (3, "2024-01-01 10:00:00", "2024-01-01 09:00:00", True),
            ],
            ["id", "pickup", "dropoff", "ts_check"],
        )
        .withColumn("pickup", F.col("pickup").cast("timestamp"))
        .withColumn("dropoff", F.col("dropoff").cast("timestamp"))
    )

    assertDataFrameEqual(result_df, expected_df)


def test_column_less_than_check_with_limit_is_expression(spark: SparkSession) -> None:
    """
    Validates that ColumnLessThanCheck can handle expressions as limits.
    """

    df = spark.createDataFrame(
        [
            ("beginner", 95, 100),
            ("expert", 90, 100),
            ("beginner", 85, 100),
        ],
        ["level", "user_score", "max_score"],
    )

    check = ColumnLessThanCheck(
        check_id="expression_check",
        column="user_score",
        limit="CASE WHEN level='expert' THEN max_score ELSE max_score * 0.9 END",
    )
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            ("beginner", 95, 100, True),  # Should pass: 95 >= 90 (100 * 0.9)
            ("expert", 90, 100, False),  # Should fail: 90 > 100
            ("beginner", 85, 100, False),  # Should fail: 85 >= 90 (100 * 0.9)
        ],
        ["level", "user_score", "max_score", "expression_check"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_column_less_than_check_with_invalid_limit(spark: SparkSession) -> None:
    """
    Validates that ColumnLessThanCheck raises an error if the limit is not a valid column or expression.
    """
    df = spark.createDataFrame(
        [(1, 10)],
        ["id", "start"],
    )

    check_missing_col = ColumnLessThanCheck(check_id="missing_col", column="id", limit="non_existent_column")

    check_malformed_syntax = ColumnLessThanCheck(check_id="malformed_syntax", column="id", limit="start ++3")

    check_dangerous_expr = ColumnLessThanCheck(
        check_id="dangerous_expr",
        column="id",
        limit="drop table test_table",
    )

    with pytest.raises(InvalidSQLExpressionError):
        check_missing_col.validate(df)
        check_malformed_syntax.validate(df)
        check_dangerous_expr.validate(df)
