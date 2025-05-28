import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from sparkdq.checks.row_level.columns_comparison_checks.column_greater_than import (
    ColumnGreaterThanCheck,
    ColumnGreaterThanCheckConfig,
)
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidSQLExpressionError, MissingColumnError


def test_column_greater_than_strict(spark: SparkSession) -> None:
    """
    Validates that ColumnGreaterThanCheck flags rows where column <= limit (strict mode).
    A row is marked as failed if end <= start.
    """
    df = spark.createDataFrame(
        [
            (1, 20, 10),
            (2, 30, 30),
            (3, 40, 50),
        ],
        ["id", "end", "start"],
    )
    check = ColumnGreaterThanCheck(check_id="gt_check", column="end", limit="start")

    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, 20, 10, False),
            (2, 30, 30, True),
            (3, 40, 50, True),
        ],
        ["id", "end", "start", "gt_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_column_greater_than_inclusive(spark: SparkSession) -> None:
    """
    Validates that ColumnGreaterThanCheck with inclusive=True allows equal values.
    A row is marked as failed if end < start.
    """
    df = spark.createDataFrame(
        [
            (1, 20, 10),
            (2, 30, 30),
            (3, 40, 50),
        ],
        ["id", "end", "start"],
    )
    check = ColumnGreaterThanCheck(check_id="gte_check", column="end", limit="start", inclusive=True)
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, 20, 10, False),
            (2, 30, 30, False),
            (3, 40, 50, True),
        ],
        ["id", "end", "start", "gte_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_column_greater_than_null_values(spark: SparkSession) -> None:
    """
    Validates that ColumnGreaterThanCheck treats nulls in either column as failures.
    """
    df = spark.createDataFrame(
        [
            (1, None, 10),
            (2, 15, None),
            (3, None, None),
            (4, 12, 1),
        ],
        ["id", "end", "start"],
    )
    check = ColumnGreaterThanCheck(check_id="gt_null_check", column="end", limit="start")
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            (1, None, 10, True),
            (2, 15, None, True),
            (3, None, None, True),
            (4, 12, 1, False),
        ],
        ["id", "end", "start", "gt_null_check"],
    )
    assertDataFrameEqual(result_df, expected_df)


def test_column_greater_than_missing_column(spark: SparkSession) -> None:
    """
    Validates that ColumnGreaterThanCheck raises MissingColumnError if a required column is missing.
    """
    df = spark.createDataFrame(
        [(1, 10)],
        ["id", "start"],
    )
    check = ColumnGreaterThanCheck(check_id="missing_col", column="end", limit="start")
    with pytest.raises(MissingColumnError):
        check.validate(df)


def test_column_greater_than_check_config_to_check() -> None:
    """
    Validates that ColumnGreaterThanCheckConfig creates a correct ColumnGreaterThanCheck instance
    via to_check().
    """
    config = ColumnGreaterThanCheckConfig(
        check_id="config_check",
        column="a",
        limit="b",
        inclusive=True,
        severity=Severity.WARNING,
    )
    check = config.to_check()

    assert isinstance(check, ColumnGreaterThanCheck)
    assert check.check_id == "config_check"
    assert check.column == "a"
    assert check.limit == "b"
    assert check.inclusive is True
    assert check.severity == Severity.WARNING


def test_column_greater_than_check_with_date_columns(spark: SparkSession) -> None:
    """
    Validates that ColumnGreaterThanCheck works correctly with DateType columns.
    Uses inclusive=True to allow equal dates.
    """
    df = (
        spark.createDataFrame(
            [
                (1, "2024-01-02", "2024-01-01"),
                (2, "2024-01-02", "2024-01-02"),
                (3, "2024-01-01", "2024-01-03"),
            ],
            ["id", "end_date", "start_date"],
        )
        .withColumn("end_date", F.col("end_date").cast("date"))
        .withColumn("start_date", F.col("start_date").cast("date"))
    )

    check = ColumnGreaterThanCheck(
        check_id="date_check", column="end_date", limit="start_date", inclusive=True
    )
    result_df = check.validate(df)

    expected_df = (
        spark.createDataFrame(
            [
                (1, "2024-01-02", "2024-01-01", False),
                (2, "2024-01-02", "2024-01-02", False),
                (3, "2024-01-01", "2024-01-03", True),
            ],
            ["id", "end_date", "start_date", "date_check"],
        )
        .withColumn("end_date", F.col("end_date").cast("date"))
        .withColumn("start_date", F.col("start_date").cast("date"))
    )

    assertDataFrameEqual(result_df, expected_df)


def test_column_greater_than_check_with_timestamp_columns(spark: SparkSession) -> None:
    """
    Validates that ColumnGreaterThanCheck works correctly with TimestampType columns.
    Uses inclusive=False to enforce strict ordering.
    """
    df = (
        spark.createDataFrame(
            [
                (1, "2024-01-01 09:00:00", "2024-01-01 08:00:00"),
                (2, "2024-01-01 09:00:00", "2024-01-01 09:00:00"),
                (3, "2024-01-01 09:00:00", "2024-01-01 10:00:00"),
            ],
            ["id", "dropoff", "pickup"],
        )
        .withColumn("dropoff", F.col("dropoff").cast("timestamp"))
        .withColumn("pickup", F.col("pickup").cast("timestamp"))
    )

    check = ColumnGreaterThanCheck(check_id="ts_check", column="dropoff", limit="pickup", inclusive=False)
    result_df = check.validate(df)

    expected_df = (
        spark.createDataFrame(
            [
                (1, "2024-01-01 09:00:00", "2024-01-01 08:00:00", False),
                (2, "2024-01-01 09:00:00", "2024-01-01 09:00:00", True),
                (3, "2024-01-01 09:00:00", "2024-01-01 10:00:00", True),
            ],
            ["id", "dropoff", "pickup", "ts_check"],
        )
        .withColumn("dropoff", F.col("dropoff").cast("timestamp"))
        .withColumn("pickup", F.col("pickup").cast("timestamp"))
    )

    assertDataFrameEqual(result_df, expected_df)


def test_column_greater_than_check_with_limit_is_expression(spark: SparkSession) -> None:
    """
    Validates that ColumnGreaterThanCheck can handle expressions as limits.
    """

    df = spark.createDataFrame(
        [
            ("beginner", 95, 80),
            ("expert", 90, 80),
            ("beginner", 85, 80),
        ],
        ["level", "user_score", "min_score"],
    )

    check = ColumnGreaterThanCheck(
        check_id="expression_check",
        column="user_score",
        limit="CASE WHEN level='expert' THEN min_score ELSE min_score * 1.1 END",
    )
    result_df = check.validate(df)

    expected_df = spark.createDataFrame(
        [
            ("beginner", 95, 80, False),  # Should pass: 95 > 88 (80 * 1.1)
            ("expert", 90, 80, False),  # Should pass: 90 > 80
            ("beginner", 85, 80, True),  # Should fail: 85 <= 88 (80 * 1.1)
        ],
        ["level", "user_score", "min_score", "expression_check"],
    )

    assertDataFrameEqual(result_df, expected_df)


def test_column_greater_than_check_with_invalid_limit(spark: SparkSession) -> None:
    """
    Validates that ColumnGreaterThanCheck raises an error if the limit is not a valid column or expression.
    """
    df = spark.createDataFrame(
        [(1, 10)],
        ["id", "end"],
    )

    check_missing_col = ColumnGreaterThanCheck(
        check_id="missing_col", column="id", limit="non_existent_column"
    )

    check_malformed_syntax = ColumnGreaterThanCheck(check_id="malformed_syntax", column="id", limit="end ++3")

    check_dangerous_expr = ColumnGreaterThanCheck(
        check_id="dangerous_expr",
        column="id",
        limit="drop table test_table",
    )

    with pytest.raises(InvalidSQLExpressionError):
        check_missing_col.validate(df)
        check_malformed_syntax.validate(df)
        check_dangerous_expr.validate(df)
