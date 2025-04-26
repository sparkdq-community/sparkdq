"""
Tests for row-level check behavior based on BaseRowCheck.

This module includes a dummy implementation of a row-level check
that marks rows as invalid based on simple string logic.

It validates that:
- A concrete row check can add an error column to a DataFrame
- Only the expected rows are marked
- The column naming follows the check's convention

This test serves as a functional reference for implementing new row-level checks.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when

from sparkdq.core.base_check import BaseRowCheck
from sparkdq.core.severity import Severity


class DummyRowCheck(BaseRowCheck):
    """
    Dummy row-level check that flags rows where the 'value' column equals 'invalid'.
    """

    def __init__(self, column: str, severity: Severity = Severity.CRITICAL):
        super().__init__(severity)
        self.column = column

    def validate(self, df: DataFrame) -> DataFrame:
        error_col = self.error_column()
        return df.withColumn(
            error_col, when(col(self.column) == lit("invalid"), lit(True)).otherwise(lit(False))
        )

    def error_column(self) -> str:
        return f"{self.column}_is_invalid"


def test_dummy_row_check_flags_invalid_rows(spark: SparkSession) -> None:
    """
    Validates that a concrete row-level check implementation can flag invalid rows
    using a custom error column.

    Given a DataFrame with valid and invalid values,
    the check should append a boolean error column indicating which rows failed.

    This test demonstrates how row-level checks mark data for downstream filtering.
    """
    # Arrange: Create a test DataFrame with valid and invalid rows
    df = spark.createDataFrame(
        [
            (1, "ok"),
            (2, "invalid"),
            (3, "ok"),
        ],
        ["id", "value"],
    )

    check = DummyRowCheck(column="value", severity=Severity.WARNING)

    # Act: Run the row-level validation
    validated_df = check.validate(df)
    results = validated_df.select("id", check.error_column()).collect()

    # Assert: Only row with 'invalid' should be flagged
    result_map = {row["id"]: row[check.error_column()] for row in results}

    assert result_map[1] is False
    assert result_map[2] is True
    assert result_map[3] is False
