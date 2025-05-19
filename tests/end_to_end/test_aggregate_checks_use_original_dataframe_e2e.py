from pyspark.sql import SparkSession

from sparkdq.checks import NullCheckConfig, SchemaCheckConfig
from sparkdq.engine import BatchDQEngine
from sparkdq.management import CheckSet


def test_aggregate_check_applies_to_original_dataframe(spark: SparkSession) -> None:
    """
    Validates that aggregate checks are executed on the original input DataFrame.

    This test ensures that the SchemaCheck (an aggregate-level check) is applied to the unmodified
    input DataFrame — not one that has already been transformed by previous row-level checks
    such as NullCheck.

    Given a DataFrame with no nulls in column 'id' and a matching schema,
    both NullCheck and SchemaCheck should pass, and summary.all_passed should return True.
    """
    # Arrange
    data = [("a", 1), ("b", 2), ("c", 3)]
    df = spark.createDataFrame(data, ["id", "value"])

    # Define NullCheck (row-level): checks that 'id' is not null
    null_check = NullCheckConfig(check_id="null-check", columns=["id"])

    # Define SchemaCheck (aggregate-level): checks schema matches expectation
    schema_check = SchemaCheckConfig(
        check_id="schema-check", expected_schema={"id": "string", "value": "bigint"}, strict=True
    )

    # Combine into CheckSet
    check_set = CheckSet()
    check_set.add_check(null_check)
    check_set.add_check(schema_check)

    # Run validation
    engine = BatchDQEngine(check_set=check_set)
    result = engine.run_batch(df)

    # Assert that validation ran on clean original data and both checks passed
    summary = result.summary()
    assert summary.all_passed is True
    assert summary.failed_records == 0
    assert summary.passed_records == df.count()
