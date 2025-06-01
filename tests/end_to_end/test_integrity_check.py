from sparkdq.checks import ForeignKeyCheckConfig
from sparkdq.core import Severity
from sparkdq.engine import BatchDQEngine
from sparkdq.management import CheckSet


def test_foreign_key_check_end_to_end(spark) -> None:
    """
    End-to-end test for the ForeignKeyCheck as an aggregate check.

    Validates that the check correctly detects invalid foreign key values
    and returns accurate aggregate-level metrics.
    """
    # Arrange: Input and reference datasets
    df_main = spark.createDataFrame(
        [
            (1, "A"),
            (2, "B"),
            (3, "C"),  # "C" does not exist in reference set
        ],
        ["id", "category"],
    )

    df_ref = spark.createDataFrame(
        [
            ("A", "test"),
            ("B", "test"),
        ],
        ["category", "test"],
    )

    reference_datasets = {"categories": df_ref}

    check_config = ForeignKeyCheckConfig(
        check_id="category_fk_check",
        column="category",
        reference_dataset="categories",
        reference_column="category",
        severity=Severity.CRITICAL,
    )

    engine = BatchDQEngine(CheckSet().add_check(check_config))

    # Act
    result = engine.run_batch(df_main, reference_datasets)

    # Assert: The result should contain a failed aggregate check
    agg_result = next(r for r in result.aggregate_results if r.check_id == "category_fk_check")
    assert agg_result.passed is False
    assert agg_result.metrics["missing_foreign_keys"] == 1
    assert agg_result.metrics["total_rows"] == 3
