import pytest
from pyspark.sql import Row

from sparkdq.checks.row_level.integrity_checks.foreign_key import ForeignKeyCheck
from sparkdq.exceptions import MissingColumnError, MissingReferenceDatasetError


def test_foreign_key_check_valid_matches(spark):
    """
    Validates that ForeignKeyCheck passes when all values in the source column
    exist in the reference dataset.
    """
    # Arrange
    df = spark.createDataFrame([Row(id=1), Row(id=2), Row(id=3)])
    ref_df = spark.createDataFrame([Row(customer_id=1), Row(customer_id=2), Row(customer_id=3)])

    check = ForeignKeyCheck(
        check_id="fk_check", column="id", reference_dataset="customers", reference_column="customer_id"
    )
    check.inject_reference_datasets({"customers": ref_df})

    # Act
    result_df = check.validate(df)
    result = [row["fk_check"] for row in result_df.collect()]

    # Assert
    assert result == [False, False, False]


def test_foreign_key_check_with_missing_values(spark):
    """
    Validates that ForeignKeyCheck flags rows where values in the source column
    do not exist in the reference dataset.
    """
    # Arrange
    df = spark.createDataFrame([Row(id=1), Row(id=99)])
    ref_df = spark.createDataFrame([Row(customer_id=1), Row(customer_id=2)])

    check = ForeignKeyCheck(
        check_id="fk_check", column="id", reference_dataset="customers", reference_column="customer_id"
    )
    check.inject_reference_datasets({"customers": ref_df})

    # Act
    result_df = check.validate(df)
    result = {row.id: row.fk_check for row in result_df.collect()}

    # Assert
    assert result[1] is False
    assert result[99] is True


def test_foreign_key_check_missing_reference_dataset(spark):
    """
    Validates that ForeignKeyCheck raises an error when the required reference dataset is missing.
    """
    # Arrange
    df = spark.createDataFrame([Row(id=1)])
    check = ForeignKeyCheck(
        check_id="fk_check", column="id", reference_dataset="missing_ds", reference_column="customer_id"
    )

    # Act & Assert
    with pytest.raises(MissingReferenceDatasetError):
        check.validate(df)


def test_foreign_key_check_missing_column(spark):
    """
    Validates that ForeignKeyCheck raises an error if the specified column is not in the DataFrame.
    """
    # Arrange
    df = spark.createDataFrame([Row(something_else=1)])
    ref_df = spark.createDataFrame([Row(customer_id=1)])

    check = ForeignKeyCheck(
        check_id="fk_check", column="id", reference_dataset="customers", reference_column="customer_id"
    )
    check.inject_reference_datasets({"customers": ref_df})

    # Act & Assert
    with pytest.raises(MissingColumnError):
        check.validate(df)
