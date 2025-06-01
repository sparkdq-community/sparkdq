import pytest
from pyspark.sql import DataFrame, SparkSession

from sparkdq.core.base_check import IntegrityCheckMixin, ReferenceDatasetDict
from sparkdq.exceptions import MissingReferenceDatasetError


class DummyCheck(IntegrityCheckMixin):
    """Minimal concrete implementation for testing the mixin."""

    pass


@pytest.fixture
def sample_datasets(spark: SparkSession) -> ReferenceDatasetDict:
    df_customers = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["customer_id", "name"])
    df_products = spark.createDataFrame([(101, "Widget"), (102, "Gadget")], ["product_id", "name"])
    return {"customers": df_customers, "products": df_products}


def test_inject_reference_datasets_sets_internal_state(sample_datasets: ReferenceDatasetDict) -> None:
    """
    Ensures that the injected reference datasets are stored internally.
    """
    # Arrange
    check = DummyCheck()

    # Act
    check.inject_reference_datasets(sample_datasets)

    # Assert
    assert check._reference_datasets == sample_datasets


def test_get_reference_df_returns_expected_dataframe(sample_datasets: ReferenceDatasetDict) -> None:
    """
    Ensures that get_reference_df returns the correct DataFrame for a valid reference name.
    """
    # Arrange
    check = DummyCheck()
    check.inject_reference_datasets(sample_datasets)

    # Act
    ref_df = check.get_reference_df("customers")

    # Assert
    assert isinstance(ref_df, DataFrame)
    assert ref_df.columns == ["customer_id", "name"]


def test_get_reference_df_raises_error_for_missing_dataset() -> None:
    """
    Ensures that get_reference_df raises MissingReferenceDatasetError
    if the reference name is not available.
    """
    # Arrange
    check = DummyCheck()
    check.inject_reference_datasets({})  # no datasets injected

    # Act & Assert
    with pytest.raises(MissingReferenceDatasetError) as exc_info:
        check.get_reference_df("nonexistent")

    assert "nonexistent" in str(exc_info.value)
