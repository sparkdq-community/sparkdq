---
wide: true
---

# Foreign Key Check

**Check name**: `foreign-key-check` · **Type**: aggregate (integrity) · **Config**: `ForeignKeyCheckConfig`

Validates referential integrity: every value in a source column must exist in a
column of a **reference dataset**. Use it to enforce foreign-key relationships
between a fact table and its dimensions.

## Parameters

| Parameter           | Type       | Required | Default    | Description                                                                             |
| ------------------- | ---------- | -------- | ---------- | --------------------------------------------------------------------------------------- |
| `check_id`          | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.                                 |
| `column`            | `str`      | yes      | —          | The source column to validate.                                                          |
| `reference_dataset` | `str`      | yes      | —          | Name of the reference dataset. YAML key: `reference-dataset`.                           |
| `reference_column`  | `str`      | yes      | —          | The column in the reference dataset holding valid values. YAML key: `reference-column`. |
| `severity`          | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports.                               |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import ForeignKeyCheckConfig
    from sparkdq.core import Severity

    ForeignKeyCheckConfig(
        check_id="customer-resolvable",
        column="customer_id",
        reference_dataset="customers",
        reference_column="cid",
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: foreign-key-check
      check-id: customer-resolvable
      column: customer_id
      reference-dataset: customers
      reference-column: cid
      severity: critical
    ```

## Behavior

- **Reference datasets are passed to `run_batch`.** The named dataset must be
  supplied via `run_batch(df, reference_datasets={"customers": customers_df})`;
  the name must match `reference_dataset`. A missing name raises
  `MissingReferenceDatasetError`.
- **Dataset-level verdict.** The check fails if any source value cannot be
  resolved. Because it is a `CRITICAL` aggregate by default, a failure marks
  **every** row `_dq_passed = False` — including rows whose own key was valid.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict reports `missing_foreign_keys`, `total_rows`, and `missing_ratio`.

## Example

Validating `customer_id` against a `customers` reference, where one value is
unresolved.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import ForeignKeyCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    orders = spark.createDataFrame([
        {"id": 1, "customer_id": 10},
        {"id": 2, "customer_id": 99},
    ])
    customers = spark.createDataFrame([{"cid": 10}, {"cid": 20}])

    check_set = CheckSet().add_check(
        ForeignKeyCheckConfig(
            check_id="customer-resolvable",
            column="customer_id",
            reference_dataset="customers",
            reference_column="cid",
        )
    )
    result = BatchDQEngine(check_set).run_batch(
        orders, reference_datasets={"customers": customers}
    )

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

=== "YAML"

    ```python
    import yaml
    from pyspark.sql import SparkSession
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    orders = spark.createDataFrame([
        {"id": 1, "customer_id": 10},
        {"id": 2, "customer_id": 99},
    ])
    customers = spark.createDataFrame([{"cid": 10}, {"cid": 20}])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(
        orders, reference_datasets={"customers": customers}
    )

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

The aggregate result reports the unresolved count and ratio:

```text
customer-resolvable False {'missing_foreign_keys': 1, 'total_rows': 2, 'missing_ratio': 0.5}
```

## Typical use cases

- Ensure every `order.customer_id` resolves to a known customer.
- Validate fact-table foreign keys against dimension tables.
- Enforce referential integrity across datasets in a lake or warehouse.

## Related checks

- [Is Contained In Check](../contained_in/is_contained_in_check.md) — restrict values to a static in-config set rather than a reference dataset.

---

[← Aggregate Checks](../../aggregate.md)
