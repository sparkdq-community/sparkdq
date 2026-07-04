---
wide: true
---

# Columns Are Complete Check

**Check name**: `columns-are-complete-check` · **Type**: aggregate · **Config**: `ColumnsAreCompleteCheckConfig`

Validates that the configured columns contain **no** null values across the whole
dataset. Use it to assert dataset-wide completeness of mandatory fields.

## Parameters

| Parameter  | Type        | Required | Default    | Description                                               |
| ---------- | ----------- | -------- | ---------- | --------------------------------------------------------- |
| `check_id` | `str`       | yes      | —          | Unique identifier for this check within the `CheckSet`.   |
| `columns`  | `list[str]` | yes      | —          | Columns that must contain no nulls. YAML key: `columns`.  |
| `severity` | `Severity`  | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports. |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import ColumnsAreCompleteCheckConfig
    from sparkdq.core import Severity

    ColumnsAreCompleteCheckConfig(
        check_id="email-complete",
        columns=["email"],
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: columns-are-complete-check
      check-id: email-complete
      columns:
        - email
      severity: critical
    ```

## Behavior

- **Dataset-level verdict.** Produces a single pass/fail; the check fails if any
  configured column has at least one null anywhere in the dataset.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict reports `null_counts` per column and the list of `failed_columns`.
- **Difference from the row-level [Null Check](../null/null_check.md).** This
  check reports a dataset-wide verdict and per-column null counts; the row-level
  Null Check flags the individual offending rows.

## Example

Requiring `email` to have no nulls on a dataset where one is null, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import ColumnsAreCompleteCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": None},
        {"id": 3, "email": "c@example.com"},
        {"id": 4, "email": "d@example.com"},
    ])

    check_set = CheckSet().add_check(
        ColumnsAreCompleteCheckConfig(check_id="email-complete", columns=["email"])
    )
    result = BatchDQEngine(check_set).run_batch(df)

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

    df = spark.createDataFrame([
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": None},
        {"id": 3, "email": "c@example.com"},
        {"id": 4, "email": "d@example.com"},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

The aggregate result reports the failure, the null count, and the failed columns:

```text
email-complete False {'null_counts': {'email': 1}, 'failed_columns': ['email']}
```

## Typical use cases

- Assert that mandatory columns are fully populated before publishing a dataset.
- Detect upstream feeds that started emitting nulls in a required field.
- Gate downstream jobs on dataset-wide completeness.

## Related checks

- [Completeness Ratio Check](completeness_ratio_check.md) — tolerate a share of nulls instead of requiring zero.
- [Null Check](../null/null_check.md) — flag the individual rows that contain nulls.

---

[← Aggregate Checks](../../aggregate.md)
