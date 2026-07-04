---
wide: true
---

# Unique Rows Check

**Check name**: `unique-rows-check` · **Type**: aggregate · **Config**: `UniqueRowsCheckConfig`

Validates that the dataset contains no duplicate rows. Duplication can be checked
across the full row or a subset of key columns. Use it to enforce primary-key or
grain uniqueness.

## Parameters

| Parameter        | Type                | Required | Default    | Description                                                                        |
| ---------------- | ------------------- | -------- | ---------- | ---------------------------------------------------------------------------------- |
| `check_id`       | `str`               | yes      | —          | Unique identifier for this check within the `CheckSet`.                            |
| `subset_columns` | `list[str] \| None` | no       | `None`     | Columns defining uniqueness; `None` uses the full row. YAML key: `subset-columns`. |
| `severity`       | `Severity`          | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports.                          |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import UniqueRowsCheckConfig
    from sparkdq.core import Severity

    UniqueRowsCheckConfig(
        check_id="unique-email",
        subset_columns=["email"],
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: unique-rows-check
      check-id: unique-email
      subset-columns:
        - email
      severity: critical
    ```

## Behavior

- **Dataset-level verdict.** Fails if any duplicate exists across the chosen
  columns (or the full row when `subset_columns` is omitted).
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict reports the number of `duplicate_row_groups` and the `checked_columns`.

## Example

Requiring unique `email` on a dataset with duplicates, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import UniqueRowsCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "a@example.com"},
        {"id": 3, "email": "b@example.com"},
        {"id": 4, "email": "b@example.com"},
    ])

    check_set = CheckSet().add_check(
        UniqueRowsCheckConfig(check_id="unique-email", subset_columns=["email"])
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
        {"id": 2, "email": "a@example.com"},
        {"id": 3, "email": "b@example.com"},
        {"id": 4, "email": "b@example.com"},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

The aggregate result reports the failure and how many duplicate groups exist:

```text
unique-email False {'duplicate_row_groups': 2, 'checked_columns': ['email']}
```

## Typical use cases

- Enforce primary-key or business-key uniqueness.
- Detect duplicate ingestion or fan-out from a bad join.
- Validate the grain of a fact or dimension table.

## Related checks

- [Unique Ratio Check](unique_ratio_check.md) — tolerate some duplication via a ratio threshold.
- [Distinct Ratio Check](distinct_ratio_check.md) — measure the share of distinct values.

---

[← Aggregate Checks](../../aggregate.md)
