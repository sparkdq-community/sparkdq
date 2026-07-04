---
wide: true
---

# Null Check

**Check name**: `null-check` · **Type**: row-level · **Config**: `NullCheckConfig`

Flags any record that contains a null value in one or more of the configured
columns. Use it to enforce completeness of mandatory fields and keep incomplete
records out of downstream processing.

## Parameters

| Parameter  | Type        | Required | Default    | Description                                             |
| ---------- | ----------- | -------- | ---------- | ------------------------------------------------------- |
| `check_id` | `str`       | yes      | —          | Unique identifier for this check within the `CheckSet`. |
| `columns`  | `list[str]` | yes      | —          | Columns that must be non-null. YAML key: `columns`.     |
| `severity` | `Severity`  | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.    |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import NullCheckConfig
    from sparkdq.core import Severity

    NullCheckConfig(
        check_id="no-null-email",
        columns=["email"],
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: null-check
      check-id: no-null-email
      columns:
        - email
      severity: critical
    ```

## Behavior

- **OR semantics across columns.** A record fails if _any_ of the listed columns
  is null. To require each column independently with its own severity or error
  id, define one `null-check` per column instead.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`;
  passing rows are unaffected. With the default severity, a failure sets
  `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist in the
  DataFrame, the check raises `MissingColumnError` at validation time rather than
  silently passing.

## Example

Given a DataFrame with one null `email`, both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import NullCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": None},
        {"id": 3, "email": "c@example.com"},
    ])

    check_set = CheckSet().add_check(
        NullCheckConfig(check_id="no-null-email", columns=["email"])
    )
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
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
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

The failing row is returned with its error metadata:

```text
+-----+---+--------------------------------------+----------+--------------------------+
|email|id |_dq_errors                            |_dq_passed|_dq_validation_ts         |
+-----+---+--------------------------------------+----------+--------------------------+
|NULL |2  |[{NullCheck, no-null-email, critical}]|false     |2026-01-01 00:00:00.000000|
+-----+---+--------------------------------------+----------+--------------------------+
```

## Typical use cases

- Enforce completeness of primary keys, foreign keys, and business-critical
  attributes.
- Prevent incomplete records from reaching downstream transformations or reports.
- Detect data gaps introduced by upstream extraction or ingestion failures.

## Related checks

- [Not Null Check](not_null_check.md) — the inverse assertion for columns that
  are expected to be null.
- [Exactly One Not Null Check](exactly_one_not_null_check.md) — require exactly
  one of several columns to be populated.
- [Completeness Ratio Check](../completeness/completeness_ratio_check.md) —
  dataset-level tolerance for a share of nulls instead of a hard per-row rule.

---

[← Row-Level Checks](../../row_level.md)
