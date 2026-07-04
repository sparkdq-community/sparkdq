---
wide: true
---

# Not Null Check

**Check name**: `not-null-check` · **Type**: row-level · **Config**: `NotNullCheckConfig`

Flags any record where one or more of the configured columns contains a value —
that is, it asserts the columns are expected to stay **null**. Use it for fields
that should remain unset under normal business conditions, such as a
`deleted_at` or `error_reason` that must be empty for active records.

## Parameters

| Parameter  | Type        | Required | Default    | Description                                             |
| ---------- | ----------- | -------- | ---------- | ------------------------------------------------------- |
| `check_id` | `str`       | yes      | —          | Unique identifier for this check within the `CheckSet`. |
| `columns`  | `list[str]` | yes      | —          | Columns that must remain null. YAML key: `columns`.     |
| `severity` | `Severity`  | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.    |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import NotNullCheckConfig
    from sparkdq.core import Severity

    NotNullCheckConfig(
        check_id="deleted-at-empty",
        columns=["deleted_at"],
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: not-null-check
      check-id: deleted-at-empty
      columns:
        - deleted_at
      severity: critical
    ```

## Behavior

- **OR semantics across columns.** A record fails if _any_ of the listed columns
  is non-null. It is the inverse of the [Null Check](null_check.md).
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`;
  passing rows are unaffected. With the default severity, a failure sets
  `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist in the
  DataFrame, the check raises `MissingColumnError` at validation time rather than
  silently passing.

## Example

Given a DataFrame where `deleted_at` should be empty for every row, both styles
produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import NotNullCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "deleted_at": None},
        {"id": 2, "deleted_at": "2026-01-05"},
        {"id": 3, "deleted_at": None},
    ])

    check_set = CheckSet().add_check(
        NotNullCheckConfig(check_id="deleted-at-empty", columns=["deleted_at"])
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
        {"id": 1, "deleted_at": None},
        {"id": 2, "deleted_at": "2026-01-05"},
        {"id": 3, "deleted_at": None},
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
+----------+---+--------------------------------------------+----------+--------------------------+
|deleted_at|id |_dq_errors                                  |_dq_passed|_dq_validation_ts         |
+----------+---+--------------------------------------------+----------+--------------------------+
|2026-01-05|2  |[{NotNullCheck, deleted-at-empty, critical}]|false     |2026-01-01 00:00:00.000000|
+----------+---+--------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Assert that lifecycle columns like `deleted_at` or `archived_at` are empty for
  records that should still be active.
- Enforce that an `error_reason` or `rejection_code` is null on successfully
  processed rows.
- Catch upstream logic that unexpectedly populates a field that should stay
  unset.

## Related checks

- [Null Check](null_check.md) — the inverse assertion for columns that must be
  populated.
- [Exactly One Not Null Check](exactly_one_not_null_check.md) — require exactly
  one of several columns to be populated.

---

[← Row-Level Checks](../../row_level.md)
