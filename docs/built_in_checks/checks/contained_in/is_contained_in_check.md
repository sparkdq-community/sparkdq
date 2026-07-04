---
wide: true
---

# Is Contained In Check

**Check name**: `is-contained-in-check` · **Type**: row-level · **Config**: `IsContainedInCheckConfig`

Flags records whose values fall outside a predefined set of allowed values. Use
it to enforce domain constraints and catch unexpected categorical values, working
with any equality-comparable type (strings, integers, dates, …).

## Parameters

| Parameter        | Type              | Required | Default    | Description                                                                  |
| ---------------- | ----------------- | -------- | ---------- | ---------------------------------------------------------------------------- |
| `check_id`       | `str`             | yes      | —          | Unique identifier for this check within the `CheckSet`.                      |
| `allowed_values` | `dict[str, list]` | yes      | —          | Mapping of column name → list of allowed values. YAML key: `allowed-values`. |
| `severity`       | `Severity`        | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                         |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import IsContainedInCheckConfig
    from sparkdq.core import Severity

    IsContainedInCheckConfig(
        check_id="valid-status",
        allowed_values={"status": ["A", "B"]},
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: is-contained-in-check
      check-id: valid-status
      allowed-values:
        status:
          - A
          - B
      severity: critical
    ```

## Behavior

- **One mapping, one or many columns.** `allowed_values` maps each column to its
  own list of permitted values, so a single check can guard several columns at
  once (e.g. `status` and `country`).
- **Multi-column reduction is AND.** With multiple columns, a row is flagged only
  when _every_ configured column holds a disallowed value. To fail a row if _any_
  single column is out of range, configure one check per column.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.

## Example

Restricting `status` to `{A, B}`, both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import IsContainedInCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "status": "A"},
        {"id": 2, "status": "X"},
        {"id": 3, "status": None},
    ])

    check_set = CheckSet().add_check(
        IsContainedInCheckConfig(check_id="valid-status", allowed_values={"status": ["A", "B"]})
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
        {"id": 1, "status": "A"},
        {"id": 2, "status": "X"},
        {"id": 3, "status": None},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the disallowed value fails:

```text
+---+------+----------------------------------------------+----------+--------------------------+
|id |status|_dq_errors                                    |_dq_passed|_dq_validation_ts         |
+---+------+----------------------------------------------+----------+--------------------------+
|2  |X     |[{IsContainedInCheck, valid-status, critical}]|false     |2026-01-01 00:00:00.000000|
+---+------+----------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Validate that status fields hold only permitted operational states.
- Ensure country or region codes belong to a known reference set.
- Detect ingestion errors introducing unrecognized categorical values.

## Related checks

- [Is Not Contained In Check](is_not_contained_in_check.md) — the inverse: reject a set of forbidden values.

---

[← Row-Level Checks](../../row_level.md)
