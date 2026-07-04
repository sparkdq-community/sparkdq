---
wide: true
---

# Is Not Contained In Check

**Check name**: `is-not-contained-in-check` · **Type**: row-level · **Config**: `IsNotContainedInCheckConfig`

Flags records whose values fall inside a set of forbidden values. Use it as a
blacklist — for example to reject deprecated status codes or known-bad
identifiers.

## Parameters

| Parameter          | Type              | Required | Default    | Description                                                                      |
| ------------------ | ----------------- | -------- | ---------- | -------------------------------------------------------------------------------- |
| `check_id`         | `str`             | yes      | —          | Unique identifier for this check within the `CheckSet`.                          |
| `forbidden_values` | `dict[str, list]` | yes      | —          | Mapping of column name → list of forbidden values. YAML key: `forbidden-values`. |
| `severity`         | `Severity`        | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                             |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import IsNotContainedInCheckConfig
    from sparkdq.core import Severity

    IsNotContainedInCheckConfig(
        check_id="no-deleted",
        forbidden_values={"status": ["DELETED"]},
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: is-not-contained-in-check
      check-id: no-deleted
      forbidden-values:
        status:
          - DELETED
      severity: critical
    ```

## Behavior

- **One mapping, one or many columns.** `forbidden_values` maps each column to
  its own list of disallowed values.
- **Multi-column reduction is AND.** With multiple columns, a row is flagged only
  when _every_ configured column holds a forbidden value. To fail a row if _any_
  single column hits a forbidden value, configure one check per column.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.

## Example

Rejecting `status == "DELETED"`, both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import IsNotContainedInCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "status": "ACTIVE"},
        {"id": 2, "status": "DELETED"},
    ])

    check_set = CheckSet().add_check(
        IsNotContainedInCheckConfig(check_id="no-deleted", forbidden_values={"status": ["DELETED"]})
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
        {"id": 1, "status": "ACTIVE"},
        {"id": 2, "status": "DELETED"},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the forbidden value fails:

```text
+---+-------+-----------------------------------------------+----------+--------------------------+
|id |status |_dq_errors                                     |_dq_passed|_dq_validation_ts         |
+---+-------+-----------------------------------------------+----------+--------------------------+
|2  |DELETED|[{IsNotContainedInCheck, no-deleted, critical}]|false     |2026-01-01 00:00:00.000000|
+---+-------+-----------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Reject deprecated or retired status codes.
- Block known-bad identifiers or placeholder values.
- Enforce a blacklist of disallowed categorical values.

## Related checks

- [Is Contained In Check](is_contained_in_check.md) — the inverse: restrict to a set of allowed values.

---

[← Row-Level Checks](../../row_level.md)
