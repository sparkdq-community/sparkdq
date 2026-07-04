---
wide: true
---

# Timestamp Min Check

**Check name**: `timestamp-min-check` · **Type**: row-level · **Config**: `TimestampMinCheckConfig`

Flags any record whose value in one of the configured timestamp columns is earlier
than a minimum timestamp. Use it to reject events before a valid start instant.

## Parameters

| Parameter   | Type        | Required | Default    | Description                                                              |
| ----------- | ----------- | -------- | ---------- | ------------------------------------------------------------------------ |
| `check_id`  | `str`       | yes      | —          | Unique identifier for this check within the `CheckSet`.                  |
| `columns`   | `list[str]` | yes      | —          | Timestamp columns to compare against the threshold. YAML key: `columns`. |
| `min_value` | `str`       | yes      | —          | Lower bound as an ISO timestamp. YAML key: `min-value`.                  |
| `inclusive` | `bool`      | no       | `False`    | Whether `min_value` itself is allowed (see Behavior).                    |
| `severity`  | `Severity`  | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                     |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import TimestampMinCheckConfig
    from sparkdq.core import Severity

    TimestampMinCheckConfig(
        check_id="min-ts",
        columns=["ts"],
        min_value="2024-01-01 00:00:00",
        inclusive=True,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: timestamp-min-check
      check-id: min-ts
      columns:
        - ts
      min-value: "2024-01-01 00:00:00"
      inclusive: true
      severity: critical
    ```

## Behavior

- **`min_value` is an ISO timestamp string.** The column is cast to `timestamp`
  before comparison.
- **`inclusive` controls the boundary, and defaults to `False`.** With
  `inclusive=True`, a value equal to `min_value` **passes** (`value >= min_value`).
  With the default, the boundary instant itself **fails** (`value > min_value`).
- **OR semantics across columns.** With multiple columns, a record fails if
  _any_ of them is before the threshold.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist, the check
  raises `MissingColumnError` at validation time.

## Example

Requiring `ts >= 2024-01-01 00:00:00` (`inclusive=True`), both styles produce the
same result.

=== "Python"

    ```python
    import datetime
    from pyspark.sql import SparkSession
    from sparkdq.checks import TimestampMinCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "ts": datetime.datetime(2024, 6, 1, 12, 0, 0)},
        {"id": 2, "ts": datetime.datetime(2023, 1, 1, 0, 0, 0)},
    ])

    check_set = CheckSet().add_check(
        TimestampMinCheckConfig(
            check_id="min-ts", columns=["ts"], min_value="2024-01-01 00:00:00", inclusive=True
        )
    )
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

=== "YAML"

    ```python
    import datetime
    import yaml
    from pyspark.sql import SparkSession
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "ts": datetime.datetime(2024, 6, 1, 12, 0, 0)},
        {"id": 2, "ts": datetime.datetime(2023, 1, 1, 0, 0, 0)},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the too-early row fails:

```text
+---+-------------------+---------------------------------------+----------+--------------------------+
|id |ts                 |_dq_errors                             |_dq_passed|_dq_validation_ts         |
+---+-------------------+---------------------------------------+----------+--------------------------+
|2  |2023-01-01 00:00:00|[{TimestampMinCheck, min-ts, critical}]|false     |2026-01-01 00:00:00.000000|
+---+-------------------+---------------------------------------+----------+--------------------------+
```

## Typical use cases

- Reject events recorded before a valid start instant.
- Enforce a lower bound aligned with a system launch time.
- Detect clock or timezone errors producing out-of-range timestamps.

## Related checks

- [Timestamp Max Check](timestamp_max_check.md) — enforce an upper bound.
- [Timestamp Between Check](timestamp_between_check.md) — enforce both bounds at once.

---

[← Row-Level Checks](../../row_level.md)
