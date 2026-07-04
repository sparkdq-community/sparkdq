---
wide: true
---

# Timestamp Between Check

**Check name**: `timestamp-between-check` · **Type**: row-level · **Config**: `TimestampBetweenCheckConfig`

Flags any record whose value in one of the configured timestamp columns falls
outside a `[min, max]` range. Use it to constrain events to a valid time window.

## Parameters

| Parameter   | Type                | Required | Default          | Description                                                          |
| ----------- | ------------------- | -------- | ---------------- | -------------------------------------------------------------------- |
| `check_id`  | `str`               | yes      | —                | Unique identifier for this check within the `CheckSet`.              |
| `columns`   | `list[str]`         | yes      | —                | Timestamp columns to compare against the range. YAML key: `columns`. |
| `min_value` | `str`               | yes      | —                | Lower bound as an ISO timestamp. YAML key: `min-value`.              |
| `max_value` | `str`               | yes      | —                | Upper bound as an ISO timestamp. YAML key: `max-value`.              |
| `inclusive` | `tuple[bool, bool]` | no       | `(False, False)` | Inclusivity of the lower and upper bound respectively.               |
| `severity`  | `Severity`          | no       | `CRITICAL`       | `CRITICAL` fails the row; `WARNING` only records it.                 |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import TimestampBetweenCheckConfig
    from sparkdq.core import Severity

    TimestampBetweenCheckConfig(
        check_id="ts-2024",
        columns=["ts"],
        min_value="2024-01-01 00:00:00",
        max_value="2024-12-31 23:59:59",
        inclusive=(True, True),
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: timestamp-between-check
      check-id: ts-2024
      columns:
        - ts
      min-value: "2024-01-01 00:00:00"
      max-value: "2024-12-31 23:59:59"
      inclusive: [true, true]
      severity: critical
    ```

## Behavior

- **Bounds are ISO timestamp strings.** The column is cast to `timestamp` before
  comparison.
- **`inclusive` is a `(lower, upper)` pair and defaults to `(False, False)`** —
  i.e. strictly between. Each bound is controlled independently (`(True, True)`
  gives `min <= value <= max`).
- **OR semantics across columns.** With multiple columns, a record fails if
  _any_ of them lies outside the range.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist, the check
  raises `MissingColumnError` at validation time.

## Example

Requiring `ts` within calendar year 2024 (both bounds inclusive), both styles
produce the same result.

=== "Python"

    ```python
    import datetime
    from pyspark.sql import SparkSession
    from sparkdq.checks import TimestampBetweenCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "ts": datetime.datetime(2024, 6, 1, 12, 0, 0)},
        {"id": 2, "ts": datetime.datetime(2023, 1, 1, 0, 0, 0)},
    ])

    check_set = CheckSet().add_check(
        TimestampBetweenCheckConfig(
            check_id="ts-2024", columns=["ts"],
            min_value="2024-01-01 00:00:00", max_value="2024-12-31 23:59:59", inclusive=(True, True),
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

The out-of-range row fails; the in-range row passes:

```text
+---+-------------------+--------------------------------------------+----------+--------------------------+
|id |ts                 |_dq_errors                                  |_dq_passed|_dq_validation_ts         |
+---+-------------------+--------------------------------------------+----------+--------------------------+
|2  |2023-01-01 00:00:00|[{TimestampBetweenCheck, ts-2024, critical}]|false     |2026-01-01 00:00:00.000000|
+---+-------------------+--------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Constrain events to a valid time window.
- Reject records outside a reporting or fiscal period.
- Detect timestamp errors from upstream systems or migrations.

## Related checks

- [Timestamp Min Check](timestamp_min_check.md) — enforce only a lower bound.
- [Timestamp Max Check](timestamp_max_check.md) — enforce only an upper bound.

---

[← Row-Level Checks](../../row_level.md)
