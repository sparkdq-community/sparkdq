---
wide: true
---

# Date Between Check

**Check name**: `date-between-check` · **Type**: row-level · **Config**: `DateBetweenCheckConfig`

Flags any record whose value in one of the configured date columns falls outside
a `[min, max]` date range. Use it to constrain dates to a valid business period.

## Parameters

| Parameter   | Type                | Required | Default          | Description                                                     |
| ----------- | ------------------- | -------- | ---------------- | --------------------------------------------------------------- |
| `check_id`  | `str`               | yes      | —                | Unique identifier for this check within the `CheckSet`.         |
| `columns`   | `list[str]`         | yes      | —                | Date columns to compare against the range. YAML key: `columns`. |
| `min_value` | `str`               | yes      | —                | Lower bound as `YYYY-MM-DD`. YAML key: `min-value`.             |
| `max_value` | `str`               | yes      | —                | Upper bound as `YYYY-MM-DD`. YAML key: `max-value`.             |
| `inclusive` | `tuple[bool, bool]` | no       | `(False, False)` | Inclusivity of the lower and upper bound respectively.          |
| `severity`  | `Severity`          | no       | `CRITICAL`       | `CRITICAL` fails the row; `WARNING` only records it.            |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import DateBetweenCheckConfig
    from sparkdq.core import Severity

    DateBetweenCheckConfig(
        check_id="date-2024",
        columns=["d"],
        min_value="2024-01-01",
        max_value="2024-12-31",
        inclusive=(True, True),
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: date-between-check
      check-id: date-2024
      columns:
        - d
      min-value: "2024-01-01"
      max-value: "2024-12-31"
      inclusive: [true, true]
      severity: critical
    ```

## Behavior

- **Bounds are `YYYY-MM-DD` strings.** The column is cast to `date` before
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

Requiring `d` within calendar year 2024 (both bounds inclusive), both styles
produce the same result.

=== "Python"

    ```python
    import datetime
    from pyspark.sql import SparkSession
    from sparkdq.checks import DateBetweenCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "d": datetime.date(2024, 6, 1)},
        {"id": 2, "d": datetime.date(2023, 1, 1)},
        {"id": 3, "d": datetime.date(2025, 1, 1)},
    ])

    check_set = CheckSet().add_check(
        DateBetweenCheckConfig(
            check_id="date-2024", columns=["d"],
            min_value="2024-01-01", max_value="2024-12-31", inclusive=(True, True),
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
        {"id": 1, "d": datetime.date(2024, 6, 1)},
        {"id": 2, "d": datetime.date(2023, 1, 1)},
        {"id": 3, "d": datetime.date(2025, 1, 1)},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Both out-of-range rows fail; the in-range row passes:

```text
+----------+---+-----------------------------------------+----------+--------------------------+
|d         |id |_dq_errors                               |_dq_passed|_dq_validation_ts         |
+----------+---+-----------------------------------------+----------+--------------------------+
|2023-01-01|2  |[{DateBetweenCheck, date-2024, critical}]|false     |2026-01-01 00:00:00.000000|
|2025-01-01|3  |[{DateBetweenCheck, date-2024, critical}]|false     |2026-01-01 00:00:00.000000|
+----------+---+-----------------------------------------+----------+--------------------------+
```

## Typical use cases

- Constrain event or transaction dates to a valid business period.
- Reject records outside a reporting or fiscal window.
- Detect date errors from upstream systems or migrations.

## Related checks

- [Date Min Check](date_min_check.md) — enforce only a lower date bound.
- [Date Max Check](date_max_check.md) — enforce only an upper date bound.

---

[← Row-Level Checks](../../row_level.md)
