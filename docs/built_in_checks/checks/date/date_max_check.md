---
wide: true
---

# Date Max Check

**Check name**: `date-max-check` · **Type**: row-level · **Config**: `DateMaxCheckConfig`

Flags any record whose value in one of the configured date columns is later than
a maximum date. Use it to reject future-dated records or dates beyond a valid
reporting window.

## Parameters

| Parameter   | Type        | Required | Default    | Description                                                         |
| ----------- | ----------- | -------- | ---------- | ------------------------------------------------------------------- |
| `check_id`  | `str`       | yes      | —          | Unique identifier for this check within the `CheckSet`.             |
| `columns`   | `list[str]` | yes      | —          | Date columns to compare against the threshold. YAML key: `columns`. |
| `max_value` | `str`       | yes      | —          | Upper bound as `YYYY-MM-DD`. YAML key: `max-value`.                 |
| `inclusive` | `bool`      | no       | `False`    | Whether `max_value` itself is allowed (see Behavior).               |
| `severity`  | `Severity`  | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import DateMaxCheckConfig
    from sparkdq.core import Severity

    DateMaxCheckConfig(
        check_id="max-date",
        columns=["d"],
        max_value="2024-12-31",
        inclusive=True,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: date-max-check
      check-id: max-date
      columns:
        - d
      max-value: "2024-12-31"
      inclusive: true
      severity: critical
    ```

## Behavior

- **`max_value` is a `YYYY-MM-DD` string.** The column is cast to `date` before
  comparison.
- **`inclusive` controls the boundary, and defaults to `False`.** With
  `inclusive=True`, a value equal to `max_value` **passes** (`value <= max_value`).
  With the default, the boundary date itself **fails** (`value < max_value`).
- **OR semantics across columns.** With multiple columns, a record fails if
  _any_ of them is after the threshold.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist, the check
  raises `MissingColumnError` at validation time.

## Example

Requiring `d <= 2024-12-31` (`inclusive=True`), both styles produce the same result.

=== "Python"

    ```python
    import datetime
    from pyspark.sql import SparkSession
    from sparkdq.checks import DateMaxCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "d": datetime.date(2024, 6, 1)},
        {"id": 2, "d": datetime.date(2025, 2, 1)},
    ])

    check_set = CheckSet().add_check(
        DateMaxCheckConfig(check_id="max-date", columns=["d"], max_value="2024-12-31", inclusive=True)
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
        {"id": 2, "d": datetime.date(2025, 2, 1)},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the too-late row fails:

```text
+----------+---+------------------------------------+----------+--------------------------+
|d         |id |_dq_errors                          |_dq_passed|_dq_validation_ts         |
+----------+---+------------------------------------+----------+--------------------------+
|2025-02-01|2  |[{DateMaxCheck, max-date, critical}]|false     |2026-01-01 00:00:00.000000|
+----------+---+------------------------------------+----------+--------------------------+
```

## Typical use cases

- Reject future-dated records where only past dates are valid.
- Enforce an upper bound aligned with a reporting or snapshot window.
- Detect clock or timezone errors producing out-of-range dates.

## Related checks

- [Date Min Check](date_min_check.md) — enforce a lower date bound.
- [Date Between Check](date_between_check.md) — enforce both date bounds at once.

---

[← Row-Level Checks](../../row_level.md)
