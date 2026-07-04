---
wide: true
---

# Date Min Check

**Check name**: `date-min-check` · **Type**: row-level · **Config**: `DateMinCheckConfig`

Flags any record whose value in one of the configured date columns is earlier
than a minimum date. Use it to reject records before an operational start date or
system go-live.

## Parameters

| Parameter   | Type        | Required | Default    | Description                                                         |
| ----------- | ----------- | -------- | ---------- | ------------------------------------------------------------------- |
| `check_id`  | `str`       | yes      | —          | Unique identifier for this check within the `CheckSet`.             |
| `columns`   | `list[str]` | yes      | —          | Date columns to compare against the threshold. YAML key: `columns`. |
| `min_value` | `str`       | yes      | —          | Lower bound as `YYYY-MM-DD`. YAML key: `min-value`.                 |
| `inclusive` | `bool`      | no       | `False`    | Whether `min_value` itself is allowed (see Behavior).               |
| `severity`  | `Severity`  | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import DateMinCheckConfig
    from sparkdq.core import Severity

    DateMinCheckConfig(
        check_id="min-date",
        columns=["d"],
        min_value="2024-01-01",
        inclusive=True,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: date-min-check
      check-id: min-date
      columns:
        - d
      min-value: "2024-01-01"
      inclusive: true
      severity: critical
    ```

## Behavior

- **`min_value` is a `YYYY-MM-DD` string.** The column is cast to `date` before
  comparison.
- **`inclusive` controls the boundary, and defaults to `False`.** With
  `inclusive=True`, a value equal to `min_value` **passes** (`value >= min_value`).
  With the default, the boundary date itself **fails** (`value > min_value`).
- **OR semantics across columns.** With multiple columns, a record fails if
  _any_ of them is before the threshold.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist, the check
  raises `MissingColumnError` at validation time.

## Example

Requiring `d >= 2024-01-01` (`inclusive=True`), both styles produce the same result.

=== "Python"

    ```python
    import datetime
    from pyspark.sql import SparkSession
    from sparkdq.checks import DateMinCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "d": datetime.date(2024, 6, 1)},
        {"id": 2, "d": datetime.date(2023, 12, 31)},
    ])

    check_set = CheckSet().add_check(
        DateMinCheckConfig(check_id="min-date", columns=["d"], min_value="2024-01-01", inclusive=True)
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
        {"id": 2, "d": datetime.date(2023, 12, 31)},
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
+----------+---+------------------------------------+----------+--------------------------+
|d         |id |_dq_errors                          |_dq_passed|_dq_validation_ts         |
+----------+---+------------------------------------+----------+--------------------------+
|2023-12-31|2  |[{DateMinCheck, min-date, critical}]|false     |2026-01-01 00:00:00.000000|
+----------+---+------------------------------------+----------+--------------------------+
```

## Typical use cases

- Ensure event or transaction dates are not before an operational start date.
- Reject historically stale records outside the valid business period.
- Align entries with a go-live or migration cutover date.

## Related checks

- [Date Max Check](date_max_check.md) — enforce an upper date bound.
- [Date Between Check](date_between_check.md) — enforce both date bounds at once.

---

[← Row-Level Checks](../../row_level.md)
