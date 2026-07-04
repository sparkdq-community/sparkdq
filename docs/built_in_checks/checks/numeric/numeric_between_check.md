---
wide: true
---

# Numeric Between Check

**Check name**: `numeric-between-check` · **Type**: row-level · **Config**: `NumericBetweenCheckConfig`

Flags any record whose value in one of the configured columns falls outside a
`[min, max]` range. Use it to constrain values to a valid interval, such as a
percentage between 0 and 100 or a plausible sensor reading.

## Parameters

| Parameter   | Type                | Required | Default          | Description                                                           |
| ----------- | ------------------- | -------- | ---------------- | --------------------------------------------------------------------- |
| `check_id`  | `str`               | yes      | —                | Unique identifier for this check within the `CheckSet`.               |
| `columns`   | `list[str]`         | yes      | —                | Numeric columns to compare against the range. YAML key: `columns`.    |
| `min_value` | `float \| int`      | yes      | —                | Lower bound of the range. YAML key: `min-value`.                      |
| `max_value` | `float \| int`      | yes      | —                | Upper bound of the range. YAML key: `max-value`.                      |
| `inclusive` | `tuple[bool, bool]` | no       | `(False, False)` | Inclusivity of the lower and upper bound respectively (see Behavior). |
| `severity`  | `Severity`          | no       | `CRITICAL`       | `CRITICAL` fails the row; `WARNING` only records it.                  |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import NumericBetweenCheckConfig
    from sparkdq.core import Severity

    NumericBetweenCheckConfig(
        check_id="temp-range",
        columns=["temp"],
        min_value=0,
        max_value=100,
        inclusive=(True, True),
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: numeric-between-check
      check-id: temp-range
      columns:
        - temp
      min-value: 0
      max-value: 100
      inclusive: [true, true]
      severity: critical
    ```

## Behavior

- **`inclusive` is a `(lower, upper)` pair and defaults to `(False, False)`** —
  i.e. strictly between. Each bound is controlled independently:

  | `inclusive`      | Passing range         |
  | ---------------- | --------------------- |
  | `(False, False)` | `min < value < max`   |
  | `(True, False)`  | `min <= value < max`  |
  | `(False, True)`  | `min < value <= max`  |
  | `(True, True)`   | `min <= value <= max` |

- **Validated at config time.** `min_value` must not be greater than `max_value`,
  or the config raises an `InvalidCheckConfigurationError` before any data is
  touched.
- **OR semantics across columns.** With multiple columns, a record fails if
  _any_ of them lies outside the range.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist, the check
  raises `MissingColumnError` at validation time.

## Example

Requiring `temp` within `[0, 100]` (both bounds inclusive), both styles produce
the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import NumericBetweenCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "temp": 50},
        {"id": 2, "temp": -5},
        {"id": 3, "temp": 120},
    ])

    check_set = CheckSet().add_check(
        NumericBetweenCheckConfig(
            check_id="temp-range", columns=["temp"], min_value=0, max_value=100, inclusive=(True, True)
        )
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
        {"id": 1, "temp": 50},
        {"id": 2, "temp": -5},
        {"id": 3, "temp": 120},
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
+---+----+---------------------------------------------+----------+--------------------------+
|id |temp|_dq_errors                                   |_dq_passed|_dq_validation_ts         |
+---+----+---------------------------------------------+----------+--------------------------+
|2  |-5  |[{NumericBetweenCheck, temp-range, critical}]|false     |2026-01-01 00:00:00.000000|
|3  |120 |[{NumericBetweenCheck, temp-range, critical}]|false     |2026-01-01 00:00:00.000000|
+---+----+---------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Constrain percentages, ratios, or scores to their valid interval (e.g. 0–100).
- Enforce physical or business-defined bounds on measurements.
- Detect outliers that fall outside an acceptable range.

## Related checks

- [Numeric Min Check](numeric_min_check.md) — enforce only a lower bound.
- [Numeric Max Check](numeric_max_check.md) — enforce only an upper bound.

---

[← Row-Level Checks](../../row_level.md)
