---
wide: true
---

# Numeric Max Check

**Check name**: `numeric-max-check` · **Type**: row-level · **Config**: `NumericMaxCheckConfig`

Flags any record whose value in one of the configured columns exceeds a maximum
threshold. Use it to enforce upper bounds such as a percentage capped at 100, a
maximum order quantity, or a plausible measurement ceiling.

## Parameters

| Parameter   | Type           | Required | Default    | Description                                                            |
| ----------- | -------------- | -------- | ---------- | ---------------------------------------------------------------------- |
| `check_id`  | `str`          | yes      | —          | Unique identifier for this check within the `CheckSet`.                |
| `columns`   | `list[str]`    | yes      | —          | Numeric columns to compare against the threshold. YAML key: `columns`. |
| `max_value` | `float \| int` | yes      | —          | The upper bound. YAML key: `max-value`.                                |
| `inclusive` | `bool`         | no       | `False`    | Whether `max_value` itself is allowed (see Behavior).                  |
| `severity`  | `Severity`     | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                   |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import NumericMaxCheckConfig
    from sparkdq.core import Severity

    NumericMaxCheckConfig(
        check_id="max-score",
        columns=["score"],
        max_value=100,
        inclusive=True,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: numeric-max-check
      check-id: max-score
      columns:
        - score
      max-value: 100
      inclusive: true
      severity: critical
    ```

## Behavior

- **`inclusive` controls the boundary, and defaults to `False`.** With
  `inclusive=True`, a value equal to `max_value` **passes** (`value <= max_value`
  is required). With the default `inclusive=False`, the boundary value itself
  **fails** (`value < max_value` is required). Set `inclusive=True` when the
  threshold should be an allowed value — this is the most common intent.
- **OR semantics across columns.** With multiple columns, a record fails if
  _any_ of them exceeds the threshold.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist, the check
  raises `MissingColumnError` at validation time.

## Example

Requiring `score <= 100` (`inclusive=True`), both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import NumericMaxCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "score": 90},
        {"id": 2, "score": 105},
    ])

    check_set = CheckSet().add_check(
        NumericMaxCheckConfig(check_id="max-score", columns=["score"], max_value=100, inclusive=True)
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
        {"id": 1, "score": 90},
        {"id": 2, "score": 105},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the out-of-range row fails:

```text
+---+-----+----------------------------------------+----------+--------------------------+
|id |score|_dq_errors                              |_dq_passed|_dq_validation_ts         |
+---+-----+----------------------------------------+----------+--------------------------+
|2  |105  |[{NumericMaxCheck, max-score, critical}]|false     |2026-01-01 00:00:00.000000|
+---+-----+----------------------------------------+----------+--------------------------+
```

## Typical use cases

- Cap percentages or scores at a known ceiling (e.g. 100).
- Enforce a maximum order quantity or transaction amount.
- Detect data entry errors or anomalies producing implausibly large values.

## Related checks

- [Numeric Min Check](numeric_min_check.md) — enforce a lower bound.
- [Numeric Between Check](numeric_between_check.md) — enforce both bounds at once.

---

[← Row-Level Checks](../../row_level.md)
