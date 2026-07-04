---
wide: true
---

# Numeric Min Check

**Check name**: `numeric-min-check` · **Type**: row-level · **Config**: `NumericMinCheckConfig`

Flags any record whose value in one of the configured columns falls below a
minimum threshold. Use it to enforce lower bounds such as a minimum age, a
non-negative balance, or an acceptable measurement floor.

## Parameters

| Parameter   | Type           | Required | Default    | Description                                                            |
| ----------- | -------------- | -------- | ---------- | ---------------------------------------------------------------------- |
| `check_id`  | `str`          | yes      | —          | Unique identifier for this check within the `CheckSet`.                |
| `columns`   | `list[str]`    | yes      | —          | Numeric columns to compare against the threshold. YAML key: `columns`. |
| `min_value` | `float \| int` | yes      | —          | The lower bound. YAML key: `min-value`.                                |
| `inclusive` | `bool`         | no       | `False`    | Whether `min_value` itself is allowed (see Behavior).                  |
| `severity`  | `Severity`     | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                   |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import NumericMinCheckConfig
    from sparkdq.core import Severity

    NumericMinCheckConfig(
        check_id="min-age",
        columns=["age"],
        min_value=18,
        inclusive=True,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: numeric-min-check
      check-id: min-age
      columns:
        - age
      min-value: 18
      inclusive: true
      severity: critical
    ```

## Behavior

- **`inclusive` controls the boundary, and defaults to `False`.** With
  `inclusive=True`, a value equal to `min_value` **passes** (`value >= min_value`
  is required). With the default `inclusive=False`, the boundary value itself
  **fails** (`value > min_value` is required). Set `inclusive=True` when the
  threshold should be an allowed value — this is the most common intent.
- **OR semantics across columns.** With multiple columns, a record fails if
  _any_ of them is below the threshold.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`; with
  the default severity, a failure sets `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist, the check
  raises `MissingColumnError` at validation time.

## Example

Requiring `age >= 18` (`inclusive=True`), both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import NumericMinCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "age": 25},
        {"id": 2, "age": 17},
        {"id": 3, "age": 18},
    ])

    check_set = CheckSet().add_check(
        NumericMinCheckConfig(check_id="min-age", columns=["age"], min_value=18, inclusive=True)
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
        {"id": 1, "age": 25},
        {"id": 2, "age": 17},
        {"id": 3, "age": 18},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the underage row fails (`age = 18` passes because the bound is inclusive):

```text
+---+---+--------------------------------------+----------+--------------------------+
|age|id |_dq_errors                            |_dq_passed|_dq_validation_ts         |
+---+---+--------------------------------------+----------+--------------------------+
|17 |2  |[{NumericMinCheck, min-age, critical}]|false     |2026-01-01 00:00:00.000000|
+---+---+--------------------------------------+----------+--------------------------+
```

## Typical use cases

- Enforce a minimum age, quantity, or order amount.
- Reject negative values where only non-negative numbers are valid.
- Guard a measurement or score against an unacceptable floor.

## Related checks

- [Numeric Max Check](numeric_max_check.md) — enforce an upper bound.
- [Numeric Between Check](numeric_between_check.md) — enforce both bounds at once.

---

[← Row-Level Checks](../../row_level.md)
