---
wide: true
---

# Column Greater Than Check

**Check name**: `column-greater-than-check` · **Type**: row-level · **Config**: `ColumnGreaterThanCheckConfig`

Flags any record where `column` is not greater than `limit`. The `limit` can be
another column or any Spark SQL expression, so this enforces relationships like
`end_time > start_time` or `selling_price > cost_price * 1.2`.

## Parameters

| Parameter   | Type       | Required | Default    | Description                                                                      |
| ----------- | ---------- | -------- | ---------- | -------------------------------------------------------------------------------- |
| `check_id`  | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.                          |
| `column`    | `str`      | yes      | —          | The column expected to hold the greater value.                                   |
| `limit`     | `str`      | yes      | —          | A column name or Spark SQL expression the value must exceed (e.g. `cost * 1.2`). |
| `inclusive` | `bool`     | no       | `False`    | If `True`, allows equality (`>=`); otherwise strict (`>`).                       |
| `severity`  | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                             |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import ColumnGreaterThanCheckConfig
    from sparkdq.core import Severity

    ColumnGreaterThanCheckConfig(
        check_id="end-after-start",
        column="end",
        limit="start",
        inclusive=False,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: column-greater-than-check
      check-id: end-after-start
      column: end
      limit: start
      inclusive: false
      severity: critical
    ```

## Behavior

- **`limit` is a Spark SQL expression.** It may be a bare column name (`start`)
  or an expression (`cost_price * 1.2`, or a `CASE WHEN …`). It is evaluated per
  row against the same DataFrame.
- **Nulls fail.** Unlike the single-value comparison checks, a null in _either_
  `column` or the evaluated `limit` marks the row as failed.
- **`inclusive` defaults to `False`** (strict `>`); set `True` to allow equality.
- **Invalid expressions raise.** A `limit` that cannot be parsed raises
  `InvalidSQLExpressionError`; a missing `column` raises `MissingColumnError`.

## Example

Requiring `end > start`, both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import ColumnGreaterThanCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "start": 10, "end": 20},
        {"id": 2, "start": 30, "end": 25},
        {"id": 3, "start": 5, "end": None},
    ])

    check_set = CheckSet().add_check(
        ColumnGreaterThanCheckConfig(check_id="end-after-start", column="end", limit="start")
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
        {"id": 1, "start": 10, "end": 20},
        {"id": 2, "start": 30, "end": 25},
        {"id": 3, "start": 5, "end": None},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

The row where `end <= start` and the row with a null `end` both fail:

```text
+----+---+-----+-----------------------------------------------------+----------+--------------------------+
|end |id |start|_dq_errors                                           |_dq_passed|_dq_validation_ts         |
+----+---+-----+-----------------------------------------------------+----------+--------------------------+
|25  |2  |30   |[{ColumnGreaterThanCheck, end-after-start, critical}]|false     |2026-01-01 00:00:00.000000|
|NULL|3  |5    |[{ColumnGreaterThanCheck, end-after-start, critical}]|false     |2026-01-01 00:00:00.000000|
+----+---+-----+-----------------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Enforce ordering between two columns (`end_time > start_time`).
- Require a margin over a computed value (`selling_price > cost_price * 1.2`).
- Validate conditional thresholds via a `CASE WHEN` expression.

## Related checks

- [Column Less Than Check](column_less.md) — the mirror comparison (`<`).

---

[← Row-Level Checks](../../row_level.md)
