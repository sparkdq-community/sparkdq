---
wide: true
---

# Column Less Than Check

**Check name**: `column-less-than-check` · **Type**: row-level · **Config**: `ColumnLessThanCheckConfig`

Flags any record where `column` is not less than `limit`. The `limit` can be
another column or any Spark SQL expression, so this enforces relationships like
`discount < price` or `cost_price < selling_price * 0.9`.

## Parameters

| Parameter   | Type       | Required | Default    | Description                                                                           |
| ----------- | ---------- | -------- | ---------- | ------------------------------------------------------------------------------------- |
| `check_id`  | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.                               |
| `column`    | `str`      | yes      | —          | The column expected to hold the smaller value.                                        |
| `limit`     | `str`      | yes      | —          | A column name or Spark SQL expression the value must stay below (e.g. `price * 0.9`). |
| `inclusive` | `bool`     | no       | `False`    | If `True`, allows equality (`<=`); otherwise strict (`<`).                            |
| `severity`  | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                                  |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import ColumnLessThanCheckConfig
    from sparkdq.core import Severity

    ColumnLessThanCheckConfig(
        check_id="cost-below-margin",
        column="cost",
        limit="price * 0.9",
        inclusive=False,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: column-less-than-check
      check-id: cost-below-margin
      column: cost
      limit: "price * 0.9"
      inclusive: false
      severity: critical
    ```

## Behavior

- **`limit` is a Spark SQL expression.** It may be a bare column name (`price`)
  or an expression (`price * 0.9`, or a `CASE WHEN …`). It is evaluated per row
  against the same DataFrame.
- **Nulls fail.** A null in _either_ `column` or the evaluated `limit` marks the
  row as failed.
- **`inclusive` defaults to `False`** (strict `<`); set `True` to allow equality.
- **Invalid expressions raise.** A `limit` that cannot be parsed raises
  `InvalidSQLExpressionError`; a missing `column` raises `MissingColumnError`.

## Example

Requiring `cost < price * 0.9`, both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import ColumnLessThanCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "cost": 50, "price": 100},
        {"id": 2, "cost": 95, "price": 100},
    ])

    check_set = CheckSet().add_check(
        ColumnLessThanCheckConfig(check_id="cost-below-margin", column="cost", limit="price * 0.9")
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
        {"id": 1, "cost": 50, "price": 100},
        {"id": 2, "cost": 95, "price": 100},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the row where `cost >= price * 0.9` fails:

```text
+----+---+-----+----------------------------------------------------+----------+--------------------------+
|cost|id |price|_dq_errors                                          |_dq_passed|_dq_validation_ts         |
+----+---+-----+----------------------------------------------------+----------+--------------------------+
|95  |2  |100  |[{ColumnLessThanCheck, cost-below-margin, critical}]|false     |2026-01-01 00:00:00.000000|
+----+---+-----+----------------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Enforce ordering between two columns (`discount < price`).
- Require a value to stay below a computed threshold (`cost < price * 0.9`).
- Validate conditional ceilings via a `CASE WHEN` expression.

## Related checks

- [Column Greater Than Check](column_greater.md) — the mirror comparison (`>`).

---

[← Row-Level Checks](../../row_level.md)
