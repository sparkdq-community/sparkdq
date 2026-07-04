---
wide: true
---

# Schema Check

**Check name**: `schema-check` · **Type**: aggregate · **Config**: `SchemaCheckConfig`

Validates that the DataFrame matches an expected schema — the required columns
are present with the expected Spark types, and (in strict mode) no unexpected
columns exist. Use it as a full structural contract on an input.

## Parameters

| Parameter         | Type             | Required | Default    | Description                                                              |
| ----------------- | ---------------- | -------- | ---------- | ------------------------------------------------------------------------ |
| `check_id`        | `str`            | yes      | —          | Unique identifier for this check within the `CheckSet`.                  |
| `expected_schema` | `dict[str, str]` | yes      | —          | Mapping of column name → Spark type string. YAML key: `expected-schema`. |
| `strict`          | `bool`           | no       | `True`     | If `True`, extra columns not in the schema fail the check.               |
| `severity`        | `Severity`       | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports.                |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import SchemaCheckConfig
    from sparkdq.core import Severity

    SchemaCheckConfig(
        check_id="input-schema",
        expected_schema={"id": "bigint", "name": "string", "email": "string"},
        strict=True,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: schema-check
      check-id: input-schema
      expected-schema:
        id: bigint
        name: string
        email: string
      strict: true
      severity: critical
    ```

## Behavior

- **Three failure modes.** The check fails on any missing column, any type
  mismatch, or — when `strict=True` — any unexpected extra column.
- **Type strings are Spark types.** Use Spark SQL type names such as `bigint`,
  `string`, `double`, `timestamp`. They must match the DataFrame's actual types
  exactly.
- **`strict` defaults to `True`.** Set `strict=False` to allow additional columns
  beyond the expected schema.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict reports `missing_columns`, `type_mismatches`, and `unexpected_columns`.

## Example

Expecting an `email` column that is absent, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import SchemaCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([{"id": 1, "name": "Alice"}])

    check_set = CheckSet().add_check(
        SchemaCheckConfig(
            check_id="input-schema",
            expected_schema={"id": "bigint", "name": "string", "email": "string"},
        )
    )
    result = BatchDQEngine(check_set).run_batch(df)

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

=== "YAML"

    ```python
    import yaml
    from pyspark.sql import SparkSession
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([{"id": 1, "name": "Alice"}])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

The aggregate result breaks the failure down by category:

```text
input-schema False {'missing_columns': ['email'], 'type_mismatches': {}, 'unexpected_columns': []}
```

## Typical use cases

- Enforce a full input contract (columns + types) before processing.
- Detect schema drift such as renamed columns or changed types.
- Reject unexpected extra columns in strict pipelines.

## Related checks

- [Column Presence Check](column_presence_check.md) — a lighter check for column existence only.

---

[← Aggregate Checks](../../aggregate.md)
