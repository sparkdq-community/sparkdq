---
wide: true
---

# Column Presence Check

**Check name**: `column-presence-check` · **Type**: aggregate · **Config**: `ColumnPresenceCheckConfig`

Validates that all required columns are present in the DataFrame schema, ignoring
their types and any extra columns. Use it as a lightweight contract on the shape
of an input.

## Parameters

| Parameter          | Type        | Required | Default    | Description                                               |
| ------------------ | ----------- | -------- | ---------- | --------------------------------------------------------- |
| `check_id`         | `str`       | yes      | —          | Unique identifier for this check within the `CheckSet`.   |
| `required_columns` | `list[str]` | yes      | —          | Columns that must exist. YAML key: `required-columns`.    |
| `severity`         | `Severity`  | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports. |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import ColumnPresenceCheckConfig
    from sparkdq.core import Severity

    ColumnPresenceCheckConfig(
        check_id="required-cols",
        required_columns=["id", "email"],
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: column-presence-check
      check-id: required-cols
      required-columns:
        - id
        - email
      severity: critical
    ```

## Behavior

- **Presence only.** Checks that each required column exists; it does not inspect
  types or reject extra columns (use the [Schema Check](schema_check.md) for that).
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict lists `missing_columns`.

## Example

Requiring `id` and `email` on a DataFrame missing `email`, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import ColumnPresenceCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([{"id": 1, "name": "Alice"}])

    check_set = CheckSet().add_check(
        ColumnPresenceCheckConfig(check_id="required-cols", required_columns=["id", "email"])
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

The aggregate result lists the missing column:

```text
required-cols False {'missing_columns': ['email']}
```

## Typical use cases

- Assert a minimal input contract before processing.
- Fail fast when an upstream feed drops an expected column.
- Guard notebooks or jobs against schema drift.

## Related checks

- [Schema Check](schema_check.md) — also validate column types and reject unexpected columns.

---

[← Aggregate Checks](../../aggregate.md)
