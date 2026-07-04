---
wide: true
---

# Row Count Max Check

**Check name**: `row-count-max-check` · **Type**: aggregate · **Config**: `RowCountMaxCheckConfig`

Validates that the dataset does not exceed a maximum number of rows. Use it to
detect unexpected data growth, runaway joins, or accidental full loads.

## Parameters

| Parameter   | Type       | Required | Default    | Description                                               |
| ----------- | ---------- | -------- | ---------- | --------------------------------------------------------- |
| `check_id`  | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.   |
| `max_count` | `int`      | yes      | —          | Maximum number of rows allowed. YAML key: `max-count`.    |
| `severity`  | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports. |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import RowCountMaxCheckConfig
    from sparkdq.core import Severity

    RowCountMaxCheckConfig(
        check_id="max-rows",
        max_count=3,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: row-count-max-check
      check-id: max-rows
      max-count: 3
      severity: critical
    ```

## Behavior

- **Dataset-level verdict.** Produces a single pass/fail for the whole DataFrame.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict is `{"actual_row_count": ..., "max_expected": ...}`.

## Example

Allowing at most 3 rows on a 5-row DataFrame, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import RowCountMaxCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([{"id": i} for i in range(1, 6)])

    check_set = CheckSet().add_check(
        RowCountMaxCheckConfig(check_id="max-rows", max_count=3)
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

    df = spark.createDataFrame([{"id": i} for i in range(1, 6)])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

The aggregate result reports the failure and the observed count:

```text
max-rows False {'actual_row_count': 5, 'max_expected': 3}
```

## Typical use cases

- Detect abnormal data growth from duplicates or incorrect joins.
- Prevent downstream systems from processing unexpectedly large datasets.
- Catch accidental full loads when only an incremental extract was intended.

## Related checks

- [Row Count Min Check](count_min_check.md) — enforce a lower row-count bound.
- [Row Count Between Check](count_between_check.md) — enforce a row-count range.
- [Row Count Exact Check](count_exact_check.md) — require an exact row count.

---

[← Aggregate Checks](../../aggregate.md)
