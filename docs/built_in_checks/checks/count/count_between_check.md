---
wide: true
---

# Row Count Between Check

**Check name**: `row-count-between-check` · **Type**: aggregate · **Config**: `RowCountBetweenCheckConfig`

Validates that the dataset's row count falls within a `[min, max]` range. Use it
to catch both partial loads (too few) and unexpected growth (too many).

## Parameters

| Parameter   | Type       | Required | Default    | Description                                               |
| ----------- | ---------- | -------- | ---------- | --------------------------------------------------------- |
| `check_id`  | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.   |
| `min_count` | `int`      | yes      | —          | Minimum number of rows expected. YAML key: `min-count`.   |
| `max_count` | `int`      | yes      | —          | Maximum number of rows allowed. YAML key: `max-count`.    |
| `severity`  | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports. |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import RowCountBetweenCheckConfig
    from sparkdq.core import Severity

    RowCountBetweenCheckConfig(
        check_id="rows-in-range",
        min_count=10,
        max_count=20,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: row-count-between-check
      check-id: rows-in-range
      min-count: 10
      max-count: 20
      severity: critical
    ```

## Behavior

- **Dataset-level verdict.** Produces a single pass/fail for the whole DataFrame.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict is `{"actual_row_count": ..., "min_expected": ..., "max_expected": ...}`.

## Example

Requiring between 10 and 20 rows on a 5-row DataFrame, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import RowCountBetweenCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([{"id": i} for i in range(1, 6)])

    check_set = CheckSet().add_check(
        RowCountBetweenCheckConfig(check_id="rows-in-range", min_count=10, max_count=20)
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
rows-in-range False {'actual_row_count': 5, 'min_expected': 10, 'max_expected': 20}
```

## Typical use cases

- Detect partial loads (too few rows) or unintended duplication (too many).
- Validate dataset size before triggering downstream jobs.
- Catch filter changes that unintentionally affect the row count.

## Related checks

- [Row Count Min Check](count_min_check.md) — enforce only a lower bound.
- [Row Count Max Check](count_max_check.md) — enforce only an upper bound.
- [Row Count Exact Check](count_exact_check.md) — require an exact row count.

---

[← Aggregate Checks](../../aggregate.md)
