---
wide: true
---

# Row Count Min Check

**Check name**: `row-count-min-check` · **Type**: aggregate · **Config**: `RowCountMinCheckConfig`

Validates that the dataset contains at least a minimum number of rows. Use it to
stop downstream processing on incomplete or unexpectedly small datasets.

## Parameters

| Parameter   | Type       | Required | Default    | Description                                               |
| ----------- | ---------- | -------- | ---------- | --------------------------------------------------------- |
| `check_id`  | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.   |
| `min_count` | `int`      | yes      | —          | Minimum number of rows expected. YAML key: `min-count`.   |
| `severity`  | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports. |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import RowCountMinCheckConfig
    from sparkdq.core import Severity

    RowCountMinCheckConfig(
        check_id="min-rows",
        min_count=1000,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: row-count-min-check
      check-id: min-rows
      min-count: 1000
      severity: critical
    ```

## Behavior

- **Dataset-level verdict.** As an aggregate check, it produces a single
  pass/fail for the whole DataFrame rather than a per-row flag.
- **A critical failure fails the batch.** When a `CRITICAL` aggregate check fails,
  _every_ row is marked `_dq_passed = False`, so `pass_df()` is empty and
  `fail_df()` holds all rows. A `WARNING` failure is reported without failing rows.
- **Result and metrics.** The outcome is available via
  `result.aggregate_results`; each carries `passed` and a `metrics` dict — here
  `{"actual_row_count": ..., "min_expected": ...}`.

## Example

Requiring at least 1000 rows on a 5-row DataFrame, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import RowCountMinCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([{"id": i} for i in range(1, 6)])

    check_set = CheckSet().add_check(
        RowCountMinCheckConfig(check_id="min-rows", min_count=1000)
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
min-rows False {'actual_row_count': 5, 'min_expected': 1000}
```

## Typical use cases

- Detect partial loads or failed transfers that yield too few records.
- Enforce a minimum data volume before analytics, reporting, or model training.
- Prevent downstream runs on datasets too small to be meaningful.

## Related checks

- [Row Count Max Check](count_max_check.md) — enforce an upper row-count bound.
- [Row Count Between Check](count_between_check.md) — enforce a row-count range.
- [Row Count Exact Check](count_exact_check.md) — require an exact row count.

---

[← Aggregate Checks](../../aggregate.md)
