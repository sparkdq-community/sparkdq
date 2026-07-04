---
wide: true
---

# Row Count Exact Check

**Check name**: `row-count-exact-check` · **Type**: aggregate · **Config**: `RowCountExactCheckConfig`

Validates that the dataset contains exactly the expected number of rows. Use it
to enforce strict volume contracts for fixed-size exports, snapshots, or
reference datasets.

## Parameters

| Parameter        | Type       | Required | Default    | Description                                                    |
| ---------------- | ---------- | -------- | ---------- | -------------------------------------------------------------- |
| `check_id`       | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.        |
| `expected_count` | `int`      | yes      | —          | The exact number of rows required. YAML key: `expected-count`. |
| `severity`       | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports.      |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import RowCountExactCheckConfig
    from sparkdq.core import Severity

    RowCountExactCheckConfig(
        check_id="exact-rows",
        expected_count=10,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: row-count-exact-check
      check-id: exact-rows
      expected-count: 10
      severity: critical
    ```

## Behavior

- **Dataset-level verdict.** Produces a single pass/fail for the whole DataFrame.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict is `{"actual_row_count": ..., "expected_row_count": ...}`.

## Example

Requiring exactly 10 rows on a 5-row DataFrame, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import RowCountExactCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([{"id": i} for i in range(1, 6)])

    check_set = CheckSet().add_check(
        RowCountExactCheckConfig(check_id="exact-rows", expected_count=10)
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
exact-rows False {'actual_row_count': 5, 'expected_row_count': 10}
```

## Typical use cases

- Validate fixed-size imports where exactly N records are expected.
- Ensure integrity of snapshot pipelines with a predictable record count.
- Detect silent load failures or unintended duplication.

## Related checks

- [Row Count Min Check](count_min_check.md) — enforce a lower bound.
- [Row Count Max Check](count_max_check.md) — enforce an upper bound.
- [Row Count Between Check](count_between_check.md) — enforce a range.

---

[← Aggregate Checks](../../aggregate.md)
