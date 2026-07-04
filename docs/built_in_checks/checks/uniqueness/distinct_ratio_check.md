---
wide: true
---

# Distinct Ratio Check

**Check name**: `distinct-ratio-check` · **Type**: aggregate · **Config**: `DistinctRatioCheckConfig`

Validates that the share of **distinct** values in a column meets a minimum ratio.
Use it to detect low-cardinality columns or excessive repetition.

## Parameters

| Parameter   | Type       | Required | Default    | Description                                                          |
| ----------- | ---------- | -------- | ---------- | -------------------------------------------------------------------- |
| `check_id`  | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.              |
| `column`    | `str`      | yes      | —          | The column whose distinct ratio is measured.                         |
| `min_ratio` | `float`    | yes      | —          | Minimum required distinct ratio, `0.0`–`1.0`. YAML key: `min-ratio`. |
| `severity`  | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports.            |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import DistinctRatioCheckConfig
    from sparkdq.core import Severity

    DistinctRatioCheckConfig(
        check_id="email-distinct-ratio",
        column="email",
        min_ratio=0.9,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: distinct-ratio-check
      check-id: email-distinct-ratio
      column: email
      min-ratio: 0.9
      severity: critical
    ```

## Behavior

- **Distinct means "number of different values".** The ratio is
  `distinct_count / row_count`. This differs from the
  [Unique Ratio Check](unique_ratio_check.md), which counts only values appearing
  exactly once.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict reports `distinct_count`, `row_count`, `distinct_ratio`, and
  `min_expected_ratio`.

## Example

Requiring a 90% distinct ratio on a two-value column, the check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import DistinctRatioCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "a@example.com"},
        {"id": 3, "email": "b@example.com"},
        {"id": 4, "email": "b@example.com"},
    ])

    check_set = CheckSet().add_check(
        DistinctRatioCheckConfig(check_id="email-distinct-ratio", column="email", min_ratio=0.9)
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

    df = spark.createDataFrame([
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "a@example.com"},
        {"id": 3, "email": "b@example.com"},
        {"id": 4, "email": "b@example.com"},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

The aggregate result reports the failure and the observed ratio:

```text
email-distinct-ratio False {'distinct_count': 2, 'row_count': 4, 'distinct_ratio': 0.5, 'min_expected_ratio': 0.9}
```

## Typical use cases

- Detect low-cardinality columns that should carry more variety.
- Catch stuck or defaulted values dominating a column.
- Monitor cardinality health over time.

## Related checks

- [Unique Ratio Check](unique_ratio_check.md) — count values appearing exactly once.
- [Unique Rows Check](unique_rows_check.md) — require full uniqueness (no duplicates).

---

[← Aggregate Checks](../../aggregate.md)
