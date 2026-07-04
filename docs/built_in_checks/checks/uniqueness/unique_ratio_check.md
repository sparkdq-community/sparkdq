---
wide: true
---

# Unique Ratio Check

**Check name**: `unique-ratio-check` · **Type**: aggregate · **Config**: `UniqueRatioCheckConfig`

Validates that the share of **unique** (appearing exactly once) non-null values
in a column meets a minimum ratio. Use it to bound the amount of duplication in a
near-key column.

## Parameters

| Parameter   | Type       | Required | Default    | Description                                                        |
| ----------- | ---------- | -------- | ---------- | ------------------------------------------------------------------ |
| `check_id`  | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.            |
| `column`    | `str`      | yes      | —          | The column whose uniqueness ratio is measured.                     |
| `min_ratio` | `float`    | yes      | —          | Minimum required unique ratio, `0.0`–`1.0`. YAML key: `min-ratio`. |
| `severity`  | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports.          |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import UniqueRatioCheckConfig
    from sparkdq.core import Severity

    UniqueRatioCheckConfig(
        check_id="email-unique-ratio",
        column="email",
        min_ratio=0.9,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: unique-ratio-check
      check-id: email-unique-ratio
      column: email
      min-ratio: 0.9
      severity: critical
    ```

## Behavior

- **Unique means "appears exactly once".** The ratio is
  `unique_count / total_count`, where `unique_count` counts values occurring a
  single time. This differs from the
  [Distinct Ratio Check](distinct_ratio_check.md), which counts _distinct_ values.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict reports `total_count`, `unique_count`, `actual_ratio`, and
  `min_required_ratio`.

## Example

Requiring a 90% unique ratio on a column where every value repeats, the check
fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import UniqueRatioCheckConfig
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
        UniqueRatioCheckConfig(check_id="email-unique-ratio", column="email", min_ratio=0.9)
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
email-unique-ratio False {'column': 'email', 'total_count': 4, 'unique_count': 2, 'actual_ratio': 0.5, 'min_required_ratio': 0.9}
```

## Typical use cases

- Bound duplication in a near-key or identifier column.
- Monitor the health of a column expected to be mostly unique.
- Detect a spike in repeated values from upstream issues.

## Related checks

- [Unique Rows Check](unique_rows_check.md) — require full uniqueness (no duplicates).
- [Distinct Ratio Check](distinct_ratio_check.md) — measure distinct values instead of singletons.

---

[← Aggregate Checks](../../aggregate.md)
