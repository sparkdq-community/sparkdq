---
wide: true
---

# Completeness Ratio Check

**Check name**: `completeness-ratio-check` · **Type**: aggregate · **Config**: `CompletenessRatioCheckConfig`

Validates that the share of non-null values in a column meets a minimum ratio.
Use it when some missing data is tolerable but should stay under a threshold.

## Parameters

| Parameter   | Type       | Required | Default    | Description                                                          |
| ----------- | ---------- | -------- | ---------- | -------------------------------------------------------------------- |
| `check_id`  | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.              |
| `column`    | `str`      | yes      | —          | The column whose completeness is measured (a single column).         |
| `min_ratio` | `float`    | yes      | —          | Minimum required non-null ratio, `0.0`–`1.0`. YAML key: `min-ratio`. |
| `severity`  | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports.            |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import CompletenessRatioCheckConfig
    from sparkdq.core import Severity

    CompletenessRatioCheckConfig(
        check_id="email-ratio",
        column="email",
        min_ratio=0.9,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: completeness-ratio-check
      check-id: email-ratio
      column: email
      min-ratio: 0.9
      severity: critical
    ```

## Behavior

- **Dataset-level verdict.** The non-null ratio is `non_null_count / total_count`;
  the check fails when it is below `min_ratio`.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict reports `total_count`, `non_null_count`, `min_required_ratio`, and the
  `actual_ratio`.

## Example

Requiring at least 90% non-null `email` on a dataset that is 75% complete, the
check fails.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import CompletenessRatioCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": None},
        {"id": 3, "email": "c@example.com"},
        {"id": 4, "email": "d@example.com"},
    ])

    check_set = CheckSet().add_check(
        CompletenessRatioCheckConfig(check_id="email-ratio", column="email", min_ratio=0.9)
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
        {"id": 2, "email": None},
        {"id": 3, "email": "c@example.com"},
        {"id": 4, "email": "d@example.com"},
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
email-ratio False {'column': 'email', 'total_count': 4, 'non_null_count': 3, 'min_required_ratio': 0.9, 'actual_ratio': 0.75}
```

## Typical use cases

- Allow a tolerable amount of missing data while capping it at a threshold.
- Monitor completeness trends of optional-but-important fields.
- Gate publishing on a minimum fill rate.

## Related checks

- [Columns Are Complete Check](columns_are_complete_check.md) — require zero nulls.
- [Null Check](../null/null_check.md) — flag the individual rows that contain nulls.

---

[← Aggregate Checks](../../aggregate.md)
