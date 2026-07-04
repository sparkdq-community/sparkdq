---
wide: true
---

# Freshness Check

**Check name**: `freshness-check` · **Type**: aggregate · **Config**: `FreshnessCheckConfig`

Validates that the most recent timestamp in a column is within a configured
recency window. Use it to detect stale datasets — feeds that stopped updating or
arrived late.

## Parameters

| Parameter  | Type       | Required | Default    | Description                                                                       |
| ---------- | ---------- | -------- | ---------- | --------------------------------------------------------------------------------- |
| `check_id` | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.                           |
| `column`   | `str`      | yes      | —          | The timestamp column to evaluate.                                                 |
| `interval` | `int`      | yes      | —          | Size of the recency window; must be positive.                                     |
| `period`   | `str`      | yes      | —          | Time unit (singular): `year`, `month`, `week`, `day`, `hour`, `minute`, `second`. |
| `severity` | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the whole batch; `WARNING` only reports.                         |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import FreshnessCheckConfig
    from sparkdq.core import Severity

    FreshnessCheckConfig(
        check_id="data-fresh",
        column="event_ts",
        interval=1,
        period="day",
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: freshness-check
      check-id: data-fresh
      column: event_ts
      interval: 1
      period: day
      severity: critical
    ```

## Behavior

- **Compares against `current_timestamp()`.** The check passes when
  `max(column) >= now - interval period`. It is evaluated at run time, so the
  verdict depends on when the pipeline runs.
- **`period` is singular.** Use `day`, not `days` — an invalid unit raises a
  validation error at config time. `interval` must be positive, or the config
  raises `InvalidCheckConfigurationError`.
- **A critical failure fails the batch.** A failing `CRITICAL` aggregate marks
  _every_ row `_dq_passed = False`. A `WARNING` failure is reported only.
- **Result and metrics.** Available via `result.aggregate_results`; the `metrics`
  dict reports the observed `max_timestamp` and the `freshness_threshold`.

## Example

Requiring data no older than 1 day, on a dataset whose latest timestamp is from
2020, the check fails.

=== "Python"

    ```python
    import datetime
    from pyspark.sql import SparkSession
    from sparkdq.checks import FreshnessCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "event_ts": datetime.datetime(2020, 1, 1, 0, 0, 0)},
    ])

    check_set = CheckSet().add_check(
        FreshnessCheckConfig(check_id="data-fresh", column="event_ts", interval=1, period="day")
    )
    result = BatchDQEngine(check_set).run_batch(df)

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

=== "YAML"

    ```python
    import datetime
    import yaml
    from pyspark.sql import SparkSession
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "event_ts": datetime.datetime(2020, 1, 1, 0, 0, 0)},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)

    for r in result.aggregate_results:
        print(r.check_id, r.passed, r.metrics)
    ```

The aggregate result reports the latest timestamp and the threshold:

```text
data-fresh False {'max_timestamp': '2020-01-01 00:00:00', 'freshness_threshold': '1 day'}
```

## Typical use cases

- Detect feeds that stopped updating or arrived late.
- Gate downstream jobs on data recency (e.g. "must be < 1 hour old").
- Monitor SLA compliance for periodic loads.

## Related checks

- [Timestamp Max Check](../timestamp/timestamp_max_check.md) — bound timestamps against a fixed instant rather than "now".

---

[← Aggregate Checks](../../aggregate.md)
