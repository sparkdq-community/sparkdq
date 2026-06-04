# Freshness Check

**Check**: `freshness-check`

**Purpose**: Validates that the most recent timestamp in a column is within a defined interval relative to the current system time. The check fails if the newest value is older than the specified threshold.

!!! note
Supported values for the `period` parameter:
`year`, `month`, `week`, `day`, `hour`, `minute`, `second`

=== "Python"

    ```python
    from sparkdq.checks import FreshnessCheckConfig
    from sparkdq.core import Severity

    FreshnessCheckConfig(
        check_id="last-updated-within-24h",
        column="last_updated",
        interval=24,
        period="hour",
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: freshness-check
      check-id: last-updated-within-24h
      column: last_updated
      interval: 24
      period: hour
      severity: critical
    ```

## Typical Use Cases

- Ensure that ingested data has been updated within the expected frequency.
- Detect delays or failures in upstream ingestion pipelines before they affect consumers.
- Monitor SLA compliance by enforcing a freshness threshold for reporting or analytics datasets.

---

[← Aggregate Checks](../../aggregate.md)
