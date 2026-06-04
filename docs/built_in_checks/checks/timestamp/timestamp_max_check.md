# Timestamp Max

**Check**: `timestamp-max-check`

**Purpose**: Validates that values in timestamp columns do not exceed a defined maximum. A row fails if any selected column contains a timestamp after `max_value`.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: false` (default) — `value < max_value`
- `inclusive: true` — `value <= max_value`

=== "Python"

    ```python
    from sparkdq.checks import TimestampMaxCheckConfig
    from sparkdq.core import Severity

    TimestampMaxCheckConfig(
        check_id="event-time-not-in-future",
        columns=["event_time"],
        max_value="2023-12-31 23:59:59",
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: timestamp-max-check
      check-id: event-time-not-in-future
      columns:
        - event_time
      max-value: "2023-12-31 23:59:59"
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Ensure that event or log timestamps do not lie in the future.
- Enforce a processing cutoff for pipelines that only consume data up to a specific point in time.
- Detect future-dated records produced by system clock errors or incorrect timezone handling.

---

[← Row-Level Checks](../../row_level.md)
