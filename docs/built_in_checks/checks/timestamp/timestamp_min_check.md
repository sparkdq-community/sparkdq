# Timestamp Min

**Check**: `timestamp-min-check`

**Purpose**: Validates that values in timestamp columns are not earlier than a defined minimum. A row fails if any selected column contains a timestamp before `min_value`.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: false` (default) — `value > min_value`
- `inclusive: true` — `value >= min_value`

=== "Python"

    ```python
    from sparkdq.checks import TimestampMinCheckConfig
    from sparkdq.core import Severity

    TimestampMinCheckConfig(
        check_id="event-time-not-before-start",
        columns=["event_time"],
        min_value="2020-01-01 00:00:00",
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: timestamp-min-check
      check-id: event-time-not-before-start
      columns:
        - event_time
      min-value: "2020-01-01 00:00:00"
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Ensure that event or log timestamps are not older than the defined operational start time.
- Validate that data collection began after a system go-live or migration cutover point.
- Reject historically stale records outside the defined processing window.

---

[← Row-Level Checks](../../row_level.md)
