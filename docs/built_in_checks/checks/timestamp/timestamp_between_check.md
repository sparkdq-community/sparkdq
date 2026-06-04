# Timestamp Between

**Check**: `timestamp-between-check`

**Purpose**: Validates that values in timestamp columns fall within a defined range. A row fails if any selected column contains a timestamp before `min_value` or after `max_value`.

Use the `inclusive` parameter to control boundary behavior:

| Value                      | Behavior                                   |
| -------------------------- | ------------------------------------------ |
| `[false, false]` (default) | strictly between: `min < value < max`      |
| `[true, false]`            | include lower bound: `min <= value < max`  |
| `[false, true]`            | include upper bound: `min < value <= max`  |
| `[true, true]`             | include both bounds: `min <= value <= max` |

=== "Python"

    ```python
    from sparkdq.checks import TimestampBetweenCheckConfig
    from sparkdq.core import Severity

    TimestampBetweenCheckConfig(
        check_id="event-time-within-range",
        columns=["event_time"],
        min_value="2020-01-01 00:00:00",
        max_value="2023-12-31 23:59:59",
        inclusive=(True, True),
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: timestamp-between-check
      check-id: event-time-within-range
      columns:
        - event_time
      min-value: "2020-01-01 00:00:00"
      max-value: "2023-12-31 23:59:59"
      inclusive: [true, true]
      severity: critical
    ```

## Typical Use Cases

- Ensure log entries or event timestamps fall within a defined processing or reporting window.
- Validate that measurement or sensor data was collected within the expected observation period.
- Reject future-dated or historically stale records before they reach downstream consumers.

---

[← Row-Level Checks](../../row_level.md)
