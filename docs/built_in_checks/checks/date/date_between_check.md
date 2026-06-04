# Date Between

**Check**: `date-between-check`

**Purpose**: Validates that values in date columns fall within a defined range. A row fails if any selected column contains a date before `min_value` or after `max_value`.

Use the `inclusive` parameter to control boundary behavior:

| Value                      | Behavior                                   |
| -------------------------- | ------------------------------------------ |
| `[false, false]` (default) | strictly between: `min < value < max`      |
| `[true, false]`            | include lower bound: `min <= value < max`  |
| `[false, true]`            | include upper bound: `min < value <= max`  |
| `[true, true]`             | include both bounds: `min <= value <= max` |

=== "Python"

    ```python
    from sparkdq.checks import DateBetweenCheckConfig
    from sparkdq.core import Severity

    DateBetweenCheckConfig(
        check_id="record-date-within-range",
        columns=["record_date"],
        min_value="2020-01-01",
        max_value="2023-12-31",
        inclusive=(True, True),
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: date-between-check
      check-id: record-date-within-range
      columns:
        - record_date
      min-value: "2020-01-01"
      max-value: "2023-12-31"
      inclusive: [true, true]
      severity: critical
    ```

## Typical Use Cases

- Ensure transaction or event dates fall within a valid business or fiscal period.
- Prevent processing of records with dates outside the expected reporting window.
- Reject future-dated or historically stale records before they reach downstream consumers.

---

[← Row-Level Checks](../../row_level.md)
