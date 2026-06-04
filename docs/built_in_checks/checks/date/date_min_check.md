# Date Min

**Check**: `date-min-check`

**Purpose**: Validates that values in date columns are not earlier than a defined minimum date. A row fails if any selected column contains a date before `min_value`.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: false` (default) — `value > min_value`
- `inclusive: true` — `value >= min_value`

=== "Python"

    ```python
    from sparkdq.checks import DateMinCheckConfig
    from sparkdq.core import Severity

    DateMinCheckConfig(
        check_id="record-date-not-before-start",
        columns=["record_date"],
        min_value="2020-01-01",
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: date-min-check
      check-id: record-date-not-before-start
      columns:
        - record_date
      min-value: "2020-01-01"
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Ensure that event or transaction dates are not earlier than the defined operational start date.
- Reject legacy or historically stale records outside the valid business period.
- Validate that data entries are aligned with a system go-live or migration cutover date.

---

[← Row-Level Checks](../../row_level.md)
