# Date Max

**Check**: `date-max-check`

**Purpose**: Validates that values in date columns do not exceed a defined maximum date. A row fails if any selected column contains a date after `max_value`.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: false` (default) — `value < max_value`
- `inclusive: true` — `value <= max_value`

=== "Python"

    ```python
    from sparkdq.checks import DateMaxCheckConfig
    from sparkdq.core import Severity

    DateMaxCheckConfig(
        check_id="record-date-not-in-future",
        columns=["record_date"],
        max_value="2023-12-31",
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: date-max-check
      check-id: record-date-not-in-future
      columns:
        - record_date
      max-value: "2023-12-31"
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Ensure that event or transaction dates do not lie in the future.
- Enforce a cutoff date for a reporting period or fiscal year end.
- Detect future-dated records caused by data entry errors or timezone misconfiguration.

---

[← Row-Level Checks](../../row_level.md)
