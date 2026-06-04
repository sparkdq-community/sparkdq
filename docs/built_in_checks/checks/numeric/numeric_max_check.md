# Numeric Max

**Check**: `numeric-max-check`

**Purpose**: Validates that values in numeric columns do not exceed a defined maximum. A row fails if any selected column contains a value greater than `max_value`.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: false` (default) — `value < max_value`
- `inclusive: true` — `value <= max_value`

=== "Python"

    ```python
    from sparkdq.checks import NumericMaxCheckConfig
    from sparkdq.core import Severity

    NumericMaxCheckConfig(
        check_id="discount-not-above-100",
        columns=["discount"],
        max_value=100.0,
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: numeric-max-check
      check-id: discount-not-above-100
      columns:
        - discount
      max-value: 100.0
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Ensure prices, quantities, or KPI values do not exceed defined upper limits.
- Validate that percentages or scores stay at or below 100.
- Detect data entry errors or system anomalies producing out-of-range values.

---

[← Row-Level Checks](../../row_level.md)
