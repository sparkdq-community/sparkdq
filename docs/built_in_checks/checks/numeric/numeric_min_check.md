# Numeric Min

**Check**: `numeric-min-check`

**Purpose**: Validates that values in numeric columns are not below a defined minimum. A row fails if any selected column contains a value less than `min_value`.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: false` (default) — `value > min_value`
- `inclusive: true` — `value >= min_value`

=== "Python"

    ```python
    from sparkdq.checks import NumericMinCheckConfig
    from sparkdq.core import Severity

    NumericMinCheckConfig(
        check_id="price-not-negative",
        columns=["price", "discount"],
        min_value=0.0,
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: numeric-min-check
      check-id: price-not-negative
      columns:
        - price
        - discount
      min-value: 0.0
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Ensure monetary amounts, quantities, or measurements are non-negative.
- Enforce minimum threshold requirements for KPIs or scoring metrics.
- Detect corrupted or invalid numeric values below the physically meaningful range.

---

[← Row-Level Checks](../../row_level.md)
