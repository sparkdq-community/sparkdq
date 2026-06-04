# Column Greater Than Check

**Check**: `column-greater-than-check`

**Purpose**: Enforces that values in one column are strictly greater than (or greater than or equal to) values in another column or the result of a Spark SQL expression. Rows with null values in either operand are treated as invalid.

Use the `inclusive` flag to control boundary behavior:

- `inclusive: false` (default) — requires `column > limit`
- `inclusive: true` — allows `column >= limit`

=== "Python"

    ```python
    from sparkdq.checks import ColumnGreaterThanCheckConfig
    from sparkdq.core import Severity

    ColumnGreaterThanCheckConfig(
        check_id="dropoff-after-pickup",
        column="dropoff_datetime",
        limit="pickup_datetime",
        inclusive=False,
        severity=Severity.CRITICAL
    )

    ColumnGreaterThanCheckConfig(
        check_id="selling-price-above-cost",
        column="selling_price",
        limit="cost_price * 1.2",
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: column-greater-than-check
      check-id: dropoff-after-pickup
      column: dropoff_datetime
      limit: pickup_datetime
      inclusive: false
      severity: critical

    - check: column-greater-than-check
      check-id: selling-price-above-cost
      column: selling_price
      limit: cost_price * 1.2
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Enforce temporal ordering, such as ensuring dropoff time is after pickup time.
- Validate that selling price exceeds a minimum margin above cost price.
- Apply dynamic business rules using Spark SQL expressions as the comparison limit.

---

[← Row-Level Checks](../../row_level.md)
