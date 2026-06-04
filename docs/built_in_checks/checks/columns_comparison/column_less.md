# Column Less Than Check

**Check**: `column-less-than-check`

**Purpose**: Enforces that values in one column are strictly less than (or less than or equal to) values in another column or the result of a Spark SQL expression. Rows with null values in either operand are treated as invalid.

Use the `inclusive` flag to control boundary behavior:

- `inclusive: false` (default) — requires `column < limit`
- `inclusive: true` — allows `column <= limit`

=== "Python"

    ```python
    from sparkdq.checks import ColumnLessThanCheckConfig
    from sparkdq.core import Severity

    ColumnLessThanCheckConfig(
        check_id="pickup-before-dropoff",
        column="pickup_datetime",
        limit="dropoff_datetime",
        inclusive=False,
        severity=Severity.CRITICAL
    )

    ColumnLessThanCheckConfig(
        check_id="cost-below-selling-price",
        column="cost_price",
        limit="selling_price * 0.8",
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: column-less-than-check
      check-id: pickup-before-dropoff
      column: pickup_datetime
      limit: dropoff_datetime
      inclusive: false
      severity: critical

    - check: column-less-than-check
      check-id: cost-below-selling-price
      column: cost_price
      limit: selling_price * 0.8
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Enforce temporal ordering, such as ensuring pickup time is before dropoff time.
- Validate that cost price stays within an acceptable fraction of selling price.
- Apply dynamic business rules using Spark SQL expressions as the comparison limit.

---

[← Row-Level Checks](../../row_level.md)
