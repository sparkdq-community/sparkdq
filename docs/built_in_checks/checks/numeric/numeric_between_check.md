# Numeric Between

**Check**: `numeric-between-check`

**Purpose**: Validates that values in numeric columns fall within a defined range. A row fails if any selected column contains a value outside the configured bounds.

Use the `inclusive` parameter to control boundary behavior:

| Value                      | Behavior                                   |
| -------------------------- | ------------------------------------------ |
| `[false, false]` (default) | strictly between: `min < value < max`      |
| `[true, false]`            | include lower bound: `min <= value < max`  |
| `[false, true]`            | include upper bound: `min < value <= max`  |
| `[true, true]`             | include both bounds: `min <= value <= max` |

=== "Python"

    ```python
    from sparkdq.checks import NumericBetweenCheckConfig
    from sparkdq.core import Severity

    NumericBetweenCheckConfig(
        check_id="discount-within-valid-range",
        columns=["discount"],
        min_value=0.0,
        max_value=100.0,
        inclusive=(True, True),
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: numeric-between-check
      check-id: discount-within-valid-range
      columns:
        - discount
      min-value: 0.0
      max-value: 100.0
      inclusive: [true, true]
      severity: critical
    ```

## Typical Use Cases

- Validate that percentages, ratios, or scores fall within their expected range (e.g., 0–100).
- Enforce physical or business-defined constraints on numeric measurements.
- Detect outliers or erroneous values that fall outside acceptable thresholds.

---

[← Row-Level Checks](../../row_level.md)
