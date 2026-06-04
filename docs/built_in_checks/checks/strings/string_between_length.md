# String Length Between Check

**Check**: `string-length-between-check`

**Purpose**: Validates that non-null string values in a column have a length within a specified range. Use this to ensure strings are neither too short nor too long — useful for validating user input, enforcing field formatting, or detecting truncation issues.

!!! note
Null values are treated as valid and are not evaluated by this check.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: [true, false]` → `min_length <= len(value) < max_length`
- `inclusive: [false, true]` → `min_length < len(value) <= max_length`
- `inclusive: [true, true]` → `min_length <= len(value) <= max_length`

=== "Python"

    ```python
    from sparkdq.checks import StringLengthBetweenCheckConfig
    from sparkdq.core import Severity

    StringLengthBetweenCheckConfig(
        check_id="zone-name-length",
        column="zone_name",
        min_length=3,
        max_length=50,
        inclusive=(True, True),
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: string-length-between-check
      check-id: zone-name-length
      column: zone_name
      min-length: 3
      max-length: 50
      inclusive: [true, true]
      severity: critical
    ```

## Typical Use Cases

- Enforce minimum and maximum length constraints on user-entered text fields.
- Detect truncated or excessively long values introduced by integration or migration issues.
- Validate that identifiers or codes conform to a known fixed-length or bounded-length format.

---

[← Row-Level Checks](../../row_level.md)
