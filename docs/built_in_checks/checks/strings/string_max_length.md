# String Max Length Check

**Check**: `string-max-length-check`

**Purpose**: Validates that non-null string values in a column do not exceed a defined maximum length. Use this to detect overflow, incorrectly padded, or malformed string data.

!!! note
Null values are treated as valid and are not evaluated by this check.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: true` (default) — `len(value) <= max_length`
- `inclusive: false` — `len(value) < max_length`

=== "Python"

    ```python
    from sparkdq.checks import StringMaxLengthCheckConfig
    from sparkdq.core import Severity

    StringMaxLengthCheckConfig(
        check_id="zone-name-max-length",
        column="zone_name",
        max_length=50,
        inclusive=True,
        severity=Severity.WARNING
    )
    ```

=== "YAML"

    ```yaml
    - check: string-max-length-check
      check-id: zone-name-max-length
      column: zone_name
      max-length: 50
      inclusive: true
      severity: warning
    ```

## Typical Use Cases

- Detect padded or overlong string values produced by integration bugs or data entry errors.
- Enforce length constraints aligned with database schema column definitions or UI field limits.
- Identify legacy fields carrying excess content that exceeds the expected value format.

---

[← Row-Level Checks](../../row_level.md)
