# String Min Length Check

**Check**: `string-min-length-check`

**Purpose**: Validates that non-null string values in a column meet a minimum length requirement. Use this to detect truncated, malformed, or unexpectedly short string data.

!!! note
Null values are treated as valid and are not evaluated by this check.

Use the `inclusive` parameter to control boundary behavior:

- `inclusive: true` (default) — `len(value) >= min_length`
- `inclusive: false` — `len(value) > min_length`

=== "Python"

    ```python
    from sparkdq.checks import StringMinLengthCheckConfig
    from sparkdq.core import Severity

    StringMinLengthCheckConfig(
        check_id="zone-name-min-length",
        column="zone_name",
        min_length=3,
        inclusive=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: string-min-length-check
      check-id: zone-name-min-length
      column: zone_name
      min-length: 3
      inclusive: true
      severity: critical
    ```

## Typical Use Cases

- Ensure that identifiers, codes, or names are not shorter than the minimum meaningful length.
- Detect truncated values caused by data extraction or encoding issues.
- Enforce minimum content requirements on free-text or structured string fields.

---

[← Row-Level Checks](../../row_level.md)
