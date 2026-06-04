# Not Null Check

**Check**: `not-null-check`

**Purpose**: Ensures that the specified columns contain at least one non-null value. Use this check to detect columns that are completely empty, which typically indicates a broken upstream feed or a deprecated field still present in the schema.

=== "Python"

    ```python
    from sparkdq.checks import NotNullCheckConfig
    from sparkdq.core import Severity

    NotNullCheckConfig(
        check_id="deactivated-at-has-values",
        columns=["deactivated_at"],
        severity=Severity.WARNING
    )
    ```

=== "YAML"

    ```yaml
    - check: not-null-check
      check-id: deactivated-at-has-values
      columns:
        - deactivated_at
      severity: warning
    ```

## Typical Use Cases

- Detect columns that are fully empty due to upstream data issues or broken feeds.
- Identify deprecated schema fields that are no longer populated.
- Guard against silent data loss where an entire column goes missing after a pipeline change.

---

[← Row-Level Checks](../../row_level.md)
