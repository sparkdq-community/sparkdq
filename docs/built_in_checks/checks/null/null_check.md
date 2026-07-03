# Null Check

**Check**: `null-check`

**Purpose**: Ensures that the specified columns contain no null values. Use this check to enforce completeness of mandatory fields and prevent incomplete records from entering downstream processes.

=== "Python"

    ```python
    from sparkdq.checks import NullCheckConfig
    from sparkdq.core import Severity

    NullCheckConfig(
        check_id="no-null-email",
        columns=["email"],
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: null-check
      check-id: no-null-email
      columns:
        - email
      severity: critical
    ```

## Typical Use Cases

- Enforce completeness of primary keys, foreign keys, and business-critical attributes.
- Prevent incomplete records from reaching downstream transformations or reports.
- Detect data gaps introduced by upstream extraction or ingestion failures.

---

[← Row-Level Checks](../../row_level.md)
