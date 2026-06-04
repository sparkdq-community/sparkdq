# Exactly One Not Null Check

**Check**: `exactly-one-not-null-check`

**Purpose**: Enforces that exactly one of the specified columns is non-null per row. A row fails if none or more than one of the columns are populated. Use this to enforce mutual exclusivity across optional identifiers or contact channels.

=== "Python"

    ```python
    from sparkdq.checks import ExactlyOneNotNullCheckConfig
    from sparkdq.core import Severity

    ExactlyOneNotNullCheckConfig(
        check_id="single-contact-method",
        columns=["email", "phone", "user_id"],
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: exactly-one-not-null-check
      check-id: single-contact-method
      columns:
        - email
        - phone
        - user_id
      severity: critical
    ```

## Typical Use Cases

- Enforce that exactly one of several optional identifiers (e.g., email, phone, user_id) is provided per record.
- Prevent ambiguous records where multiple mutually exclusive fields are populated simultaneously.
- Detect rows where no identification channel is present at all.

---

[← Row-Level Checks](../../row_level.md)
