# Regex Match Check

**Check**: `regex-match-check`

**Purpose**: Validates that string values in a column match a given regular expression pattern. Use this to enforce format compliance for structured fields such as email addresses, identifiers, or standardized codes.

!!! note - Null values are skipped by default. Set `treat_null_as_failure: true` to treat them as invalid. - Pattern matching is case-sensitive by default. Use `ignore_case: true` for case-insensitive matching.

=== "Python"

    ```python
    from sparkdq.checks import RegexMatchCheckConfig
    from sparkdq.core import Severity

    RegexMatchCheckConfig(
        check_id="valid-email-format",
        column="email",
        pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
        ignore_case=True,
        treat_null_as_failure=False,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: regex-match-check
      check-id: valid-email-format
      column: email
      pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
      ignore-case: true
      treat-null-as-failure: false
      severity: critical
    ```

## Typical Use Cases

- Validate format compliance for email addresses, phone numbers, or postal codes.
- Enforce structured identifier patterns such as `AB-12345` or `ISO-8601` date strings.
- Detect malformed or free-text values in fields that require a standardized format.

---

[← Row-Level Checks](../../row_level.md)
