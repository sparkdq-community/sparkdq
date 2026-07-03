# Is Contained In

**Check**: `is-contained-in-check`

**Purpose**: Validates that column values belong to a predefined set of allowed values. Use this to enforce domain constraints and catch unexpected or invalid categorical values early in the pipeline.

This check supports any data type comparable by equality (strings, integers, dates, etc.).

=== "Python"

    ```python
    from sparkdq.checks import IsContainedInCheckConfig
    from sparkdq.core import Severity

    IsContainedInCheckConfig(
        check_id="valid-status-and-country",
        allowed_values={
            "status": ["ACTIVE", "INACTIVE", "PENDING"],
            "country": ["DE", "FR", "IT"]
        },
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: is-contained-in-check
      check-id: valid-status-and-country
      allowed-values:
        status:
          - ACTIVE
          - INACTIVE
          - PENDING
        country:
          - DE
          - FR
          - IT
      severity: critical
    ```

## Typical Use Cases

- Validate that status fields contain only permitted operational states.
- Ensure country codes or region identifiers belong to a known reference set.
- Detect ingestion errors where unrecognized or malformed categorical values are introduced.

---

[← Row-Level Checks](../../row_level.md)
