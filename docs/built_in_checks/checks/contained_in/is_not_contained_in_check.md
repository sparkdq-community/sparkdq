# Is Not Contained In

**Check**: `is-not-contained-in-check`

**Purpose**: Validates that column values do not belong to a defined set of forbidden values. Use this to block known invalid states, test data, or disallowed categories from entering the pipeline.

This check supports any data type comparable by equality (strings, integers, dates, etc.).

=== "Python"

    ```python
    from sparkdq.checks import IsNotContainedInCheckConfig
    from sparkdq.core import Severity

    IsNotContainedInCheckConfig(
        check_id="no-test-or-invalid-entries",
        forbidden_values={
            "status": ["TEST", "DUMMY"],
            "country": ["XX"]
        },
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: is-not-contained-in-check
      check-id: no-test-or-invalid-entries
      forbidden-values:
        status:
          - TEST
          - DUMMY
        country:
          - XX
      severity: critical
    ```

## Typical Use Cases

- Block test or dummy records from being processed in production datasets.
- Reject entries with invalid country codes or placeholder values.
- Detect rows carrying known-bad status values that should have been filtered upstream.

---

[← Row-Level Checks](../../row_level.md)
