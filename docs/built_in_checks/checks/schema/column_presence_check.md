# Column Presence

**Check**: `column-presence-check`

**Purpose**: Validates that all required columns are present in the DataFrame, regardless of data type. Use this as an early schema guard to prevent downstream failures caused by missing columns.

=== "Python"

    ```python
    from sparkdq.checks import ColumnPresenceCheckConfig
    from sparkdq.core import Severity

    ColumnPresenceCheckConfig(
        check_id="required-columns-present",
        required_columns=["id", "event_timestamp", "status"],
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: column-presence-check
      check-id: required-columns-present
      required-columns:
        - id
        - event_timestamp
        - status
      severity: critical
    ```

## Typical Use Cases

- Detect schema regressions where key columns are accidentally dropped by upstream producers.
- Validate that technical metadata columns such as `event_timestamp` or `partition_key` are present.
- Prevent silent failures in transformation logic that depends on specific column names.

---

[← Aggregate Checks](../../aggregate.md)
