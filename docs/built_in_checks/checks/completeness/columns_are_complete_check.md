# Columns Are Complete

**Check**: `columns-are-complete-check`

**Purpose**: Ensures that all specified columns are fully populated with no null values. If any null is found in any of the listed columns, the entire dataset is considered invalid. This is an aggregate-level check.

=== "Python"

    ```python
    from sparkdq.checks import ColumnsAreCompleteCheckConfig
    from sparkdq.core import Severity

    ColumnsAreCompleteCheckConfig(
        check_id="required-fields-complete",
        columns=["trip_id", "pickup_time"],
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: columns-are-complete-check
      check-id: required-fields-complete
      columns:
        - trip_id
        - pickup_time
      severity: critical
    ```

## Typical Use Cases

- Enforce that primary keys and mandatory timestamps are fully populated before downstream processing.
- Detect data loss or corruption introduced by ETL failures or schema mismatches.
- Use as an early pipeline gate to quarantine incomplete datasets before they reach consumers.

---

[← Aggregate Checks](../../aggregate.md)
