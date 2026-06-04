# Unique Rows

**Check**: `unique-rows-check`

**Purpose**: Validates that all rows in the dataset are unique, either across all columns or a defined subset. The check fails on any duplicate row within the evaluated scope.

!!! note
If no `subset_columns` are provided, uniqueness is evaluated across all columns.

=== "Python"

    ```python
    from sparkdq.checks import UniqueRowsCheckConfig
    from sparkdq.core import Severity

    UniqueRowsCheckConfig(
        check_id="no-duplicate-trips",
        subset_columns=["trip_id", "pickup_time"],
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: unique-rows-check
      check-id: no-duplicate-trips
      subset-columns:
        - trip_id
        - pickup_time
      severity: critical
    ```

## Typical Use Cases

- Enforce uniqueness on primary key–like columns such as `trip_id` or `user_id`.
- Detect duplicate records introduced by faulty joins, reprocessing, or double ingestion.
- Validate the correctness of deduplication logic before writing to transactional stores.

---

[← Aggregate Checks](../../aggregate.md)
