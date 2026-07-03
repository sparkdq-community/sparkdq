# Count Max

**Check**: `row-count-max-check`

**Purpose**: Validates that the dataset does not exceed a defined maximum number of rows. Use this to detect unexpected data growth, runaway joins, or accidental full loads when only incremental data is expected.

=== "Python"

    ```python
    from sparkdq.checks import RowCountMaxCheckConfig
    from sparkdq.core import Severity

    RowCountMaxCheckConfig(
        check_id="batch-size-upper-bound",
        max_count=100000,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: row-count-max-check
      check-id: batch-size-upper-bound
      max-count: 100000
      severity: critical
    ```

## Typical Use Cases

- Detect abnormal data growth that may indicate duplicates or incorrect joins.
- Prevent downstream systems from processing unexpectedly large datasets.
- Catch accidental full loads when only an incremental extract was intended.

---

[← Aggregate Checks](../../aggregate.md)
