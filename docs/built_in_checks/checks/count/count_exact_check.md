# Count Exact

**Check**: `row-count-exact-check`

**Purpose**: Validates that the dataset contains exactly the expected number of rows. Use this to enforce strict volume contracts, particularly for fixed-size exports, snapshots, or reference datasets.

=== "Python"

    ```python
    from sparkdq.checks import RowCountExactCheckConfig
    from sparkdq.core import Severity

    RowCountExactCheckConfig(
        check_id="snapshot-row-count",
        expected_count=500,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: row-count-exact-check
      check-id: snapshot-row-count
      expected-count: 500
      severity: critical
    ```

## Typical Use Cases

- Validate fixed-size imports where exactly a known number of records is expected.
- Ensure integrity of snapshot-based pipelines with predictable record counts.
- Detect silent load failures or unintended duplications early in the process.

---

[← Aggregate Checks](../../aggregate.md)
