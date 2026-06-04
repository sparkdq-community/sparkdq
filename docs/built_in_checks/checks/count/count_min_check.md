# Count Min

**Check**: `row-count-min-check`

**Purpose**: Validates that the dataset contains at least a defined minimum number of rows. Use this to prevent downstream processing on incomplete or unexpectedly small datasets.

=== "Python"

    ```python
    from sparkdq.checks import RowCountMinCheckConfig
    from sparkdq.core import Severity

    RowCountMinCheckConfig(
        check_id="minimum-record-count",
        min_count=10000,
        severity=Severity.WARNING
    )
    ```

=== "YAML"

    ```yaml
    - check: row-count-min-check
      check-id: minimum-record-count
      min-count: 10000
      severity: warning
    ```

## Typical Use Cases

- Detect partial loads or failed data transfers that result in fewer records than expected.
- Enforce minimum data volume requirements for reliable analytics, reporting, or model training.
- Prevent downstream processes from running on datasets that are too small to be meaningful.

---

[← Aggregate Checks](../../aggregate.md)
