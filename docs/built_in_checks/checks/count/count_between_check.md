# Count Between

**Check**: `row-count-between-check`

**Purpose**: Validates that the number of rows falls within a defined minimum and maximum range. Use this to enforce expected data volume and detect both partial loads and unintended data growth.

=== "Python"

    ```python
    from sparkdq.checks import RowCountBetweenCheckConfig
    from sparkdq.core import Severity

    RowCountBetweenCheckConfig(
        check_id="daily-batch-size",
        min_count=1000,
        max_count=5000,
        severity=Severity.ERROR
    )
    ```

=== "YAML"

    ```yaml
    - check: row-count-between-check
      check-id: daily-batch-size
      min-count: 1000
      max-count: 5000
      severity: error
    ```

## Typical Use Cases

- Detect partial loads (too few rows) or unintended duplications (too many rows).
- Validate dataset size before triggering downstream jobs such as model training or reporting.
- Catch filter changes that unintentionally affect row count.

---

[← Aggregate Checks](../../aggregate.md)
