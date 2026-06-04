# Completeness Ratio

**Check**: `completeness-ratio-check`

**Purpose**: Validates that the ratio of non-null values in a column meets or exceeds a defined threshold. Use this for soft completeness validation on fields that are expected to be mostly populated but may tolerate a small proportion of missing values.

=== "Python"

    ```python
    from sparkdq.checks import CompletenessRatioCheckConfig
    from sparkdq.core import Severity

    CompletenessRatioCheckConfig(
        check_id="pickup-time-completeness",
        column="pickup_datetime",
        min_ratio=0.95,
        severity=Severity.WARNING
    )
    ```

=== "YAML"

    ```yaml
    - check: completeness-ratio-check
      check-id: pickup-time-completeness
      column: pickup_datetime
      min-ratio: 0.95
      severity: warning
    ```

## Typical Use Cases

- Detect columns with an unexpectedly high proportion of missing values.
- Enforce soft completeness thresholds on optional or partially-populated fields.
- Provide early signals for upstream data loss or extraction failures.

---

[← Aggregate Checks](../../aggregate.md)
