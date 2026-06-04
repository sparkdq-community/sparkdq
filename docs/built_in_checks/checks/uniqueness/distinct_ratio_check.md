# Distinct Ratio

**Check**: `distinct-ratio-check`

**Purpose**: Validates that the ratio of distinct non-null values in a column meets or exceeds a defined threshold. The check fails if the actual distinct ratio falls below `min_ratio`.

=== "Python"

    ```python
    from sparkdq.checks import DistinctRatioCheckConfig
    from sparkdq.core import Severity

    DistinctRatioCheckConfig(
        check_id="category-distinct-ratio",
        column="category",
        min_ratio=0.8,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: distinct-ratio-check
      check-id: category-distinct-ratio
      column: category
      min-ratio: 0.8
      severity: critical
    ```

## Typical Use Cases

- Detect columns with excessive repetition or insufficient value diversity.
- Identify constant-filled, default-padded, or data-entry-error-prone fields.
- Enforce minimum entropy requirements for features used in ML models or analytics pipelines.

---

[← Aggregate Checks](../../aggregate.md)
