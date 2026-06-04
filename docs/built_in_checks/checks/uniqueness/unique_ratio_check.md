# Unique Ratio

**Check**: `unique-ratio-check`

**Purpose**: Validates that the ratio of unique non-null values in a column meets or exceeds a defined threshold. The check fails if the proportion of unique values falls below `min_ratio`.

!!! note
Null values are excluded from the uniqueness calculation. The total number of rows (including nulls) is used as the denominator.

=== "Python"

    ```python
    from sparkdq.checks import UniqueRatioCheckConfig
    from sparkdq.core import Severity

    UniqueRatioCheckConfig(
        check_id="order-id-unique-ratio",
        column="order_id",
        min_ratio=0.95,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: unique-ratio-check
      check-id: order-id-unique-ratio
      column: order_id
      min-ratio: 0.95
      severity: critical
    ```

## Typical Use Cases

- Ensure that columns expected to be mostly unique (e.g., IDs, hashes, transaction codes) behave as intended.
- Detect high-cardinality violations where a small set of values is unexpectedly repeated at scale.
- Support feature quality validation in ML preprocessing pipelines.

---

[← Aggregate Checks](../../aggregate.md)
