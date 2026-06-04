# Foreign Key Check

**Check**: `foreign-key-check`

**Purpose**: Validates that all values in a column exist in a reference dataset's column. The check fails if any value in the source column cannot be resolved in the referenced column. This is an aggregate-level check that reports the count and ratio of unresolvable references.

=== "Python"

    ```python
    from sparkdq.checks import ForeignKeyCheckConfig
    from sparkdq.core import Severity

    ForeignKeyCheckConfig(
        check_id="customer-id-resolvable",
        column="customer_id",
        reference_dataset="customers",
        reference_column="id",
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: foreign-key-check
      check-id: customer-id-resolvable
      column: customer_id
      reference-dataset: customers
      reference-column: id
      severity: critical
    ```

## Typical Use Cases

- Ensure every `order.customer_id` resolves to an existing entry in the `customers` table.
- Validate foreign key references in fact tables against known dimension entries.
- Enforce referential integrity between datasets in data lakes or data warehouses.

---

[← Aggregate Checks](../../aggregate.md)
