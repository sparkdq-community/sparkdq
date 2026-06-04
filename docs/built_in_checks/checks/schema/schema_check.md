# Schema Check

**Check**: `schema-check`

**Purpose**: Validates that the DataFrame matches an expected schema in terms of column names and Spark data types. Optionally enforces strict matching to reject unexpected additional columns.

## Supported Data Types

The following Spark types are supported and must be specified as lowercase strings:

`string`, `boolean`, `int`, `bigint`, `float`, `double`, `date`, `timestamp`, `binary`, `array`, `map`, `struct`, `decimal(precision, scale)` — e.g., `decimal(10,2)`

!!! important
For `decimal` types, both precision and scale must be specified inside parentheses. Formats such as `integer` or `decimal(10.2)` are not accepted.

=== "Python"

    ```python
    from sparkdq.checks import SchemaCheckConfig
    from sparkdq.core import Severity

    SchemaCheckConfig(
        check_id="enforce-schema-contract",
        expected_schema={
            "id": "int",
            "name": "string",
            "amount": "decimal(10,2)",
            "created_at": "timestamp"
        },
        strict=True,
        severity=Severity.CRITICAL
    )
    ```

=== "YAML"

    ```yaml
    - check: schema-check
      check-id: enforce-schema-contract
      expected-schema:
        id: int
        name: string
        amount: decimal(10,2)
        created_at: timestamp
      strict: true
      severity: critical
    ```

## Typical Use Cases

- Enforce schema contracts between ingestion, transformation, and consumption stages.
- Detect missing, renamed, or type-changed columns introduced by upstream schema evolution.
- Prevent silent data corruption caused by implicit type casting on incorrect column types.

---

[← Aggregate Checks](../../aggregate.md)
