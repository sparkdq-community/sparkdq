---
wide: true
---

# Exactly One Not Null Check

**Check name**: `exactly-one-not-null-check` · **Type**: row-level · **Config**: `ExactlyOneNotNullCheckConfig`

Enforces that exactly one of the configured columns is non-null per record. A row
fails if none or more than one of the columns are populated. Use it for mutually
exclusive fields such as alternative identifiers or contact channels, where
exactly one option must be chosen.

## Parameters

| Parameter  | Type        | Required | Default    | Description                                                            |
| ---------- | ----------- | -------- | ---------- | ---------------------------------------------------------------------- |
| `check_id` | `str`       | yes      | —          | Unique identifier for this check within the `CheckSet`.                |
| `columns`  | `list[str]` | yes      | —          | Columns among which exactly one must be non-null. YAML key: `columns`. |
| `severity` | `Severity`  | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                   |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import ExactlyOneNotNullCheckConfig
    from sparkdq.core import Severity

    ExactlyOneNotNullCheckConfig(
        check_id="one-contact",
        columns=["phone", "email"],
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: exactly-one-not-null-check
      check-id: one-contact
      columns:
        - phone
        - email
      severity: critical
    ```

## Behavior

- **Exactly-one semantics.** A record passes only when precisely one of the
  listed columns is non-null. It fails both when _zero_ columns are populated and
  when _two or more_ are.
- **Failure is row-level.** Each failing row is annotated in `_dq_errors`;
  passing rows are unaffected. With the default severity, a failure sets
  `_dq_passed = False`.
- **Missing columns raise.** If a configured column does not exist in the
  DataFrame, the check raises `MissingColumnError` at validation time rather than
  silently passing.

## Example

Given a DataFrame that must carry exactly one of `phone` or `email`, both styles
produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import ExactlyOneNotNullCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "phone": "123", "email": None},
        {"id": 2, "phone": None, "email": None},
        {"id": 3, "phone": "555", "email": "b@example.com"},
    ])

    check_set = CheckSet().add_check(
        ExactlyOneNotNullCheckConfig(check_id="one-contact", columns=["phone", "email"])
    )
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

=== "YAML"

    ```python
    import yaml
    from pyspark.sql import SparkSession
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "phone": "123", "email": None},
        {"id": 2, "phone": None, "email": None},
        {"id": 3, "phone": "555", "email": "b@example.com"},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Both violating rows are returned — the one with no contact and the one with two:

```text
+-------------+---+-----+-------------------------------------------------+----------+--------------------------+
|email        |id |phone|_dq_errors                                       |_dq_passed|_dq_validation_ts         |
+-------------+---+-----+-------------------------------------------------+----------+--------------------------+
|NULL         |2  |NULL |[{ExactlyOneNotNullCheck, one-contact, critical}]|false     |2026-01-01 00:00:00.000000|
|b@example.com|3  |555  |[{ExactlyOneNotNullCheck, one-contact, critical}]|false     |2026-01-01 00:00:00.000000|
+-------------+---+-----+-------------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Enforce that exactly one of several optional identifiers (e.g. `email`,
  `phone`, `user_id`) is provided per record.
- Prevent ambiguous records where multiple mutually exclusive fields are
  populated at once.
- Detect rows where no identification channel is present at all.

## Related checks

- [Null Check](null_check.md) — require one or more columns to be populated.
- [Not Null Check](not_null_check.md) — require columns to stay null.

---

[← Row-Level Checks](../../row_level.md)
