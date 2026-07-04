---
wide: true
---

# Regex Match Check

**Check name**: `regex-match-check` · **Type**: row-level · **Config**: `RegexMatchCheckConfig`

Flags any record whose string value in the configured column does not match a
regular expression. Use it to enforce format compliance for structured fields
such as email addresses, identifiers, or standardized codes.

## Parameters

| Parameter               | Type       | Required | Default    | Description                                                         |
| ----------------------- | ---------- | -------- | ---------- | ------------------------------------------------------------------- |
| `check_id`              | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`.             |
| `column`                | `str`      | yes      | —          | The string column to validate (a single column).                    |
| `pattern`               | `str`      | yes      | —          | The regular expression the value must match (Spark `rlike` syntax). |
| `ignore_case`           | `bool`     | no       | `False`    | Case-insensitive matching. YAML key: `ignore-case`.                 |
| `treat_null_as_failure` | `bool`     | no       | `False`    | Treat nulls as failures. YAML key: `treat-null-as-failure`.         |
| `severity`              | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.                |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import RegexMatchCheckConfig
    from sparkdq.core import Severity

    RegexMatchCheckConfig(
        check_id="email-format",
        column="email",
        pattern=r"^[^@]+@[^@]+\.[^@]+$",
        ignore_case=True,
        treat_null_as_failure=False,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: regex-match-check
      check-id: email-format
      column: email
      pattern: "^[^@]+@[^@]+\\.[^@]+$"
      ignore-case: true
      treat-null-as-failure: false
      severity: critical
    ```

## Behavior

- **Null handling is configurable.** By default (`treat_null_as_failure=False`),
  null values are **skipped** and pass. Set `treat_null_as_failure=True` to fail
  nulls as well.
- **Case sensitivity.** Matching is case-sensitive unless `ignore_case=True`,
  which applies the `(?i)` flag to the pattern.
- **Match semantics.** A row fails when a non-null value does _not_ match the
  pattern. The pattern uses Spark's `rlike` (Java regex) syntax; in YAML,
  backslashes must be escaped (`\\.`).
- **Single column.** This check takes one `column`.
- **Missing column raises.** If the column does not exist, the check raises
  `MissingColumnError` at validation time.

## Example

Requiring `email` to look like an address, both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import RegexMatchCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "nope"},
    ])

    check_set = CheckSet().add_check(
        RegexMatchCheckConfig(check_id="email-format", column="email", pattern=r"^[^@]+@[^@]+\.[^@]+$")
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
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "nope"},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the malformed value fails:

```text
+-----+---+-------------------------------------------+----------+--------------------------+
|email|id |_dq_errors                                 |_dq_passed|_dq_validation_ts         |
+-----+---+-------------------------------------------+----------+--------------------------+
|nope |2  |[{RegexMatchCheck, email-format, critical}]|false     |2026-01-01 00:00:00.000000|
+-----+---+-------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Validate format compliance for email addresses, phone numbers, or postal codes.
- Enforce structured identifier patterns such as `AB-12345`.
- Detect malformed or free-text values in fields that require a standardized format.

## Related checks

- [String Between Length Check](string_between_length.md) — constrain length rather than format.
- [Is Contained In Check](../contained_in/is_contained_in_check.md) — restrict to a fixed set of allowed values.

---

[← Row-Level Checks](../../row_level.md)
