---
wide: true
---

# String Max Length Check

**Check name**: `string-max-length-check` · **Type**: row-level · **Config**: `StringMaxLengthCheckConfig`

Flags any record whose string value in the configured column is longer than a
maximum length. Use it to catch overflowing, padded, or malformed values, or to
enforce limits aligned with a database column or UI field.

## Parameters

| Parameter    | Type       | Required | Default    | Description                                             |
| ------------ | ---------- | -------- | ---------- | ------------------------------------------------------- |
| `check_id`   | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`. |
| `column`     | `str`      | yes      | —          | The string column to validate (a single column).        |
| `max_length` | `int`      | yes      | —          | Maximum length. YAML key: `max-length`.                 |
| `inclusive`  | `bool`     | no       | `True`     | Whether `max_length` itself is allowed (see Behavior).  |
| `severity`   | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.    |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import StringMaxLengthCheckConfig
    from sparkdq.core import Severity

    StringMaxLengthCheckConfig(
        check_id="max-name",
        column="name",
        max_length=5,
        inclusive=True,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: string-max-length-check
      check-id: max-name
      column: name
      max-length: 5
      inclusive: true
      severity: critical
    ```

## Behavior

- **Null values pass.** Only non-null strings are evaluated; a null in the column
  never fails this check.
- **`inclusive` controls the boundary, and defaults to `True`.** With the default,
  a string of exactly `max_length` characters **passes** (`length <= max_length`).
  With `inclusive=False`, that boundary length **fails** (`length < max_length`).
- **Single column.** This check takes one `column`.
- **Missing column raises.** If the column does not exist, the check raises
  `MissingColumnError` at validation time.

## Example

Requiring `name` to be at most 5 characters, both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import StringMaxLengthCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "name": "Bob"},
        {"id": 2, "name": "Bartholomew"},
    ])

    check_set = CheckSet().add_check(
        StringMaxLengthCheckConfig(check_id="max-name", column="name", max_length=5)
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
        {"id": 1, "name": "Bob"},
        {"id": 2, "name": "Bartholomew"},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the overlong value fails:

```text
+---+-----------+--------------------------------------------+----------+--------------------------+
|id |name       |_dq_errors                                  |_dq_passed|_dq_validation_ts         |
+---+-----------+--------------------------------------------+----------+--------------------------+
|2  |Bartholomew|[{StringMaxLengthCheck, max-name, critical}]|false     |2026-01-01 00:00:00.000000|
+---+-----------+--------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Detect padded or overlong values from integration bugs or data entry errors.
- Enforce length limits aligned with database schema or UI field constraints.
- Identify legacy fields carrying excess content.

## Related checks

- [String Min Length Check](string_min_length.md) — enforce a lower length bound.
- [String Between Length Check](string_between_length.md) — enforce both length bounds.
- [Regex Match Check](regex_match_check.md) — validate the value's format.

---

[← Row-Level Checks](../../row_level.md)
