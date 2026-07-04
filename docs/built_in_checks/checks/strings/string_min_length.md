---
wide: true
---

# String Min Length Check

**Check name**: `string-min-length-check` · **Type**: row-level · **Config**: `StringMinLengthCheckConfig`

Flags any record whose string value in the configured column is shorter than a
minimum length. Use it to catch truncated, malformed, or unexpectedly short
values such as codes or identifiers.

## Parameters

| Parameter    | Type       | Required | Default    | Description                                             |
| ------------ | ---------- | -------- | ---------- | ------------------------------------------------------- |
| `check_id`   | `str`      | yes      | —          | Unique identifier for this check within the `CheckSet`. |
| `column`     | `str`      | yes      | —          | The string column to validate (a single column).        |
| `min_length` | `int`      | yes      | —          | Minimum length; must be `> 0`. YAML key: `min-length`.  |
| `inclusive`  | `bool`     | no       | `True`     | Whether `min_length` itself is allowed (see Behavior).  |
| `severity`   | `Severity` | no       | `CRITICAL` | `CRITICAL` fails the row; `WARNING` only records it.    |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import StringMinLengthCheckConfig
    from sparkdq.core import Severity

    StringMinLengthCheckConfig(
        check_id="min-code",
        column="code",
        min_length=3,
        inclusive=True,
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: string-min-length-check
      check-id: min-code
      column: code
      min-length: 3
      inclusive: true
      severity: critical
    ```

## Behavior

- **Null values pass.** Only non-null strings are evaluated; a null in the column
  never fails this check. Combine with a [Null Check](../null/null_check.md) if the
  column must also be populated.
- **`inclusive` controls the boundary, and defaults to `True`.** With the default,
  a string of exactly `min_length` characters **passes** (`length >= min_length`).
  With `inclusive=False`, that boundary length **fails** (`length > min_length`).
- **Single column.** Unlike the numeric checks, this check takes one `column`.
- **`min_length` must be positive**, or the config raises
  `InvalidCheckConfigurationError` before any data is touched.
- **Missing column raises.** If the column does not exist, the check raises
  `MissingColumnError` at validation time.

## Example

Requiring `code` to be at least 3 characters, both styles produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import StringMinLengthCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "code": "abc"},
        {"id": 2, "code": "ab"},
        {"id": 3, "code": None},
    ])

    check_set = CheckSet().add_check(
        StringMinLengthCheckConfig(check_id="min-code", column="code", min_length=3)
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
        {"id": 1, "code": "abc"},
        {"id": 2, "code": "ab"},
        {"id": 3, "code": None},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

Only the too-short value fails; the null row passes:

```text
+----+---+--------------------------------------------+----------+--------------------------+
|code|id |_dq_errors                                  |_dq_passed|_dq_validation_ts         |
+----+---+--------------------------------------------+----------+--------------------------+
|ab  |2  |[{StringMinLengthCheck, min-code, critical}]|false     |2026-01-01 00:00:00.000000|
+----+---+--------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Ensure codes, identifiers, or names meet a minimum meaningful length.
- Detect truncated values from extraction or encoding issues.
- Enforce minimum content on structured string fields.

## Related checks

- [String Max Length Check](string_max_length.md) — enforce an upper length bound.
- [String Between Length Check](string_between_length.md) — enforce both length bounds.
- [Regex Match Check](regex_match_check.md) — validate the value's format, not just its length.

---

[← Row-Level Checks](../../row_level.md)
