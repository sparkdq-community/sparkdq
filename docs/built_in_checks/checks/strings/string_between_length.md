---
wide: true
---

# String Length Between Check

**Check name**: `string-length-between-check` · **Type**: row-level · **Config**: `StringLengthBetweenCheckConfig`

Flags any record whose string value in the configured column has a length outside
a `[min, max]` range. Use it to keep values within a bounded format — neither too
short nor too long.

## Parameters

| Parameter    | Type                | Required | Default        | Description                                                |
| ------------ | ------------------- | -------- | -------------- | ---------------------------------------------------------- |
| `check_id`   | `str`               | yes      | —              | Unique identifier for this check within the `CheckSet`.    |
| `column`     | `str`               | yes      | —              | The string column to validate (a single column).           |
| `min_length` | `int`               | yes      | —              | Lower length bound; must be `> 0`. YAML key: `min-length`. |
| `max_length` | `int`               | yes      | —              | Upper length bound. YAML key: `max-length`.                |
| `inclusive`  | `tuple[bool, bool]` | no       | `(True, True)` | Inclusivity of the lower and upper bound respectively.     |
| `severity`   | `Severity`          | no       | `CRITICAL`     | `CRITICAL` fails the row; `WARNING` only records it.       |

## Usage

=== "Python"

    ```python
    from sparkdq.checks import StringLengthBetweenCheckConfig
    from sparkdq.core import Severity

    StringLengthBetweenCheckConfig(
        check_id="tag-len",
        column="tag",
        min_length=2,
        max_length=4,
        inclusive=(True, True),
        severity=Severity.CRITICAL,
    )
    ```

=== "YAML"

    ```yaml
    - check: string-length-between-check
      check-id: tag-len
      column: tag
      min-length: 2
      max-length: 4
      inclusive: [true, true]
      severity: critical
    ```

## Behavior

- **Null values pass.** Only non-null strings are evaluated.
- **`inclusive` is a `(lower, upper)` pair and defaults to `(True, True)`** — i.e.
  both bounds are allowed by default. Each bound is controlled independently:

  | `inclusive`      | Passing length range   |
  | ---------------- | ---------------------- |
  | `(True, True)`   | `min <= length <= max` |
  | `(True, False)`  | `min <= length < max`  |
  | `(False, True)`  | `min < length <= max`  |
  | `(False, False)` | `min < length < max`   |

- **Single column.** This check takes one `column`.
- **`min_length` must be positive**, or the config raises
  `InvalidCheckConfigurationError` before any data is touched.
- **Missing column raises.** If the column does not exist, the check raises
  `MissingColumnError` at validation time.

## Example

Requiring `tag` length within `[2, 4]` (both bounds inclusive), both styles
produce the same result.

=== "Python"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import StringLengthBetweenCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame([
        {"id": 1, "tag": "ab"},
        {"id": 2, "tag": "a"},
        {"id": 3, "tag": "abcde"},
    ])

    check_set = CheckSet().add_check(
        StringLengthBetweenCheckConfig(
            check_id="tag-len", column="tag", min_length=2, max_length=4, inclusive=(True, True)
        )
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
        {"id": 1, "tag": "ab"},
        {"id": 2, "tag": "a"},
        {"id": 3, "tag": "abcde"},
    ])

    with open("checks.yml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)
    result = BatchDQEngine(check_set).run_batch(df)
    result.fail_df().show(truncate=False)
    ```

The too-short and too-long values fail; the in-range value passes:

```text
+---+-----+-----------------------------------------------+----------+--------------------------+
|id |tag  |_dq_errors                                     |_dq_passed|_dq_validation_ts         |
+---+-----+-----------------------------------------------+----------+--------------------------+
|2  |a    |[{StringLengthBetweenCheck, tag-len, critical}]|false     |2026-01-01 00:00:00.000000|
|3  |abcde|[{StringLengthBetweenCheck, tag-len, critical}]|false     |2026-01-01 00:00:00.000000|
+---+-----+-----------------------------------------------+----------+--------------------------+
```

## Typical use cases

- Enforce minimum and maximum length on user-entered text fields.
- Detect truncated or overlong values from integration or migration issues.
- Validate that identifiers or codes conform to a bounded-length format.

## Related checks

- [String Min Length Check](string_min_length.md) — enforce only a lower length bound.
- [String Max Length Check](string_max_length.md) — enforce only an upper length bound.
- [Regex Match Check](regex_match_check.md) — validate the value's format.

---

[← Row-Level Checks](../../row_level.md)
