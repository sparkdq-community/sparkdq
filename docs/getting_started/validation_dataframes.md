# Validating DataFrames

Take the `check_set` from the [previous step](defining_checks.md) and the
`orders` DataFrame from the [introduction](introduction.md), and pass both to a
`BatchDQEngine`. The engine evaluates every rule in a single pass and returns a
`BatchValidationResult`:

```python
from sparkdq.engine import BatchDQEngine

result = BatchDQEngine(check_set).run_batch(df)
```

## What the engine does

In a single pass, the engine:

- applies each row-level check and flags failing rows individually,
- evaluates each aggregate check against the full DataFrame,
- annotates every row with `_dq_passed` and `_dq_errors`,
- returns a `BatchValidationResult` ready to be queried or routed.

## Result views

`BatchValidationResult` exposes filtered views over the annotated data:

| View        | Returns                                                                              |
| ----------- | ------------------------------------------------------------------------------------ |
| `pass_df()` | Rows that passed every failure-level check (`CRITICAL` by default), original schema. |
| `fail_df()` | Rows that failed at least one failure-level check, plus error metadata.              |
| `warn_df()` | Passing rows that carry at least one `WARNING`-level violation.                      |
| `summary()` | Counts, pass rate, and timestamp for the run.                                        |

By default only `CRITICAL` checks cause a row to fail; `WARNING` checks are
recorded without failing it. You can change which severities count as failures
via the engine's `fail_levels` parameter.

## Reading the result

`pass_df()` returns only the clean order, restored to its original schema:

```python
result.pass_df().show(truncate=False)
```

```text
+------+-----------------+--------+
|amount|customer_email   |order_id|
+------+-----------------+--------+
|42.0  |alice@example.com|1       |
+------+-----------------+--------+
```

`fail_df()` returns the rows that failed a critical check, with `_dq_errors`
listing every violation. Note the `WARNING`-level `min-volume` check appears here
too, alongside the critical one that failed each row:

```python
result.fail_df().select("order_id", "_dq_errors", "_dq_passed").show(truncate=False)
```

```text
+--------+---------------------------------------------------------------------------------------+----------+
|order_id|_dq_errors                                                                             |_dq_passed|
+--------+---------------------------------------------------------------------------------------+----------+
|2       |[{NullCheck, email-required, critical}, {RowCountMinCheck, min-volume, warning}]       |false     |
|3       |[{NumericMinCheck, amount-positive, critical}, {RowCountMinCheck, min-volume, warning}]|false     |
+--------+---------------------------------------------------------------------------------------+----------+
```

`warn_df()` surfaces rows that _passed_ but still carry a warning — here the valid
order, flagged only by the batch-level `min-volume` check:

```python
result.warn_df().select("order_id", "_dq_errors").show(truncate=False)
```

```text
+--------+-----------------------------------------+
|order_id|_dq_errors                               |
+--------+-----------------------------------------+
|1       |[{RowCountMinCheck, min-volume, warning}]|
+--------+-----------------------------------------+
```

`summary()` gives run-level statistics:

```python
print(result.summary())
```

```text
Validation Summary (2026-01-01 00:00:00)
Total records:   3
Passed records:  1
Failed records:  2
Warnings:        1
Pass rate:       33.00%
```

## Metadata columns

The engine adds these columns to the DataFrame:

| Column              | Description                                                                                   |
| ------------------- | --------------------------------------------------------------------------------------------- |
| `_dq_passed`        | `true` if the row passed every failure-level check.                                           |
| `_dq_errors`        | Array of `{check, check-id, severity}` structs for each violation (row- and aggregate-level). |
| `_dq_validation_ts` | Timestamp of the run (added by `fail_df()` / `warn_df()`).                                    |

Next: [act on the result](applying_validation.md) in your pipeline.
