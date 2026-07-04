# Acting on Results

Once `run_batch` returns a `result`, you decide what happens next. The right
pattern depends on how much trust downstream systems require and how you want to
handle bad data. Two common approaches — fail-fast and quarantine — each take
only a few lines, and both build on the `result` from the
[previous step](validation_dataframes.md).

## Fail-fast

Stop the pipeline as soon as data quality drops, before anything is written
downstream. `summary().all_passed` is `True` only when every row passed:

```python
if not result.summary().all_passed:
    raise RuntimeError("Data quality checks failed — stopping pipeline.")
```

Use this when downstream consumers require complete trust in the data, or in
regulated domains where partial data is worse than no data. In the `orders`
scenario this raises, because two of three rows failed a critical check.

## Quarantine

Route valid and invalid records to separate destinations, so clean data flows on
while bad records are preserved for inspection. Failing rows keep their
`_dq_errors` metadata, giving you the full reason for each rejection:

```python
result.pass_df().write.format("delta").save("/trusted/orders")
result.fail_df().write.format("delta").save("/quarantine/orders")
```

Use this when clean data should flow forward uninterrupted while invalid records
are set aside for remediation, alerting, or backfill.

!!! tip "Cache before deriving multiple views"
    Each of `pass_df()`, `fail_df()`, `warn_df()`, and `summary()` triggers a
    Spark action over the validated data. If you use more than one, cache the
    result once to avoid recomputing the plan:

    ```python
    result.df.cache()
    ```

## Warnings alongside either pattern

Warnings never fail a row, so they compose with both patterns. Log them for
monitoring while the pipeline continues:

```python
warn_count = result.warn_df().count()
if warn_count:
    logger.warning("%d record(s) passed with warnings", warn_count)
```

That completes the core workflow: **define** checks, **collect** them in a
`CheckSet`, **run** the engine, and **act** on the result. From here, browse the
[built-in checks](../built_in_checks/row_level.md) or learn to write your own in
[Custom Checks](../custom_checks/overview.md).
