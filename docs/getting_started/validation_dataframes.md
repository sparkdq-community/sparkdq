# Validating DataFrames

Once your checks are defined and grouped into a `CheckSet`, pass them to the `BatchDQEngine` together with your Spark DataFrame. The engine evaluates all rules in a single pass and returns a structured `BatchValidationResult`.

```python
from sparkdq.checks import NullCheckConfig
from sparkdq.engine import BatchDQEngine
from sparkdq.management import CheckSet

check_set = CheckSet()
check_set.add_check(NullCheckConfig(check_id="my-null-check", columns=["email"]))

result = BatchDQEngine(check_set).run_batch(df)

result.pass_df().show()
result.fail_df().select("_dq_errors").show(truncate=False)
print(result.summary())
```

## What Happens Under the Hood

When `run_batch()` is called, the engine:

- evaluates all row-level checks and marks failing rows individually
- evaluates all aggregate checks against the full DataFrame
- annotates every row with `_dq_passed`, `_dq_errors`, and `_dq_validation_ts`
- returns a result object ready to be queried or routed

## Working with the Result

| Method      | Description                                                           |
| ----------- | --------------------------------------------------------------------- |
| `pass_df()` | Records that passed every failure-level check (`CRITICAL` by default) |
| `fail_df()` | Records that failed at least one failure-level check                  |
| `warn_df()` | Passing records that triggered one or more warning-level violations   |
| `summary()` | Structured summary with record counts, pass rate, and timestamp       |

By default only `CRITICAL` checks cause a row to fail; `WARNING` checks are
recorded without failing the row. You can change which severities count as
failures via the engine's `fail_levels` parameter.

## Result Columns

SparkDQ automatically adds the following columns to your DataFrame:

| Column              | Description                                                            |
| ------------------- | ---------------------------------------------------------------------- |
| `_dq_passed`        | Boolean flag — `true` if the row passed every failure-level check      |
| `_dq_errors`        | Array of structured errors per failed check (check-id, name, severity) |
| `_dq_validation_ts` | Timestamp of the validation run                                        |
