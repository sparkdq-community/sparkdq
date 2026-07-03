# Output Model

SparkDQ returns results _on the data itself_. A validation run annotates the
input DataFrame with metadata columns, and `BatchValidationResult`
(`engine/batch/validation_result.py`) exposes filtered views derived from them.
This keeps every verdict attached to the row it describes — no join back to the
source is ever required to answer "which rows failed, and why".

## Metadata columns

| Column       | Type                                       | Meaning                                                                                                                                                                        |
| ------------ | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `_dq_passed` | `boolean`                                  | Row-level verdict. `False` when a check whose severity is in `fail_levels` (`CRITICAL` by default) fired, or when such an aggregate failed the batch.                          |
| `_dq_errors` | `array<struct<check, check-id, severity>>` | Every check that fired for the row; empty array when clean. Failed **aggregate** checks are appended to this same array, so it carries both row- and dataset-level violations. |

## Result views

`BatchValidationResult` is a frozen dataclass carrying the annotated `df`, the
ordered `aggregate_results`, the original `input_columns`, and a run
`timestamp`. Its views:

| View        | Rows returned                                                  | Schema                                                               |
| ----------- | -------------------------------------------------------------- | -------------------------------------------------------------------- |
| `pass_df()` | `_dq_passed = True`                                            | **Original input schema** — metadata columns are dropped.            |
| `fail_df()` | `_dq_passed = False`                                           | Input columns + `_dq_errors`, `_dq_passed`, and `_dq_validation_ts`. |
| `warn_df()` | `_dq_passed = True` **and** ≥1 `WARNING` entry in `_dq_errors` | Input columns + `_dq_errors` + `_dq_validation_ts`.                  |
| `summary()` | —                                                              | A `ValidationSummary` (counts + `pass_rate` + `timestamp`).          |

## Notes and guarantees

- **`pass_df()` round-trips cleanly.** It selects exactly `input_columns`, so the
  passing subset is schema-compatible with the source and can be written straight
  back to a table without dropping columns.
- **A single validation timestamp.** `_dq_validation_ts` is taken from the
  result's `timestamp` (fixed at result construction), not evaluated per row, so
  all rows in one run share an identical, auditable timestamp.
- **Warnings coexist with passing.** Because warnings never clear `_dq_passed`, a
  row can appear in both `pass_df()` and `warn_df()`. This is intentional: route
  it to production _and_ log the warning.
- **Views are actions.** `summary()` performs multiple `count()` calls, and each
  `*_df()` view re-scans the plan. If several views are needed, cache the
  annotated DataFrame (`result.df.cache()`) before deriving them.
- **`pass_rate` is rounded.** `summary()` reports `pass_rate` rounded to two
  decimal places, and returns `0.0` for an empty input rather than dividing by
  zero.
