# Metadata Columns on the DataFrame

**Decision.** Validation results are written back onto the DataFrame itself as
`_dq_*` metadata columns (`_dq_passed` and `_dq_errors`), rather than returned as
a separate, detached report object. Failed aggregate checks are folded into the
same `_dq_errors` array, so a single column carries every violation.

## Context

Once checks have run, a caller almost always wants to _act_ on the outcome:
route the clean rows onward, quarantine the bad ones, log the warnings. If the
verdict lived in a separate report keyed by some row identifier, the caller would
have to join that report back to the data to act on it — and that assumes a
stable identifier exists, which for arbitrary input DataFrames it may not.

Keeping the verdict physically attached to the row it describes removes that
whole class of problem.

## Benefits

- **Every row carries its own verdict.** `_dq_passed` says whether the row passed;
  `_dq_errors` lists exactly which checks fired and at what severity. The "which
  rows failed, and why" question is answered by looking at the row — never by
  reconstructing a correlation.
- **Result views are trivial filters.** `pass_df()`, `fail_df()`, and `warn_df()`
  are just predicates over the annotated DataFrame. There is no bookkeeping to
  keep a side report in sync with the data, because there is no side report.
- **Immediate, natural downstream use.** The annotated DataFrame drops straight
  into the rest of a Spark pipeline. Writing failures to a quarantine table is a
  `fail_df().write` away, metadata included — no extra join, no extra stage.
- **Clean round-trip for the happy path.** `pass_df()` selects exactly the
  original input columns, so the passing subset is schema-compatible with the
  source and can be written back without stripping the `_dq_*` columns by hand.
- **Auditable by construction.** `fail_df()` and `warn_df()` carry a single
  `_dq_validation_ts` fixed at result-construction time, so every row from one
  run shares an identical, traceable timestamp.

## Trade-offs

The output DataFrame is wider than the input while the `_dq_*` columns are
present, and the result views are Spark _actions_ — `summary()` performs several
`count()`s, and each `*_df()` re-scans the plan. The mitigation is conventional
Spark hygiene: cache the annotated DataFrame (`result.df.cache()`) when several
views will be derived from it. This is a familiar cost with a familiar remedy,
and it is far cheaper than the join-back a detached report would require on every
access.

## Where to look

- `engine/batch/validation_result.py` — `BatchValidationResult` and its views.
- [Output Model](../output_model.md).
