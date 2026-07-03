# Row-Level vs. Aggregate-Level Separation

**Decision.** Row checks and aggregate checks are kept strictly apart: separate
base classes (`BaseRowCheck`, `BaseAggregateCheck`), separate result models, and
separate execution paths inside `BatchCheckRunner`.

## Context

There is a tempting simplification available: model every check as "a thing that
looks at data and returns a verdict", and unify them behind one interface. But
the two kinds of verdict are not the same shape at all. "Is _this row's_ email
null?" produces one answer per row. "Does _the dataset_ have between 1,000 and
10,000 rows?" produces exactly one answer for the whole dataset. Forcing both
through a single abstraction means every consumer downstream has to keep asking
"but which kind is this really?" — and the abstraction leaks anyway.

The two kinds also _execute_ differently. A per-row verdict is naturally a column
you append; a dataset-wide verdict is naturally a Spark aggregation. Those are
different mechanisms with different performance characteristics.

## Benefits

- **The model matches the domain.** Two genuinely different concepts are
  represented as two types. A `BaseRowCheck` returns an annotated DataFrame; a
  `BaseAggregateCheck` returns an `AggregateCheckResult`. Neither has to pretend
  to be the other, and neither carries fields that only make sense for the other.
- **Each path can be optimized independently.** Row checks compose as lazy column
  transformations and never trigger an action on their own. Aggregate checks run
  as Spark aggregations — and because they are a distinct group, the runner can
  apply the [single-pass batching](observable_aggregation.md) optimization to
  them without that logic bleeding into row-check handling.
- **Outputs stay clean and purpose-fit.** Row results flow into per-row
  metadata columns and the `pass_df()`/`fail_df()`/`warn_df()` views; aggregate
  results flow into a separate ordered list for reporting. Consumers pick exactly
  the shape they need instead of unpacking a union type.
- **Clearer extension surface.** An author writing a custom check chooses a base
  class up front, and the base makes the required method obvious (`validate()`
  vs. `_evaluate_logic()`). The type system guides the implementation rather than
  leaving it to convention.

## Trade-offs

The runner has to branch on check type and maintain two code paths, and the
framework carries two base classes instead of one. This is accepted precisely
because the alternative — a single abstraction — pushes that same branching onto
_every_ consumer of a check result, where it would be repeated and error-prone.
Concentrating the split in one place (the runner and the base classes) is the
cheaper of the two.

## Where to look

- `core/base_check.py` — the two base classes.
- `engine/batch/check_runner.py` — the two-phase execution.
- [Execution Flow](../execution_flow.md).
