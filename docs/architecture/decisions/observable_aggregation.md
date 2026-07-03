# Observable Single-Pass Aggregation

**Decision.** Aggregate checks that can express their logic as Spark aggregation
expressions extend `ObservableAggregateCheck` and declare their needs via
`aggregations()`. The `BatchCheckRunner` collects the expressions from _all_ such
checks and executes them in a **single** `df.agg()` call, then dispatches the
resolved values back to each check's `_evaluate_from_agg_results()`.

## Context

The obvious way to run aggregate checks is one at a time: ask each check to
evaluate itself against the DataFrame. It works, and it is easy to reason about —
but every self-contained evaluation is its own Spark action, and every action
triggers a scan (and sometimes a shuffle) of the data.

On a small test dataset the difference is invisible. On a production-sized table
it is not: with the naive approach, N aggregate checks trigger N independent full
scans of the same data. The observable pattern collapses those into one, which is
the motivation for the design.

## Benefits

- **Many checks, one pass over the data.** All observable checks contribute their
  metric expressions to a single `df.agg()`. Ten row-count-and-completeness style
  checks collapse from ten scans into one. The saving grows linearly with the
  number of aggregate checks.
- **Lower cost on managed compute.** Fewer full-table scans mean less cluster
  time, which on managed platforms such as Databricks maps directly to cost. The
  optimization pays off exactly where it matters most — large datasets in
  production.
- **The optimization is transparent to authors.** A check author only declares
  _what_ to measure (`aggregations()`) and _how to judge it_
  (`_evaluate_from_agg_results()`). The batching is entirely the runner's
  concern; the author never writes a `df.agg()` call and never thinks about
  Spark actions.
- **Safe composition.** Metric keys only need to be unique within a single check;
  the runner namespaces them per instance, so unrelated checks can freely reuse
  common names like `"count"` without colliding.
- **Still runnable in isolation.** The base class ships a working
  `_evaluate_logic()` that runs the aggregations in its own `df.agg()`. The engine
  bypasses it for batching, but the standalone path keeps a single check testable
  on its own — the optimization does not sacrifice unit-test ergonomics.

## Trade-offs

Not every aggregate check _can_ be reduced to a single `agg()`. Anything needing
`groupBy`, `join`, or `distinct` cannot participate, and those checks stay on
plain `BaseAggregateCheck` and are evaluated individually (the "classic" path).
The framework accepts this two-track model rather than forcing an awkward fit:
observable is the fast common case, classic is the correct fallback, and result
ordering is preserved regardless of which path a check takes.

## Where to look

- `core/observable_check.py` — the base class and its contract.
- `engine/batch/check_runner.py` — `_run_observable_checks()`, the batched
  `df.agg()`.
- [Core Abstractions → ObservableAggregateCheck](../core_abstractions.md#observableaggregatecheck-aggregates-that-batch-together).
