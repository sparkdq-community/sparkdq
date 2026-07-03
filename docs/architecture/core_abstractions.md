# Core Abstractions

The core abstractions define the contracts every check implementation must
honour. They live in `sparkdq/core/` and are deliberately minimal: a check is an
object with an identity, a severity, and one of two evaluation shapes — per-row
or per-dataset. Everything else in the framework is built on top of these few
types.

## The shared root: `BaseCheck`

Every check descends from `BaseCheck` (`core/base_check.py`), which contributes
the two things all checks have in common: a `check_id` and a `severity`
(`Severity.CRITICAL` by default). On top of that it provides the reporting
machinery — `name` returns the implementing class name (used as the `check`
field in every result), and `description()` produces the serializable metadata
record `{check, check-id, severity, parameters}`.

The `parameters` in that record are not hand-maintained. `_parameters()`
inspects the subclass constructor signature and reads the matching instance
attributes, which has one practical consequence for implementers: **every
constructor argument that should appear in reports must be stored as an instance
attribute of the same name**, since `check_id`, `severity`, and `columns` are
excluded by design.

The other rule to remember is that `check_id` must be **unique within a
`CheckSet`**. Row checks use it verbatim as a DataFrame column name (see below),
so a duplicate silently overwrites an earlier check's result column. `BaseCheck`
itself is never subclassed directly — checks extend one of the two specializations
that follow.

## Two evaluation shapes

The split between row-level and dataset-level checks is the central distinction
in the framework, and it runs all the way through to the [execution
flow](execution_flow.md) and [output model](output_model.md).

### `BaseRowCheck` — one verdict per row

A row check implements `validate(df: DataFrame) -> DataFrame`. It must return the
input unchanged except for a single appended boolean column named after its
`check_id`; the helper `with_check_result_column(df, condition)` enforces that
convention. The column's polarity is inverted on purpose — `True` marks a
**failed** row, `False` a passing one — so the engine can treat any `True` as a
violation without knowing anything check-specific.

Because row checks are composed into one query plan, `validate()` must stay a
pure, lazy transformation: it adds a column and nothing more. Triggering a Spark
action (`count`, `collect`, `first`, …) inside a row check would force a scan per
check and defeat single-pass execution — that work belongs to the engine.

### `BaseAggregateCheck` — one verdict per dataset

An aggregate check evaluates a global property (row counts, uniqueness ratios,
schema conformance, …) and yields exactly one outcome per run, regardless of row
count. It implements `_evaluate_logic(df) -> AggregateEvaluationResult`; callers
invoke `evaluate(df)`, which wraps that raw outcome in a full
`AggregateCheckResult` (adding `check`, `check_id`, `severity`, and `parameters`).

One detail is worth calling out because it looks like an oversight but isn't: on
`BaseAggregateCheck`, `_evaluate_logic()` _raises_ `NotImplementedError` instead
of being marked `@abstractmethod`. That is what lets the observable subclass
below provide a working default, while a classic aggregate check that forgets to
override it still fails loudly.

## `ObservableAggregateCheck` — aggregates that batch together

Most aggregate checks can express their logic as Spark aggregation _expressions_
rather than as imperative code. Those extend `ObservableAggregateCheck`
(`core/observable_check.py`), and doing so is what enables single-pass
aggregation — the reasoning is in
[Design Decisions → Observable aggregation](decisions/observable_aggregation.md).

Such a check implements two methods:

- `aggregations() -> dict[str, Column]` — the metric expressions it needs.
- `_evaluate_from_agg_results(results: dict[str, Any])` — the verdict, computed
  from the already-resolved metric values.

Metric keys only need to be unique _within a single check_; the runner
namespaces them per instance, so two checks may both use `"count"` without
colliding. The class also ships a working `_evaluate_logic()` that runs the
aggregations in its own `df.agg()` — the engine bypasses this and feeds each
check batched results directly, but the standalone path keeps a check runnable in
isolation, which is convenient in unit tests. Checks that genuinely cannot be
reduced to a single `agg()` (needing `groupBy`, `join`, or `distinct`) simply
stay on plain `BaseAggregateCheck` and are executed individually.

## `IntegrityCheckMixin` — validating across datasets

Some checks — foreign keys, referential integrity, cross-dataset consistency —
need a _second_ dataset to compare against. `IntegrityCheckMixin` supplies that
capability as an opt-in: the engine calls `inject_reference_datasets(datasets)`
during preparation, and the check later resolves a dataset by name with
`get_reference_df(name)`, which raises `MissingReferenceDatasetError` if the name
was never supplied. Because injection happens per run, a check must resolve
references inside `validate()` / `_evaluate_logic()` rather than in its
constructor, where the datasets are not yet available.

## `Severity` — how failures are handled

Severity is a two-valued enum (`core/severity.py`). Which severities actually
fail a row is governed by the engine's `fail_levels` (default `[CRITICAL]`): a
failure whose severity is in `fail_levels` marks rows invalid
(`_dq_passed = False`) and, for aggregates, can fail the whole batch, while any
other failure is recorded in `_dq_errors` but leaves `_dq_passed` untouched. With
the default configuration this means `CRITICAL` fails and `WARNING` only warns.
When configs arrive from YAML or JSON, `normalize_severity()` accepts either an
enum or a case-insensitive string and raises `InvalidSeverityLevelError` on
anything else.

## From definition to instance: the configuration layer

A check is _defined_ through a paired **Config class** — a Pydantic model
inheriting from `BaseRowCheckConfig` or `BaseAggregateCheckConfig`
(`core/base_config.py`). It declares a `check_class` ClassVar pointing at the
implementation plus one typed field per constructor parameter; `to_check()` then
builds a validated instance from those fields. Thanks to `populate_by_name`, the
same class serves both Python callers (`check_id`) and YAML callers
(`check-id`).

The payoff is that misconfiguration is caught early. The `check_class` ↔
base-class binding is verified at **class-definition time** via
`__init_subclass__`: a row config pointing at an aggregate check — or one missing
`check_class` entirely — raises `TypeError` the moment the module is imported,
long before any data is touched.

## From name to class: the plugin layer

Declarative configs refer to checks by string (`"null-check"`), so something has
to map those names to classes. That is the `CheckConfigRegistry`
(`plugin/check_config_registry.py`), a process-wide `name → config class` table.
The `@register_check_config(check_name="…")` decorator populates it as an import
side effect; registering a name twice raises `ValueError`, which turns accidental
clashes into loud import-time errors rather than silent shadowing.

The `CheckFactory` (`plugin/check_factory.py`) is the consumer. Given a raw dict
it requires a `check` key (otherwise `MissingCheckTypeError`), then normalizes any
`severity` _without mutating the caller's dict_ — an invalid severity string
fails here with `InvalidSeverityLevelError`, before Pydantic runs. It then looks
up the config class and validates the remaining parameters through Pydantic,
which raises a `ValidationError` on bad input, and returns the built check. Its
`from_list()` entry point first calls `load_config_module("sparkdq.checks")` so
every built-in is registered before resolution begins.

## Bringing it together: `CheckSet`

`CheckSet` (`management/check_set.py`) is the user-facing container and the
single source of truth for one validation run. Checks go in either as typed
config objects via the chainable `add_check()`, or as raw dicts via
`add_checks_from_dicts()` (which routes through the factory above). Internally it
keeps the row and aggregate checks separable, exposing `get_row_checks()` and
`get_aggregate_checks()` — the very split that drives the two-phase
[execution flow](execution_flow.md).
