# Design Decisions

This section records the _why_ behind SparkDQ's architecture — the reasoning,
the benefits, and the trade-offs. The _what_ and _how_ are covered in the rest of
the [Architecture](overview.md) section; these pages exist so that contributors
and users can understand the intent behind the structure and avoid re-litigating
settled choices.

Each decision has its own page, following the same shape: the **decision**, the
**context** that motivated it, the **benefits** it delivers, and the
**trade-offs** it accepts.

## Guiding principles

A few principles run through all of the individual decisions below:

- **Rule-based, not statistical.** SparkDQ validates data against _explicit_
  rules. Automatic data profiling and ML-based anomaly detection are a deliberate
  **non-goal** — the framework does exactly what you tell it to check, and nothing
  implicit. Behavior is predictable and every result is auditable.
- **Minimal dependencies.** The core depends only on Pydantic. PySpark is
  expected to come from the runtime platform (e.g. Databricks) rather than being
  bundled, keeping the footprint small and avoiding version conflicts with the
  host environment.
- **Batch first, streaming later.** The current engine targets batch validation.
  The engine layer is abstracted behind `BaseDQEngine` specifically so a
  streaming engine can be added later without reworking the check catalogue or
  the configuration system.

## The decisions

| Decision                                                                   | In one line                                                                                  |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [Declarative configuration layer](decisions/declarative_config.md)         | Checks are defined as validated Pydantic data, authorable from YAML/JSON.                    |
| [Registry + Factory + Decorator plugin system](decisions/plugin_system.md) | Check names resolve to classes through a registry, so the core never changes to add a check. |
| [Row-level vs. aggregate-level separation](decisions/row_vs_aggregate.md)  | Two genuinely different verdict shapes get two base classes and two execution paths.         |
| [Observable single-pass aggregation](decisions/observable_aggregation.md)  | All observable aggregate checks run in one `df.agg()` to minimize scans and cost.            |
| [Metadata columns on the DataFrame](decisions/dataframe_annotations.md)    | Verdicts are attached to the rows themselves, not returned as a detached report.             |

## At a glance

| Decision                           | Primary driver                                                              |
| ---------------------------------- | --------------------------------------------------------------------------- |
| Declarative Pydantic configs       | External YAML/JSON authoring, early validation, portability, typing         |
| Registry + Factory + Decorator     | Extensibility without core changes; name resolution for declarative configs |
| Row vs. aggregate separation       | Different semantics and different execution strategies                      |
| Observable single-pass aggregation | Minimize Spark actions / cost at scale (N scans collapse to one)            |
| `_dq_*` columns on the DataFrame   | Row-level traceability and direct downstream routing                        |
| Rule-based only                    | Predictable, auditable behavior (non-goal: profiling/ML anomaly detection)  |
| Minimal deps / batch-first         | Small footprint; room to add streaming behind `BaseDQEngine`                |
