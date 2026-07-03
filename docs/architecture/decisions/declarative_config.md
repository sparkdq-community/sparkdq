# Declarative Configuration Layer

**Decision.** Every check is _defined_ through a Pydantic **Config class** rather
than by instantiating check objects directly. A config can be constructed from
typed Python or built from a plain dictionary loaded from YAML or JSON. The
config validates its parameters and, via `to_check()`, produces the runnable
check instance.

## Context

A data quality rule ("this column must not be null", "the row count must sit
between 1,000 and 10,000") is a piece of _configuration_, not a piece of program
logic. The people who know those rules — data owners, analysts, domain experts —
are not always the people who maintain the pipeline code. And even when they are,
a rule that lives as a hand-instantiated Python object is hard to review,
version, or move between environments.

Separating the _definition_ of a check from its _execution_ addresses this. The
config class is the definition; the check instance is the execution.

## Benefits

- **Rules can live outside the code.** Because a config is just data, checks can
  be authored and maintained in YAML or JSON — in a separate repository, a config
  store, or a database — and loaded at runtime. The pipeline no longer has to
  change when a rule changes.
- **Errors surface early and cheaply.** Pydantic validates every parameter the
  moment the config is built, _before_ a Spark job is ever submitted. A typo in a
  threshold or a missing required field fails immediately with a precise message,
  instead of after minutes of cluster time.
- **Definitions are reviewable and portable.** Plain-data definitions can be
  diffed in a pull request, versioned in Git, and promoted from staging to
  production unchanged. The rule set becomes an auditable artifact in its own
  right.
- **Type safety without giving up flexibility.** Typed config classes give IDE
  autocompletion and `mypy` coverage for Python authors, while
  `populate_by_name` lets the _same_ class accept kebab-case keys
  (`check-id`) from YAML. One definition serves both audiences.
- **Misconfiguration is caught at import.** The `check_class` ↔ base-class
  binding is enforced in `__init_subclass__`, so a config wired to the wrong kind
  of check raises `TypeError` when its module loads — not mid-run.

## Trade-offs

Each check needs a paired config class, which is a small, repetitive amount of
boilerplate. This is a deliberate exchange: a few extra lines per check buys
validation, portability, tooling, and a single declarative surface. In practice
the config class is also the natural home for the check's documented parameter
schema, so the boilerplate does double duty.

## Where to look

- `core/base_config.py` — `BaseCheckConfig`, `BaseRowCheckConfig`,
  `BaseAggregateCheckConfig`.
- [Core Abstractions → configuration layer](../core_abstractions.md#from-definition-to-instance-the-configuration-layer).
