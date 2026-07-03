# Registry + Factory + Decorator Plugin System

**Decision.** Check names — the strings used in declarative configs, such as
`"null-check"` — are mapped to config classes through a central
`CheckConfigRegistry`. The registry is populated by the
`@register_check_config` decorator and consumed by a `CheckFactory`. There is no
central `if/elif` dispatch and no hand-maintained list of imports.

## Context

Declarative configs refer to checks by string. Something has to turn
`"null-check"` into the concrete class that knows how to validate the
parameters and build the check. The naive approaches all age badly: a giant
`if/elif` block becomes a merge-conflict magnet and couples every check to a
central switch, while a hand-maintained import list silently drifts out of date
whenever someone adds a check and forgets to register it.

The framework also has an explicit goal of being extensible by _third parties_ —
users should be able to ship their own checks without forking or patching the
core.

## Benefits

- **Extensible without touching the core.** A new check is added purely by
  defining its class and decorating its config with
  `@register_check_config("my-check")`. No core file is edited, no central
  registry is hand-updated — a direct application of the open/closed principle.
  Third-party checks are first-class citizens, indistinguishable from built-ins
  at resolution time.
- **Name and implementation are decoupled.** The public contract is the string
  name in YAML. The implementing class can be renamed, moved, or refactored
  freely without breaking a single existing config — the registry is the only
  thing that has to agree on the name.
- **Registration lives with the definition.** The decorator sits on the config
  class itself, so the fact _that_ a check is registered and _under what name_ is
  visible right where the check is defined, not in a distant lookup table that
  has to be kept in sync.
- **Clashes fail loudly.** Registering a name twice raises `ValueError` at import
  time. An accidental duplicate is caught immediately instead of silently
  shadowing an existing check and changing behavior at runtime.
- **A single, testable resolution path.** All name resolution flows through
  `CheckConfigRegistry.get()` and `CheckFactory`, giving one place to reason
  about — and test — how a string becomes a running check.

## Trade-offs

Registration is a side effect of importing the module that defines the check.
Built-ins are handled transparently — `CheckFactory.from_list()` calls
`load_config_module("sparkdq.checks")` first — but a _custom_ check's module must
be imported (directly, via `load_config_module`, or implicitly by adding the
config to a `CheckSet`) before its name will resolve. This import-to-register
coupling is the standard cost of decorator-based plugin systems, and it is
predictable once understood.

## Where to look

- `plugin/check_config_registry.py` — the registry and the decorator.
- `plugin/check_factory.py` — dict-to-check resolution.
- [Custom Checks](../../custom_checks/overview.md) — the author-facing walkthrough.
