# Defining Checks

A check is defined through a typed **config class**. SparkDQ supports two kinds:
**row-level** checks that validate each record individually, and **aggregate**
checks that evaluate the dataset as a whole. Both can be written directly in
Python or loaded declaratively from YAML or JSON.

Every check accepts an optional `severity`: `Severity.CRITICAL` (default) marks
failing rows invalid, while `Severity.WARNING` records the violation but keeps
the rows in `pass_df()`.

We will define the three rules from the [introduction](introduction.md) scenario:
`customer_email` must not be null, `amount` must be positive, and the batch must
contain at least 100 rows.

## Python-native

For code-driven use (notebooks, CI pipelines), define checks with type-safe
config classes. `CheckSet` supports both a fluent and a classic style.

=== "Fluent API (recommended)"

    ```python
    from sparkdq.checks import NullCheckConfig, NumericMinCheckConfig, RowCountMinCheckConfig
    from sparkdq.core import Severity
    from sparkdq.management import CheckSet

    check_set = (
        CheckSet()
        .add_check(
            NullCheckConfig(
                check_id="email-required",
                columns=["customer_email"],
            )
        )
        .add_check(
            NumericMinCheckConfig(
                check_id="amount-positive",
                columns=["amount"],
                min_value=0,
            )
        )
        .add_check(
            RowCountMinCheckConfig(
                check_id="min-volume",
                min_count=100,
                severity=Severity.WARNING,
            )
        )
    )
    ```

=== "Classic API"

    ```python
    from sparkdq.checks import NullCheckConfig, NumericMinCheckConfig, RowCountMinCheckConfig
    from sparkdq.core import Severity
    from sparkdq.management import CheckSet

    check_set = CheckSet()
    check_set.add_check(
        NullCheckConfig(
            check_id="email-required",
            columns=["customer_email"],
        )
    )
    check_set.add_check(
        NumericMinCheckConfig(
            check_id="amount-positive",
            columns=["amount"],
            min_value=0,
        )
    )
    check_set.add_check(
        RowCountMinCheckConfig(
            check_id="min-volume",
            min_count=100,
            severity=Severity.WARNING,
        )
    )
    ```

!!! tip "Choosing severity and check IDs"
    Use `CRITICAL` for rules that must hold for a record to be usable, and
    `WARNING` for signals you want to monitor without failing the batch. Give
    each check a stable, descriptive `check_id` — it appears verbatim in
    `_dq_errors`, so `email-required` reads better in a report than `check_1`.

## Declarative (YAML / JSON)

For a metadata-driven or config-as-code approach, the same checks can be defined
as dictionaries — for example loaded from a YAML file:

```yaml
# checks.yml
- check: null-check
  check-id: email-required
  columns:
    - customer_email

- check: numeric-min-check
  check-id: amount-positive
  columns:
    - amount
  min-value: 0

- check: row-count-min-check
  check-id: min-volume
  min-count: 100
  severity: warning
```

!!! note
    SparkDQ does not bundle `pyyaml` or any config parser. You load the config
    into a Python list of dicts; SparkDQ takes it from there.

```python
import yaml
from sparkdq.management import CheckSet

with open("checks.yml") as f:
    config = yaml.safe_load(f)

check_set = CheckSet()
check_set.add_checks_from_dicts(config)
```

Next: [validate a DataFrame](validation_dataframes.md) with this `CheckSet`.
