# Defining Checks

SparkDQ supports two types of checks: **row-level** checks that validate each record individually, and **aggregate** checks that evaluate the dataset as a whole. Both can be defined directly in Python or loaded declaratively from YAML or JSON.

Every check accepts an optional `severity` parameter: `Severity.CRITICAL` (default) marks failing rows as invalid, `Severity.WARNING` records the violation but keeps the rows in `pass_df()`.

## Python-native Configuration

For dynamic or code-driven use cases (e.g. notebooks, CI pipelines), you can define checks directly in Python using type-safe config classes. The `CheckSet` supports both the classic and the fluent API style.

=== "Fluent API (recommended)"

    ```python
    from sparkdq.checks import NullCheckConfig, RowCountBetweenCheckConfig
    from sparkdq.core import Severity
    from sparkdq.management import CheckSet

    check_set = (
        CheckSet()
        .add_check(
            NullCheckConfig(
                check_id="my-null-check",
                columns=["email"]
            )
        )
        .add_check(
            RowCountBetweenCheckConfig(
                check_id="my-count-check",
                min_count=100,
                max_count=5000
            )
        )
    )
    ```

=== "Classic API"

    ```python
    from sparkdq.checks import NullCheckConfig, RowCountBetweenCheckConfig
    from sparkdq.core import Severity
    from sparkdq.management import CheckSet

    check_set = CheckSet()
    check_set.add_check(
        NullCheckConfig(
            check_id="my-null-check",
            columns=["email"]
        )
    )
    check_set.add_check(
        RowCountBetweenCheckConfig(
            check_id="my-count-check",
            min_count=100,
            max_count=5000
        )
    )
    ```

## Declarative Configuration

If you use a metadata-driven or config-as-code approach, SparkDQ also supports declarative check definitions via dictionaries — for example loaded from YAML or JSON files.

```yaml
# dq_checks.yaml
- check: null-check
  check-id: my-null-check
  columns:
    - email
  severity: warning

- check: row-count-between-check
  check-id: my-count-check
  min-count: 100
  max-count: 5000
```

To load the configuration into SparkDQ, use the following code:

!!! note
SparkDQ does not install `pyyaml` or any other config parser. You are responsible for loading your config into a Python dictionary — SparkDQ only takes it from there.

```python
import yaml
from sparkdq.management import CheckSet

with open("dq_checks.yaml") as f:
    config = yaml.safe_load(f)

check_set = CheckSet()
check_set.add_checks_from_dicts(config)
```
