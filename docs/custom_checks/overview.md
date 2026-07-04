# Overview

SparkDQ ships with a broad set of built-in checks for common data quality scenarios. But real-world pipelines often require validations that are specific to your domain, business rules, or data contracts — things like "this field must follow a proprietary format" or "this column must satisfy a company-specific constraint" that no generic check can cover out of the box.

Custom checks let you implement exactly that logic and plug it directly into the SparkDQ engine. Once registered, they behave identically to built-in checks: they support declarative configuration via Python, YAML, or JSON, integrate with the validation engine, and produce the same structured result output.

Every check in SparkDQ — built-in or custom — consists of two components:

- A `Check` class — implements the validation logic against a Spark DataFrame
- A `CheckConfig` class — declares and validates parameters via Pydantic, and is linked to its check class via `check_class`

The config class is registered under a unique name using `@register_check_config`. The `CheckFactory` uses this registry to resolve the right config class by name, validate its parameters, and call `.to_check()` to produce the executable check instance — without any changes to the engine.

## Example

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core import BaseRowCheck, BaseRowCheckConfig, Severity
from sparkdq.plugin import register_check_config


class PositiveValueCheck(BaseRowCheck):

    def __init__(self, check_id: str, column: str, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column

    def validate(self, df: DataFrame) -> DataFrame:
        condition = F.col(self.column) <= 0
        return self.with_check_result_column(df, condition)


@register_check_config(check_name="positive-value")
class PositiveValueCheckConfig(BaseRowCheckConfig):
    column: str
    check_class = PositiveValueCheck
```

Once registered, the check is available declaratively by name. The factory resolves it, validates the config, and instantiates the check automatically:

```python
from sparkdq.management import CheckSet

check_set = CheckSet()
check_set.add_checks_from_dicts([
    {"check": "positive-value", "check-id": "positive-salary", "column": "salary"}
])
```

## Loading Custom Checks

The module containing your config classes must be imported before the factory can resolve them. Use `load_config_module` for this — typically once at application startup or before building a `CheckSet`:

```python
from sparkdq.plugin import load_config_module

load_config_module("myproject.custom_checks")
```

---

The next section walks through a complete implementation — a row-level and an aggregate check — from check class to declarative usage.
