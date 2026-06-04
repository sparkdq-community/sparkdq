# Implementation

Place your checks in a dedicated package and expose all config classes from `__init__.py` so registration decorators run on import:

```
myproject/
└── custom_checks/
    ├── __init__.py
    ├── row_checks.py
    └── aggregate_checks.py
```

```python
# __init__.py
from .row_checks import MyRowCheckConfig
from .aggregate_checks import MyAggregateCheckConfig
```

Once the package is structured this way, register all checks with a single call before building a `CheckSet`:

```python
from sparkdq.plugin import load_config_module

load_config_module("myproject.custom_checks")
```

## Row-Level Check

Rows where the condition evaluates to `True` are marked as **failing**.

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core import BaseRowCheck, Severity
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

=== "Python"

    ```python
    check_set = CheckSet().add_checks_from_dicts([
        {"check": "positive-value", "check-id": "positive-salary", "column": "salary"}
    ])
    ```

=== "YAML"

    ```yaml
    - check: positive-value
      check-id: positive-salary
      column: salary
    ```

## Aggregate Check

Aggregate checks evaluate the dataset as a whole. Implement `_evaluate_logic(df)` and return an `AggregateEvaluationResult` with a `passed` flag and a `metrics` dictionary.

```python
from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from sparkdq.core import AggregateEvaluationResult, BaseAggregateCheck, BaseAggregateCheckConfig, Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin import register_check_config


class RowCountMinCheck(BaseAggregateCheck):

    def __init__(self, check_id: str, min_count: int, severity: Severity = Severity.CRITICAL):
        super().__init__(check_id=check_id, severity=severity)
        self.min_count = min_count

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        actual = df.count()
        return AggregateEvaluationResult(
            passed=actual >= self.min_count,
            metrics={"actual_row_count": actual, "min_expected": self.min_count},
        )


@register_check_config(check_name="custom-row-count-min")
class RowCountMinCheckConfig(BaseAggregateCheckConfig):
    check_class = RowCountMinCheck
    min_count: int = Field(..., alias="min-count")

    @model_validator(mode="after")
    def validate_min(self) -> "RowCountMinCheckConfig":
        if self.min_count <= 0:
            raise InvalidCheckConfigurationError(
                f"min_count ({self.min_count}) must be greater than 0"
            )
        return self
```

=== "Python"

    ```python
    check_set = CheckSet().add_checks_from_dicts([
        {"check": "custom-row-count-min", "check-id": "min-records", "min-count": 1000}
    ])
    ```

=== "YAML"

    ```yaml
    - check: custom-row-count-min
      check-id: min-records
      min-count: 1000
    ```

---

Your custom checks are now fully integrated into SparkDQ — validated, declarative, and indistinguishable from built-in checks at runtime.
