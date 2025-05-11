Aggregate Checks
================

Aggregate checks evaluate the dataset as a whole, rather than row by row.  
They are ideal for enforcing global conditions — such as row counts, uniqueness, value distributions, or statistical properties.

To implement a custom aggregate check, you need two components:

* A ``Check`` class that inherits from `BaseAggregateCheck`  
* A corresponding ``CheckConfig`` that inherits from `BaseAggregateCheckConfig`

Implementing the Check class
----------------------------

Your custom check must inherit from **BaseAggregateCheck**, which defines the structure and evaluation
lifecycle for aggregate-level checks. The core method to implement is ``_evaluate_logic(df: DataFrame)``,
which should return an **AggregateEvaluationResult** containing both the outcome of the check (pass or fail)
and a dictionary of metrics such as actual values, thresholds, or other summary statistics.


Constructor
^^^^^^^^^^^

Every check must also be initialized with two required parameters:

- ``check_id``: a unique identifier for the check instance
- ``severity``: the severity level (default: ``Severity.CRITICAL``)

Both parameters must be passed to the **super().__init__** call inside your check’s constructor.  
This ensures that the check is properly registered in the validation result and handled correctly by the engine.


Minimal Example
^^^^^^^^^^^^^^^

.. code-block:: python

   from pyspark.sql import DataFrame
   from sparkdq.core import AggregateEvaluationResult, BaseAggregateCheck, Severity

   class RowCountMinCheck(BaseAggregateCheck):

       def __init__(self, check_id: str, min_count: int, severity: Severity = Severity.CRITICAL):
           super().__init__(check_id=check_id, severity=severity)
           self.min_count = min_count

       def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
           actual = df.count()
           passed = actual >= self.min_count
           return AggregateEvaluationResult(
               passed=passed,
               metrics={
                   "actual_row_count": actual,
                   "min_expected": self.min_count,
               },
           )

Defining the Configuration Class
--------------------------------

The configuration class inherits from **BaseAggregateCheckConfig**, a Pydantic model used to define and
validate parameters. You must set the check_class to link it to the check implementation, and use typed
fields to declare required inputs. Optional validation logic can be added via @model_validator, and the
class must be registered using ``@register_check_config(...)`` with a unique check name.

Minimal Example
^^^^^^^^^^^^^^^

.. code-block:: python

   from sparkdq.core import BaseAggregateCheckConfig
   from sparkdq.exceptions import InvalidCheckConfigurationError
   from sparkdq.plugin import register_check_config

   @register_check_config(check_name="my-custom-count-check")
   class RowCountMinCheckConfig(BaseAggregateCheckConfig):
       check_class = RowCountMinCheck
       min_count: int = Field(..., description="Minimum number of rows expected", alias="min-count")

       @model_validator(mode="after")
       def validate_min(self) -> "RowCountMinCheckConfig":
           if self.min_count <= 0:
               raise InvalidCheckConfigurationError(f"min_count ({self.min_count}) must be greater than 0")
           return self

This configuration will be automatically validated when passed to the factory, and any logical issues
(like a non-positive threshold) will raise an error early.


.. raw:: html

   <hr>

🚀 **Summary**:

By now, you’ve seen how the framework’s plugin architecture works — and how configuration, logic, registry, and factory all come together to support fully declarative, extensible data validation.

You’ve also learned how to implement your own row-level and aggregate checks, how to validate their parameters, and how to register them so they behave exactly like built-in checks.

At this point, you should be fully equipped to design and integrate custom, production-ready checks that fit your specific data and business needs — using all the tools the framework provides.
