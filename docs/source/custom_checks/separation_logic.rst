Configuration and Check
=======================

In this framework, each data quality check is split into two distinct components:

- A ``CheckConfig`` class that declares *what* should be validated and with which parameters
- A ``Check`` class that implements *how* that validation is executed on a Spark DataFrame

This design might seem unusual at first, but it’s a deliberate architectural choice — and essential for the
framework’s plugin-based and factory-driven approach.

Why are config and logic separated?
-----------------------------------

Declarative setup
^^^^^^^^^^^^^^^^^

Configurations define checks in a declarative way. This makes them easy to serialize (e.g. in YAML/JSON),
document, validate, and reuse across environments or pipelines.

Centralized validation
^^^^^^^^^^^^^^^^^^^^^^

All input validation happens in the ``CheckConfig`` class, using `Pydantic`. This ensures that logical issues
like **min_value > max_value** are caught early — before any Spark computation happens.

Separation of concerns
^^^^^^^^^^^^^^^^^^^^^^
By separating configuration (parameters) from logic (execution), both components stay simple and focused.
Config classes don’t contain Spark logic, and check classes don’t contain parsing or validation.

Required for the Factory Pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This separation is what enables the factory mechanism. The ``CheckFactory`` takes in raw configuration data,
creates a validated ``CheckConfig`` instance, and then uses it to build the executable check.

How they work together
----------------------

Each configuration class is explicitly linked to a corresponding check class. This relationship is defined
using a ``check_class`` attribute inside the config. The configuration class is also registered under a unique
check name using a decorator, so that the framework can later identify it through the registry.

Once a configuration has been defined and validated (typically using Pydantic), it exposes a ``.to_check()``
method. This method creates the actual check instance based on the configuration’s internal parameters. The
resulting check is fully initialized and ready to be executed by the engine.

This two-step design — from configuration to check — makes it possible to define data quality checks
declaratively and instantiate them safely and consistently. It also ensures that users never have to
manually instantiate check classes themselves. Instead, they work entirely with configuration objects,
keeping the system modular, declarative, and robust.

Minimal Example
^^^^^^^^^^^^^^^

.. code-block:: python

   class PositiveValueCheck(RowLevelCheck):
       
       def __init__(self, check_id: str, column: str, severity: Severity = Severity.CRITICAL):
           super().__init__(check_id=check_id, severity=severity)
           self.column = column

       def validate(self, df: DataFrame) -> DataFrame:
           condition = F.col(self.column) > 0
           return self.with_check_result_column(df, condition)

   @register_check_config(check_name="positive-value")
   class PositiveValueCheckConfig(BaseRowCheckConfig):
       column: str
       check_class = PositiveValueCheck

   # Usage
   config = PositiveValueCheckConfig(check_id="check-1", column="salary")
   check = config.to_check()

.. raw:: html

   <hr>

🚀 **Next Step**: In the next section, we’ll take a closer look at the plugin architecture — how checks
are registered, discovered, and created dynamically.
