Row-Level Checks
================

Row-level checks validate individual rows in a DataFrame by applying a boolean condition.  
To implement a custom row-level check, you need two components:

* A ``Check`` class that contains the validation logic  
* A corresponding ``CheckConfig`` class that defines the parameters and registers the check

Implementing the Check Class
----------------------------

Your custom check must inherit from **BaseRowCheck** and implement the ``validate(df: DataFrame)`` method.
This method returns the input DataFrame with an added boolean column indicating whether each row passes.
The base class provides ``with_check_result_column(...)`` to append this result automatically.

Constructor
^^^^^^^^^^^

Every check must also be initialized with two required parameters:

- ``check_id``: a unique identifier for the check instance
- ``severity``: the severity level (default: ``Severity.CRITICAL``)

Both parameters must be passed to the **super().__init__** call inside your check’s constructor.  
This ensures that the check is properly registered in the validation result and handled correctly by the engine.

Minimal Example
^^^^^^^^^^^^^^^

In this example, the check passes if the given column contains a positive value.

.. code-block:: python

   from pyspark.sql import DataFrame
   from pyspark.sql import functions as F

   from sparkdq.core import BaseRowCheck, Severity

   class PositiveValueCheck(BaseRowCheck):
       
       def __init__(self, check_id: str, column: str, severity: Severity = Severity.CRITICAL):
           super().__init__(check_id=check_id, severity=severity)
           self.column = column

       def validate(self, df: DataFrame) -> DataFrame:
           condition = F.col(self.column) > 0
           return self.with_check_result_column(df, condition)

Defining the Configuration class
--------------------------------

The configuration class declares and validates all parameters required for the check. It must inherit from
``BaseRowCheckConfig``, which is based on Pydantic. This means you can take full advantage of all standard
Pydantic features, including type annotations, required and optional fields, default values, and custom
validators. These capabilities allow you to clearly define the shape of your configuration and ensure that
any invalid input is caught early — before the check is created or executed.

To connect the config to the check class, set the ``check_class`` attribute accordingly.  
You must also register the config class using the ``@register_check_config(...)`` decorator, providing a unique name:

.. code-block:: python

   from sparkdq.core import BaseRowCheckConfig
   from sparkdq.plugin import register_check_config

   @register_check_config(check_name="my-positive-value-check")
   class PositiveValueCheckConfig(BaseRowCheckConfig):
       column: str
       check_class = PositiveValueCheck

.. raw:: html

   <hr>

🚀 **Next Step**: In the next section, you’ll learn how to create a custom aggregate check — perfect for
validations based on counts, distributions, or summary statistics.