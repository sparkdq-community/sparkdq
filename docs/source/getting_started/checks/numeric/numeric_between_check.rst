.. _numeric-between-check:

Numeric Between
===============

**Check**: ``numeric-between-check``

**Purpose**:
Checks whether values in the specified numeric columns are within a defined inclusive range between `min_value` and `max_value`.  
A row fails the check if any of the selected columns contains a value below `min_value` or above `max_value`.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import NumericBetweenCheckConfig
   from sparkdq.core import Severity

   NumericBetweenCheckConfig(
       check_id="allowed_discount_range",
       columns=["discount"],
       min_value=0.0,
       max_value=100.0,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: numeric-between-check
      check-id: allowed_discount_range
      columns:
        - discount
      min_value: 0.0
      max_value: 100.0
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that numeric values fall within expected physical or business-defined ranges.

* ✅ Validate that percentages, ratios, or scores stay between 0 and 100.

* ✅ Detect outliers or incorrect data entries outside of acceptable thresholds.
