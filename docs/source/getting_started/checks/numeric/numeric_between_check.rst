.. _numeric-between-check:

Numeric Between
===============

**Check**: ``numeric-between-check``

**Purpose**:  
Checks whether values in the specified numeric columns are within a defined range between `min_value` and `max_value`.  
A row fails the check if any of the selected columns contains a value below `min_value` or above `max_value`, based on the configured inclusivity.

You can control inclusivity for each boundary using the `inclusive` parameter:

- inclusive: **[False, False]** (default): strictly between ``min < value < max``
- inclusive: **[True, False]**: include only the lower bound ``min <= value < max``
- inclusive: **[False, True]**: include only the upper bound ``min < value <= max``
- inclusive: **[True, True]**: include both bounds ``min <= value <= max``

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
       inclusive=(True, True),
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: numeric-between-check
      check-id: allowed_discount_range
      columns:
        - discount
      min-value: 0.0
      max-value: 100.0
      inclusive: [true, true]
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that numeric values fall within expected physical or business-defined ranges.
* ✅ Validate that percentages, ratios, or scores stay between 0 and 100.
* ✅ Detect outliers or incorrect data entries outside of acceptable thresholds.
