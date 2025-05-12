.. _numeric-min-check:

Numeric Min
===========

**Check**: ``numeric-min-check``

**Purpose**:  
Checks whether values in the specified numeric columns are **strictly greater than** a defined minimum value (`min_value`).  
A row fails the check if any of the selected columns contains a value **less than** the minimum, or **equal to it** if `inclusive=False`.

You can control inclusivity with the `inclusive` parameter:

- inclusive = **False** (default): ``value > min_value``
- inclusive = **True**: ``value >= min_value``

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import NumericMinCheckConfig
   from sparkdq.core import Severity

   NumericMinCheckConfig(
       check_id="minimum_allowed_price",
       columns=["price", "discount"],
       min_value=0.0,
       inclusive=True,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: numeric-min-check
      check-id: minimum_allowed_price
      columns:
        - price
        - discount
      min-value: 0.0
      inclusive: true
      severity: critical

Typical Use Cases
-----------------

- ✅ Ensure that sales amounts are not negative.
- ✅ Validate sensor readings against physical minimum thresholds.
- ✅ Check minimum allowed values for KPIs or metrics.
