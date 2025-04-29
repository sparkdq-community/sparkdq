.. _numeric-max-check:

Numeric Max
===========

**Check**: ``numeric-max-check``

**Purpose**:
Checks whether values in the specified numeric columns are less than a defined maximum value (`max_value`).  
A row fails the check if any of the selected columns contains a value greater than or equal to the configured `max_value`.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import NumericMaxCheckConfig
   from sparkdq.core import Severity

   NumericMaxCheckConfig(
       check_id="maximum_allowed_discount",
       columns=["discount"],
       max_value=100.0,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: numeric-max-check
      check-id: maximum_allowed_discount
      columns:
        - discount
      max_value: 100.0
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that prices, quantities, or measurements do not exceed defined maximum limits.

* ✅ Validate sensor readings against physical upper thresholds.

* ✅ Check maximum allowed values for KPIs, metrics, or score calculations (e.g., percentages ≤ 100).

* ✅ Detect outliers or erroneous data entries that exceed expected ranges.
