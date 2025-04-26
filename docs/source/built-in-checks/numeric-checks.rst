Numeric Checks
==============

Numeric-Min-Check
-----------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified numeric columns are strictly greater than a defined minimum value (`min_value`).  
A row fails the check if any of the selected columns contains a value less than or equal to the configured `min_value`.

**Type**: Row-Level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import NumericMinCheckConfig
   from sparkdq.core import Severity

   NumericMinCheckConfig(
       check_id="minimum_allowed_price",
       columns=["price", "discount"],
       min_value=0.0,
       severity=Severity.CRITICAL
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: numeric-min-check
      check_id: minimum_allowed_price
      columns:
        - price
        - discount
      min_value: 0.0
      severity: critical


Numeric-Max-Check
-----------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified numeric columns are less than or equal to a defined maximum value (`max_value`).  
A row fails the check if any of the selected columns contains a value greater than the configured `max_value`.

**Type**: Row-Level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import NumericMaxCheckConfig
   from sparkdq.core import Severity

   NumericMaxCheckConfig(
       check_id="maximum_allowed_discount",
       columns=["discount"],
       max_value=100.0,
       severity=Severity.CRITICAL
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: numeric-max-check
      check_id: maximum_allowed_discount
      columns:
        - discount
      max_value: 100.0
      severity: critical


Numeric-Between-Check
---------------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified numeric columns are within a defined inclusive range between `min_value` and `max_value`.  
A row fails the check if any of the selected columns contains a value below `min_value` or above `max_value`.

**Type**: Row-Level

**Python Configuration**:

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

**YAML Configuration**:

.. code-block:: yaml

    - check: numeric-between-check
      check_id: allowed_discount_range
      columns:
        - discount
      min_value: 0.0
      max_value: 100.0
      severity: critical
