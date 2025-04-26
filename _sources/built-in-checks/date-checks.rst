Date Checks
===========

Date-Min-Check
--------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified date columns are greater than or equal to a defined
minimum date (`min_value`). A row fails the check if any of the selected columns contains a
date before the configured `min_value`.

**Type**: Row-Level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import DateMinCheckConfig
   from sparkdq.core import Severity

   DateMinCheckConfig(
       check_id="minimum_allowed_record_date",
       columns=["record_date"],
       min_value="2020-01-01",
       severity=Severity.CRITICAL
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: date-min-check
      check_id: minimum_allowed_record_date
      columns:
        - record_date
      min_value: "2020-01-01"
      severity: critical


Date-Max-Check
--------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified date columns are less than or equal to a defined maximum date (`max_value`).  
A row fails the check if any of the selected columns contains a date after the configured `max_value`.

**Type**: Row-Level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import DateMaxCheckConfig
   from sparkdq.core import Severity

   DateMaxCheckConfig(
       check_id="maximum_allowed_record_date",
       columns=["record_date"],
       max_value="2023-12-31",
       severity=Severity.CRITICAL
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: date-max-check
      check_id: maximum_allowed_record_date
      columns:
        - record_date
      max_value: "2023-12-31"
      severity: critical


Date-Between-Check
------------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified date columns are within a defined inclusive range between `min_value` and `max_value`.  
A row fails the check if any of the selected columns contains a date before `min_value` or after `max_value`.

**Type**: Row-Level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import DateBetweenCheckConfig
   from sparkdq.core import Severity

   DateBetweenCheckConfig(
       check_id="allowed_record_date_range",
       columns=["record_date"],
       min_value="2020-01-01",
       max_value="2023-12-31",
       severity=Severity.CRITICAL
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: date-between-check
      check_id: allowed_record_date_range
      columns:
        - record_date
      min_value: "2020-01-01"
      max_value: "2023-12-31"
      severity: critical
