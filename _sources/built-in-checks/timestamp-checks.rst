Timestamp Checks
================

Timestamp-Min-Check
-------------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified timestamp columns are greater than or equal to a defined minimum timestamp (`min_value`).  
A row fails the check if any of the selected columns contains a timestamp before the configured `min_value`.

**Type**: Row-Level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import TimestampMinCheckConfig
   from sparkdq.core import Severity

   TimestampMinCheckConfig(
       check_id="minimum_allowed_event_time",
       columns=["event_time"],
       min_value="2020-01-01 00:00:00",
       severity=Severity.CRITICAL
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: timestamp-min-check
      check_id: minimum_allowed_event_time
      columns:
        - event_time
      min_value: "2020-01-01 00:00:00"
      severity: critical


Timestamp-Max-Check
-------------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified timestamp columns are less than or equal to a defined maximum timestamp (`max_value`).  
A row fails the check if any of the selected columns contains a timestamp after the configured `max_value`.

**Type**: Row-Level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import TimestampMaxCheckConfig
   from sparkdq.core import Severity

   TimestampMaxCheckConfig(
       check_id="maximum_allowed_event_time",
       columns=["event_time"],
       max_value="2023-12-31 23:59:59",
       severity=Severity.CRITICAL
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: timestamp-max-check
      check_id: maximum_allowed_event_time
      columns:
        - event_time
      max_value: "2023-12-31 23:59:59"
      severity: critical


Timestamp-Between-Check
-----------------------

.. raw:: html

   <hr>

**Purpose**:  
Checks whether values in the specified timestamp columns are within a defined inclusive range between `min_value` and `max_value`.  
A row fails the check if any of the selected columns contains a timestamp before `min_value` or after `max_value`.

**Type**: Row-Level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import TimestampBetweenCheckConfig
   from sparkdq.core import Severity

   TimestampBetweenCheckConfig(
       check_id="allowed_event_time_range",
       columns=["event_time"],
       min_value="2020-01-01 00:00:00",
       max_value="2023-12-31 23:59:59",
       severity=Severity.CRITICAL
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: timestamp-between-check
      check_id: allowed_event_time_range
      columns:
        - event_time
      min_value: "2020-01-01 00:00:00"
      max_value: "2023-12-31 23:59:59"
      severity: critical
