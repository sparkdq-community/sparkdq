.. _timestamp-max-check:

Timestamp Max
=============

**Check**: ``timestamp-max-check``

**Purpose**:  
Checks whether values in the specified timestamp columns are less than a defined maximum timestamp (`max_value`).  
A row fails the check if any of the selected columns contains a timestamp after the configured `max_value`.

Use the `inclusive` parameter to control boundary behavior:

- inclusive = **False** (default): ``value < max_value``
- inclusive = **True**: ``value <= max_value``

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import TimestampMaxCheckConfig
   from sparkdq.core import Severity

   TimestampMaxCheckConfig(
       check_id="maximum_allowed_event_time",
       columns=["event_time"],
       max_value="2023-12-31 23:59:59",
       inclusive=True,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: timestamp-max-check
      check-id: maximum_allowed_event_time
      columns:
        - event_time
      max-value: "2023-12-31 23:59:59"
      inclusive: true
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that log entries or event timestamps do not lie in the future.
* ✅ Validate that data processing only includes records up to a specific cutoff time.
* ✅ Check that sensor data or measurements were captured within the allowed timeframe.
* ✅ Prevent the inclusion of incorrect future-dated records caused by system errors.
