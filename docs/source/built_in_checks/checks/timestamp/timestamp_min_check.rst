.. _timestamp-min-check:

Timestamp Min
=============

**Check**: ``timestamp-min-check``

**Purpose**:  
Checks whether values in the specified timestamp columns are greater than a defined minimum timestamp (`min_value`).  
A row fails the check if any of the selected columns contains a timestamp before the configured `min_value`.

Use the `inclusive` parameter to control boundary behavior:

- inclusive = **False** (default): ``value > min_value``
- inclusive = **True**: ``value >= min_value``


Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import TimestampMinCheckConfig
   from sparkdq.core import Severity

   TimestampMinCheckConfig(
       check_id="minimum_allowed_event_time",
       columns=["event_time"],
       min_value="2020-01-01 00:00:00",
       inclusive=True,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: timestamp-min-check
      check-id: minimum_allowed_event_time
      columns:
        - event_time
      min-value: "2020-01-01 00:00:00"
      inclusive: true
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that log entries or event timestamps are not older than a specified start time.
* ✅ Validate that data collection started after a certain system deployment or go-live timestamp.
* ✅ Check that sensor readings or measurement data are within the valid observation period.
* ✅ Prevent processing of outdated records outside the defined operational timeframe.
