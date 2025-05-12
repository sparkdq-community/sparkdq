.. _timestamp-between-check:

Timestamp Between
=================

**Check**: ``timestamp-between-check``

**Purpose**:  
Checks whether values in the specified timestamp columns are within a defined range between `min_value` and `max_value`.  
A row fails the check if any of the selected columns contains a timestamp before `min_value` or after `max_value`.

Use the `inclusive` parameter to control boundary behavior:

- inclusive: **[False, False]** (default): strictly between ``min < value < max``
- inclusive: **[True, False]**: include only the lower bound ``min <= value < max``
- inclusive: **[False, True]**: include only the upper bound ``min < value <= max``
- inclusive: **[True, True]**: include both bounds ``min <= value <= max``

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import TimestampBetweenCheckConfig
   from sparkdq.core import Severity

   TimestampBetweenCheckConfig(
       check_id="allowed_event_time_range",
       columns=["event_time"],
       min_value="2020-01-01 00:00:00",
       max_value="2023-12-31 23:59:59",
       inclusive=(True, True),
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: timestamp-between-check
      check-id: allowed_event_time_range
      columns:
        - event_time
      min-value: "2020-01-01 00:00:00"
      max-value: "2023-12-31 23:59:59"
      inclusive: [true, true]
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that log entries or event timestamps fall within a specific processing window.
* ✅ Validate that measurement or sensor data was collected within the expected observation period.
* ✅ Check that data entries are limited to a valid reporting timeframe.
* ✅ Prevent the processing of outdated or future-dated records outside of the allowed timestamp range.
