.. _date-between-check:

Date Between
============

**Check**: ``date-between-check``

**Purpose**:  
Checks whether values in the specified date columns are within a defined range between `min_value` and `max_value`.  
A row fails the check if any of the selected columns contains a date before `min_value` or after `max_value`.

You can control boundary inclusivity using the `inclusive` parameter:

- inclusive: **[False, False]** (default): strictly between ``min < value < max``
- inclusive: **[True, False]**: include only the lower bound ``min <= value < max``
- inclusive: **[False, True]**: include only the upper bound ``min < value <= max``
- inclusive: **[True, True]**: include both bounds ``min <= value <= max``

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import DateBetweenCheckConfig
   from sparkdq.core import Severity

   DateBetweenCheckConfig(
       check_id="allowed_record_date_range",
       columns=["record_date"],
       min_value="2020-01-01",
       max_value="2023-12-31",
       inclusive=(True, True),
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: date-between-check
      check-id: allowed_record_date_range
      columns:
        - record_date
      min-value: "2020-01-01"
      max-value: "2023-12-31"
      inclusive: [true, true]
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that transaction or event dates fall within a valid business period (e.g., fiscal year).
* ✅ Validate that data entries only contain dates within a specific reporting window.
* ✅ Prevent the processing of outdated or future-dated records outside of the allowed range.
