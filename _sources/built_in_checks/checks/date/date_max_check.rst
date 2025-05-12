.. _date-max-check:

Date Max
========

**Check**: ``date-max-check``

**Purpose**:  
Checks whether values in the specified date columns are **less than** a defined maximum date.  
A row fails the check if any of the selected columns contains a date after the configured `max_value`.

You can control inclusivity using the `inclusive` parameter:

- inclusive = **False** (default): ``value < max_value``
- inclusive = **True**: ``value <= max_value``

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import DateMaxCheckConfig
   from sparkdq.core import Severity

   DateMaxCheckConfig(
       check_id="maximum_allowed_record_date",
       columns=["record_date"],
       max_value="2023-12-31",
       inclusive=True,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: date-max-check
      check-id: maximum_allowed_record_date
      columns:
        - record_date
      max-value: "2023-12-31"
      inclusive: true
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that event or transaction dates do not lie in the future.
* ✅ Validate that timestamps for data entries stay within the expected reporting period.
* ✅ Check that measurement dates are not beyond a configured cutoff date (e.g., end of fiscal year).
* ✅ Prevent accidental inclusion of incorrectly future-dated records due to data entry errors.
