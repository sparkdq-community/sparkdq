.. _date-min-check:

Date Min
========

**Check**: ``date-min-check``

**Purpose**:  
Checks whether values in the specified date columns are **greater than** a defined minimum date.  
A row fails the check if any of the selected columns contains a date before the configured `min_value`.

You can control inclusivity using the `inclusive` parameter:

- inclusive = **False** (default): ``value > min_value``
- inclusive = **True**: ``value >= min_value``

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import DateMinCheckConfig
   from sparkdq.core import Severity

   DateMinCheckConfig(
       check_id="minimum_allowed_record_date",
       columns=["record_date"],
       min_value="2020-01-01",
       inclusive=True,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: date-min-check
      check-id: minimum_allowed_record_date
      columns:
        - record_date
      min-value: "2020-01-01"
      inclusive: true
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that event or transaction dates are not earlier than a defined start date.
* ✅ Check that data entries only contain dates within the valid business period.
* ✅ Prevent processing of outdated or legacy data outside of the expected time range.
