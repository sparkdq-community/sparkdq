.. _column_less_than_check:

Column Less Than Check
=======================

**Check**: ``column-less-than-check``

**Purpose**: Ensures that values in one column are strictly less than (or less than or equal to)
the values in another column.

.. note::

    * Rows with ``null`` values in either column are treated as **invalid** and will fail the check.

    * Use the ``inclusive`` flag to control whether equality (``<=``) is permitted.

        - ``inclusive: false``: requires ``smaller_column < greater_column``  
        - ``inclusive: true``: allows ``smaller_column <= greater_column``

Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import ColumnLessThanCheckConfig
    from sparkdq.core import Severity

    ColumnLessThanCheckConfig(
        check_id="pickup-before-dropoff",
        smaller_column="pickup_datetime",
        greater_column="dropoff_datetime",
        inclusive=False,  # use True to allow equality
        severity=Severity.CRITICAL
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: column-less-than-check
      check-id: pickup-before-dropoff
      smaller-column: pickup_datetime
      greater-column: dropoff_datetime
      inclusive: false
      severity: critical

Typical Use Cases
-----------------

* ✅ Enforce correct ordering of timestamps, such as ensuring pickup time is before dropoff time.

* ✅ Detect incorrect or corrupted ranges in financial or numeric fields.

* ✅ Prevent invalid or logically inconsistent data entries in business-critical systems.
