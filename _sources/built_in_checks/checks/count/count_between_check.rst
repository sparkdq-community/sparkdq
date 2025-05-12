.. _count-between-check:

Count Between
=============

**Check**: ``row-count-between-check``

**Purpose**:
Verifies that the number of rows in the dataset falls within a specified minimum and maximum range.
Ensures data completeness while preventing unexpected data volume.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import RowCountBetweenCheckConfig
   from sparkdq.core import Severity

   RowCountBetweenCheckConfig(
       check_id="expected_daily_batch_size",
       min_count=1000,
       max_count=5000,
       severity=Severity.ERROR
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: row-count-between-check
      check-id: expected_daily_batch_size
      min-count: 1000
      max-count: 5000
      severity: error

Typical Use Cases
-----------------

* ✅ Detect partial loads (too few rows) or runaway joins / duplications (too many rows).

* ✅ Validate dataset size before triggering downstream jobs like training, reporting, or exports.

* ✅ Catch filter changes that unintentionally affect row count.
