.. _columns-are-complete-check:

Columns Are Complete
====================

**Check**: ``columns-are-complete-check``

**Purpose**:  
Ensures that all specified columns are **fully populated** (i.e. contain no null values).  
If any null values are found in one of the columns, the **entire dataset is considered invalid**.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import ColumnsAreCompleteCheckConfig
   from sparkdq.core import Severity

   ColumnsAreCompleteCheckConfig(
       check_id="required_fields_check",
       columns=["trip_id", "pickup_time"],
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: columns-are-complete-check
      check-id: required_fields_check
      columns:
        - trip_id
        - pickup_time
      severity: critical

Typical Use Cases
-----------------

* ✅ Enforce critical business fields to be complete (e.g. primary keys, timestamps).
* ✅ Detect corruption or data loss caused by ETL errors or schema mismatches.
* ✅ Ensure key fields required for downstream processing or analytics are intact.
* ✅ Use as a hard fail condition to quarantine incomplete datasets early in the pipeline.
