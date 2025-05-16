.. _unique-rows-check:

Unique Rows
===========

**Check**: ``unique-rows-check``

**Purpose**:  
Ensures that all rows in the dataset are unique, either across all columns or a specified subset.  
This check helps detect unintended data duplication and enforces row-level uniqueness constraints.

.. note::
    
    If no subset is provided, the check considers all columns to determine uniqueness.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import UniqueRowsCheckConfig
   from sparkdq.core import Severity

   UniqueRowsCheckConfig(
       check_id="no_duplicate_rows",
       subset_columns=["trip_id", "pickup_time"],
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: unique-rows-check
      check-id: no_duplicate_rows
      subset-columns:
        - trip_id
        - pickup_time
      severity: critical

Typical Use Cases
-----------------

* ✅ Enforce uniqueness on primary key–like columns (e.g., ``trip_id``, ``user_id``)
* ✅ Detect duplicated records caused by faulty joins, reprocessing, or ingestion errors
* ✅ Ensure referential integrity before merging datasets or writing to transactional stores
* ✅ Validate correctness of deduplication logic in preprocessing pipelines
