.. _column-presence-check:

Column Presence
===============

**Check**: ``column-presence-check``

**Purpose**:
Verifies that all required columns are present in the DataFrame, regardless of their data types.
Ensures critical columns are available before further processing.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import ColumnPresenceCheckConfig
   from sparkdq.core import Severity

   ColumnPresenceCheckConfig(
       check_id="enforce_required_columns",
       required_columns=["id", "event_timestamp", "status"],
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: column-presence-check
      check-id: enforce_required_columns
      required-columns:
        - id
        - event_timestamp
        - status
      severity: critical

Typical Use Cases
-----------------

* ✅ Detect schema changes early where key columns are accidentally dropped.

* ✅ Validate presence of technical metadata columns like `event_timestamp` or `partition_key`.

* ✅ Prevent silent failures caused by missing columns in ingestion or export pipelines.
