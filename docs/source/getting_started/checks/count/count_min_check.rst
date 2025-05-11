.. _count-min-check:

Count Min
=========

**Check**: ``row-count-min-check``

**Purpose**:
Verifies that the input DataFrame contains at least a specified minimum number of rows.
Prevents downstream processing on incomplete or unexpectedly small datasets.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import RowCountMinCheckConfig
   from sparkdq.core import Severity

   RowCountMinCheckConfig(
       check_id="minimum_required_records",
       min_count=10000,
       severity=Severity.WARNING
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: row-count-min-check
      check-id: minimum_required_records
      min-count: 10000
      severity: warning

Typical Use Cases
-----------------

* ✅ Validate that a data extraction or ingestion process has produced a sufficient number of records.

* ✅ Detect partial loads or failed data transfers that result in missing data.

* ✅ Enforce minimum data volume requirements for reliable analytics, training datasets, or reporting.

* ✅ Prevent downstream processes from running on incomplete data.
