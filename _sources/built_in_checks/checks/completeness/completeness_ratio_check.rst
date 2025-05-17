.. _completeness-ratio-check:

Completeness Ratio
==================

**Check**: ``completeness-ratio-check``

**Purpose**:  
Validates that the ratio of non-null values in a specified column meets or exceeds a defined threshold (``min_ratio``).  
This allows for soft validation of column completeness without enforcing strict non-null constraints.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import CompletenessRatioCheckConfig
   from sparkdq.core import Severity

   CompletenessRatioCheckConfig(
       check_id="pickup-time-mostly-complete",
       column="tpep_pickup_datetime",
       min_ratio=0.95,
       severity=Severity.WARNING
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: completeness-ratio-check
      check-id: pickup-time-mostly-complete
      column: tpep_pickup_datetime
      min-ratio: 0.95
      severity: warning

Typical Use Cases
-----------------

* ✅ Detect columns with unexpectedly high proportions of missing values.
* ✅ Enforce soft completeness thresholds on optional or partially-populated fields.
* ✅ Ensure minimum data quality for downstream analytics or feature generation.
* ✅ Provide early signals for upstream data loss or extraction failures.
