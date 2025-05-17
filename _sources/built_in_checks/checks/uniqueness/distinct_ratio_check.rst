.. _distinct-ratio-check:

Distinct Ratio
==============

**Check**: ``distinct-ratio-check``

**Purpose**:  
Validates that the ratio of distinct (non-null) values in a specified column exceeds a minimum threshold.  
A row set fails the check if the actual ratio of distinct values is lower than the configured ``min_ratio``.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import DistinctRatioCheckConfig
   from sparkdq.core import Severity

   DistinctRatioCheckConfig(
       check_id="passenger-count-uniqueness",
       column="passenger_count",
       min_ratio=0.8,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: distinct-ratio-check
      check-id: passenger-count-uniqueness
      column: passenger_count
      min-ratio: 0.8
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that a column has a sufficiently high number of distinct (non-null) values.
* ✅ Detect columns that may have too much repetition or lack of variability.
* ✅ Identify potential issues such as constants, default-filled fields, or data entry errors.
* ✅ Enforce entropy or uniqueness expectations for features used in ML models or analytics.
