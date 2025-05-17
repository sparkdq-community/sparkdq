.. _unique-ratio-check:

Unique Ratio
============

**Check**: ``unique-ratio-check``

**Purpose**:  
Checks whether the ratio of unique (non-null) values in a specified column meets or exceeds a configured threshold.  
A row does not directly fail; instead, the dataset is considered invalid if the proportion of unique values is too low.

.. note::

    * If the configured ``min-ratio`` is not met, the check fails.  
    * Null values are excluded from the uniqueness calculation.  
    * The total number of rows is used as the denominator (including nulls).

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import UniqueRatioCheckConfig
   from sparkdq.core import Severity

   UniqueRatioCheckConfig(
       check_id="vendor-id-uniqueness",
       column="VendorID",
       min_ratio=0.7,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: unique-ratio-check
      check-id: vendor-id-uniqueness
      column: VendorID
      min-ratio: 0.7
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that a column intended to be mostly unique (e.g., IDs, hashes) behaves as expected.

* ✅ Detect issues where only a few values are repeated frequently, reducing feature usefulness.

* ✅ Prevent downstream errors due to low-entropy or non-discriminative values.

* ✅ Support feature quality checks in ML preprocessing pipelines.

