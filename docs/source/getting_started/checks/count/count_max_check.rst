.. _count-max-check:

Count Max
=========

**Check**: ``row-count-max-check``

**Purpose**:
Verifies that the input DataFrame does not exceed a specified maximum number of rows.
Helps to detect unexpected data growth or duplication early in the pipeline.

Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import RowCountMaxCheckConfig
    from sparkdq.core import Severity

    RowCountMaxCheckConfig(
        check_id="prevent_oversize_batch",
        max_count=100000,
        severity=Severity.ERROR
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: row-count-max-check
      check-id: prevent_oversize_batch
      max-count: 100000
      severity: error


Typical Use Cases
-----------------

* ✅ Detect abnormal data growth that may indicate duplicates or incorrect joins.

* ✅ Prevent downstream systems (e.g., reports or dashboards) from processing overly large datasets.

* ✅ Catch unintentional full loads when only incremental data was expected.
