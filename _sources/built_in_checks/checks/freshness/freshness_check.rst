.. _freshness-check:

Freshness Check
===============

**Check**: ``freshness-check``

**Purpose**:  
Validates that the **most recent timestamp** in a given column is within a defined freshness threshold relative to the current system time.  
The check fails if the newest value is **older than** the specified interval (e.g., more than 24 hours ago).

.. note::

    The following time units are supported for the `period` parameter:

    * ``year``
    * ``month``
    * ``week``
    * ``day``
    * ``hour``
    * ``minute``
    * ``second``

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import FreshnessCheckConfig
   from sparkdq.core import Severity

   FreshnessCheckConfig(
       check_id="data-update-recent",
       column="last_updated",
       interval=24,
       period="hour",
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: freshness-check
      check-id: data-update-recent
      column: last_updated
      interval: 24
      period: hour
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that ingested data was recently updated (e.g., within the last hour).
* ✅ Detect delays or failures in upstream data ingestion pipelines.
* ✅ Validate that partitioned tables or snapshots are being refreshed regularly.
* ✅ Use to monitor SLAs and enforce data freshness for reporting or analytics.
