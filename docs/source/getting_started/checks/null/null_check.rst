.. _null_check:

Null Check
==========

**Check**: ``null-check``

**Purpose**: Verifies that the specified columns do not contain null values.
This check is typically used to ensure the completeness of mandatory fields
and to prevent incomplete or invalid records from entering downstream processes.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import NullCheckConfig
   from sparkdq.core import Severity

   NullCheckConfig(
       check_id="require_email",
       columns=["email"],
       severity=Severity.ERROR
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: null-check
      check-id: my-null-check
      columns:
        - email
      severity: error

Typical Use Cases
-----------------

* ✅ Ensure that mandatory fields (e.g., IDs, keys, or business-critical attributes) are fully populated.

* ✅ Validate that primary key columns do not contain null values to guarantee referential integrity.

* ✅ Prevent incomplete records from entering downstream processes, reports, or analytics.

* ✅ Detect data gaps early to avoid issues in data transformations or aggregations.
