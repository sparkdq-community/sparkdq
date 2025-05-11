.. _is-not-contained-in-check:

Is Not Contained In
====================

**Check**: ``is-not-contained-in-check``

**Purpose**:
Verifies that the values in the specified columns are **not** contained within forbidden sets.
Useful for blocking unwanted states, codes, or categories early in the pipeline.

Python Configuration
---------------------

.. code-block:: python

    from sparkdq.checks import IsNotContainedInCheckConfig
    from sparkdq.core import Severity

    IsNotContainedInCheckConfig(
        check_id="block_test_status_and_invalid_countries",
        forbidden_values={
            "status": ["TEST", "DUMMY"],
            "country": ["XX"]
        },
        severity=Severity.CRITICAL
    )

Declarative Configuration
--------------------------

.. code-block:: yaml

    - check: is-not-contained-in-check
      check-id: block_test_status_and_invalid_countries
      forbidden-values:
        status:
          - TEST
          - DUMMY
        country:
          - XX
      severity: critical

Typical Use Cases
-----------------

* ✅ Block processing of test or dummy records in production datasets.
* ✅ Enforce that no invalid country codes (like ``XX`` or ``ZZ``) are present.
* ✅ Detect entries with forbidden status values (e.g., ``DELETED``, ``CANCELLED``).
* ✅ Clean datasets by preventing known bad categories or invalid mappings.

Supported Data Types
---------------------

This check supports any data type that can be compared using equality (e.g., strings, integers, dates).
