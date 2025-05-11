.. _is-contained-in-check:

Is Contained In
===============

**Check**: ``is-contained-in-check``

**Purpose**:
Verifies that the values in the specified columns are contained within predefined sets of allowed values.
Useful to enforce domain constraints or catch unexpected values early in the pipeline.

Python Configuration
---------------------

.. code-block:: python

    from sparkdq.checks import IsContainedInCheckConfig
    from sparkdq.core import Severity

    IsContainedInCheckConfig(
        check_id="validate_status_and_country",
        allowed_values={
            "status": ["ACTIVE", "INACTIVE", "PENDING"],
            "country": ["DE", "FR", "IT"]
        },
        severity=Severity.ERROR
    )

Declarative Configuration
--------------------------

.. code-block:: yaml

    - check: is-contained-in-check
      check-id: validate_status_and_country
      allowed-values:
        status:
          - ACTIVE
          - INACTIVE
          - PENDING
        country:
          - DE
          - FR
          - IT
      severity: error

Typical Use Cases
-----------------

* ✅ Validate that status fields contain only allowed operational states.
* ✅ Check that country codes belong to a known set of supported countries.
* ✅ Ensure reference data mappings only include allowed categories.
* ✅ Catch ingestion errors where unexpected values are introduced.

Supported Data Types
---------------------

This check supports any data type that can be compared using equality (e.g., strings, integers, dates).
