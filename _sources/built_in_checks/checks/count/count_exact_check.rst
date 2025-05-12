.. _count-exact-check:

Count Exact
===========

**Check**: ``row-count-exact-check``

**Purpose**:
Verifies that the dataset contains exactly the specified number of rows.
Useful for enforcing strict expectations on data volume.

Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import RowCountExactCheckConfig
    from sparkdq.core import Severity

    RowCountExactCheckConfig(
        check_id="validate_snapshot_size",
        expected_count=500,
        severity=Severity.ERROR
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: row-count-exact-check
      check-id: validate_snapshot_size
      expected-count: 500
      severity: error

Typical Use Cases
-----------------

* ✅ Validate fixed-size imports (e.g., daily exports with exactly 1,000 records).

* ✅ Ensure integrity in snapshot-based pipelines with exact record counts.

* ✅ Identify silent load failures or unintended duplications early in the process.
