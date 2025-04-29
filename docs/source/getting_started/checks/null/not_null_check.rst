.. _not_null_check:

Not Null Check
==============

**Check**: ``not-null-check``

**Purpose**: Checks whether the specified column contains at least one non-null value.
This helps to detect completely empty columns, indicating unused or broken data fields.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import NotNullCheckConfig
   from sparkdq.core import Severity

   NotNullCheckConfig(
       check_id="must_remain_empty",
       columns=["deactivated_at"],
       severity=Severity.WARNING
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: not-null-check
      check-id: my-not-null-check
      columns:
        - deactivated_at
      severity: warning

Typical Use Cases
-----------------

* ✅ Detect columns that are completely empty due to upstream data issues.

* ✅ Identify deprecated fields that are no longer populated but still present in the schema.

* ✅ Prevent unnecessary storage or processing of columns that hold no information.
