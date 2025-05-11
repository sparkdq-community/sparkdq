.. _string_max_length_check:

String Max Length Check
========================

**Check**: ``string-max-length-check``

**Purpose**: Validates that non-null string values in a column do not exceed a maximum length.
This helps detect overflow, incorrectly padded, or malformed string data.

.. note::

    * Null values are treated as valid (they are ignored by the check).

    * Use `inclusive` to control whether `length == max_length` is allowed:

        ``inclusive: true`` → `len(column) ≤ max_length`

        ``inclusive: false`` → `len(column) < max_length`

Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import StringMaxLengthCheckConfig
    from sparkdq.core import Severity

    StringMaxLengthCheckConfig(
        check_id="short-zone-names",
        column="Zone",
        max_length=20,
        inclusive=True,
        severity=Severity.WARNING
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: string-max-length-check
      check-id: short-zone-names
      column: Zone
      max-length: 20
      inclusive: true
      severity: warning

Typical Use Cases
-----------------

* ✅ Detect padded values or overlong entries due to integration bugs.

* ✅ Enforce length constraints in user-facing systems (e.g. UI field limits, database schema).

* ✅ Identify legacy fields that may hold excess or irrelevant string content.
