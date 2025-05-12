.. _string_length_between_check:

String Length Between Check
============================

**Check**: ``string-length-between-check``

**Purpose**: Validates that non-null string values in a column have a length within a specified range.
This helps ensure that strings are neither too short nor too long, which is useful for validating
user input, field formatting, or value truncation risks.

.. note::

    * Null values are ignored and treated as valid.

    * You can configure both ends of the range to be inclusive or exclusive:

        ``inclusive: [true, false]`` → `min_length <= len(value) < max_length`

        ``inclusive: [false, true]`` → `min_length < len(value) <= max_length`

Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import StringLengthBetweenCheckConfig
    from sparkdq.core import Severity

    StringLengthBetweenCheckConfig(
        check_id="valid-zone-length",
        column="Zone",
        min_length=3,
        max_length=20,
        inclusive=(True, False),
        severity=Severity.CRITICAL
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: string-length-between-check
      check-id: valid-zone-length
      column: Zone
      min-length: 3
      max-length: 20
      inclusive: [true, false]
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure user-entered strings meet minimum and maximum length requirements.

* ✅ Detect truncated, malformed, or overly verbose values in system integrations.

* ✅ Enforce business rules that require string values to fall within a specific range.
