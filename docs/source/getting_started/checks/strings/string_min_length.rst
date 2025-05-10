.. _string_min_length_check:

String Min Length Check
=======================

**Check**: ``string-min-length-check``

**Purpose**: Checks that non-null string values in a specified column meet a minimum length requirement.
This helps ensure that textual data is not truncated or malformed.

Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import StringMinLengthCheckConfig
    from sparkdq.core import Severity

    StringMinLengthCheckConfig(
        check_id="zone-names-valid-length",
        column="Zone",
        min_length=3,
        inclusive=True,  # Use False for strict mode (i.e. > instead of >=)
        severity=Severity.CRITICAL
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: string-min-length-check
      check-id: zone-names-valid-length
      column: Zone
      min_length: 3
      inclusive: true
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensuring that key text fields (e.g. zone names, codes) are not too short.
* ✅ Detecting truncated or improperly formatted string data.
