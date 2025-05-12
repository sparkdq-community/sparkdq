.. _regex_match_check:

Regex Match Check
==================

**Check**: ``regex-match-check``

**Purpose**: Validates that string values in a column match a given regular expression pattern.
This is useful for checking format compliance, such as email addresses, identifiers, or codes.

.. note::

    * By default, null values are skipped. Set ``treat_null_as_failure: true`` to treat them as invalid.

    * Regex is case-sensitive by default. Use ``ignore_case: true`` for case-insensitive matching.


Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import RegexMatchCheckConfig
    from sparkdq.core import Severity

    RegexMatchCheckConfig(
        check_id="valid-email",
        column="email",
        pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
        ignore_case=True,
        treat_null_as_failure=False,
        severity=Severity.CRITICAL
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: regex-match-check
      check-id: valid-email
      column: email
      pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
      ignore-case: true
      treat-null-as-failure: false
      severity: critical

Typical Use Cases
-----------------

* ✅ Validate email address formatting
* ✅ Check code or ID patterns (e.g. `AB-12345`)
* ✅ Detect unexpected value structures
