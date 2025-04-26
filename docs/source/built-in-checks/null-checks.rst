Null Checks
===========

Null-Check
----------

.. raw:: html

   <hr>

**Purpose**: Verifies whether a given column contains any null values.
Commonly used to ensure mandatory fields are fully populated.

**Type**: Row-level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import NullCheckConfig
   from sparkdq.core import Severity

   NullCheckConfig(
       check_id="require_email",
       columns=["email"],
       severity=Severity.ERROR
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: null-check
      check-id: my-null-check
      columns:
        - email
      severity: error


Not-Null-Check
--------------

.. raw:: html

   <hr>

**Purpose**: Verifies whether a given column contains at least one non-null value.
Helps to detect completely empty columns in the dataset.

**Type**: Row-level

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import NotNullCheckConfig
   from sparkdq.core import Severity

   NotNullCheckConfig(
       check_id="must_remain_empty",
       columns=["deactivated_at"],
       severity=Severity.WARNING
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: not-null-check
      check_id: my-not-null-check
      columns:
        - deactivated_at
      severity: warning
