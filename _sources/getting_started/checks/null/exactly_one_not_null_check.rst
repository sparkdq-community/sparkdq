.. _exactly_one_not_null_check:

Exactly One Not Null Check
==========================

**Check**: ``exactly-one-not-null-check``

**Purpose**:  
Checks whether **exactly one** of the specified columns is non-null in each row.  
A row fails the check if **none** or **more than one** of the columns are populated.
This is useful for enforcing mutual exclusivity — for example, when exactly
one of `email`, `phone`, or `user_id` must be present.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import ExactlyOneNotNullCheckConfig
   from sparkdq.core import Severity

   ExactlyOneNotNullCheckConfig(
       check_id="must_provide_exactly_one_id",
       columns=["email", "phone", "user_id"],
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: exactly-one-not-null-check
      check-id: must_provide_exactly_one_id
      columns:
        - email
        - phone
        - user_id
      severity: critical

Typical Use Cases
-----------------

* ✅ Enforce mutual exclusivity across optional identifiers (e.g. email **or** phone **or** user_id).

* ✅ Prevent ambiguous data entries where multiple conflicting fields are filled.

* ✅ Detect rows missing essential identification if all relevant fields are null.
