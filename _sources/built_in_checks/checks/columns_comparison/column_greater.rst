.. _column-greater-than-check:

Column Greater Than Check
=========================

**Check**: ``column-greater-than-check``

**Purpose**: Ensures that values in one column are strictly greater than (or greater than or equal to)
the values in another column or the result of evaluating a Spark SQL expression.

.. note::

    * Rows with ``null`` values in either column or the limit result are treated as **invalid** and will fail the check.
    * Use the ``inclusive`` flag to control whether equality (``>=``) is permitted.

        - ``inclusive: false``: requires ``column > limit``
        - ``inclusive: true``: allows ``column >= limit``

Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import ColumnGreaterThanCheckConfig
    from sparkdq.core import Severity

    # Simple column comparison
    ColumnGreaterThanCheckConfig(
        check_id="dropoff-after-pickup",
        column="dropoff_datetime",
        limit="pickup_datetime",
        inclusive=False,  # use True to allow equality
        severity=Severity.CRITICAL
    )

    # Expression with mathematical operation
    ColumnGreaterThanCheckConfig(
        check_id="price-with-margin",
        column="selling_price",
        limit="cost_price * 1.2",
        inclusive=True,  # use False to require strict greater than
        severity=Severity.CRITICAL
    )

    # Complex conditional expression
    ColumnGreaterThanCheckConfig(
        check_id="score-validation",
        column="user_score",
        limit="CASE WHEN level='beginner' THEN min_score ELSE min_score * 1.1 END",
        inclusive=False,
        severity=Severity.CRITICAL
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: column-greater-than-check
      check-id: dropoff-after-pickup
      column: dropoff_datetime
      limit: pickup_datetime
      inclusive: false
      severity: critical

    - check: column-greater-than-check
      check-id: price-with-margin
      column: selling_price
      limit: cost_price * 1.2
      inclusive: true
      severity: critical

    - check: column-greater-than-check
      check-id: score-validation
      column: user_score
      limit: CASE WHEN level='beginner' THEN min_score ELSE min_score * 1.1 END
      inclusive: false
      severity: critical

Typical Use Cases
-----------------

* ✅ Enforce correct ordering of timestamps, such as ensuring dropoff time is after pickup time.

* ✅ Detect incorrect or corrupted ranges in financial or numeric fields.

* ✅ Validate business rules with dynamic limits, such as ensuring selling price exceeds minimum margins above cost price (e.g., selling_price > cost_price * 1.2).

* ✅ Apply conditional validation logic based on categories or statuses (e.g., score > CASE WHEN level='beginner' THEN min_score ELSE min_score * 1.1 END).

* ✅ Prevent invalid or logically inconsistent data entries in business-critical systems.