.. _column-less-than-check:

Column Less Than Check
=======================

**Check**: ``column-less-than-check``

**Purpose**: Ensures that values in one column are strictly less than (or less than or equal to)
the values in another column or the result of evaluating a Spark SQL expression.

.. note::

    * Rows with ``null`` values in either column or the limit result are treated as **invalid** and will fail the check.
    * Use the ``inclusive`` flag to control whether equality (``<=``) is permitted.

        - ``inclusive: false``: requires ``column < limit``
        - ``inclusive: true``: allows ``column <= limit``

Python Configuration
--------------------

.. code-block:: python

    from sparkdq.checks import ColumnLessThanCheckConfig
    from sparkdq.core import Severity

    # Simple column comparison
    ColumnLessThanCheckConfig(
        check_id="pickup-before-dropoff",
        column="pickup_datetime",
        limit="dropoff_datetime",
        inclusive=False,  # use True to allow equality
        severity=Severity.CRITICAL
    )

    # Expression with mathematical operation
    ColumnLessThanCheckConfig(
        check_id="price-with-margin",
        column="cost_price",
        limit="selling_price * 0.8",
        inclusive=True,  # use False to require strict less than
        severity=Severity.CRITICAL
    )

    # Complex conditional expression
    ColumnLessThanCheckConfig(
        check_id="score-validation",
        column="user_score",
        limit="CASE WHEN level='expert' THEN max_score ELSE max_score * 0.9 END",
        inclusive=False,
        severity=Severity.CRITICAL
    )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: column-less-than-check
      check-id: pickup-before-dropoff
      column: pickup_datetime
      limit: dropoff_datetime
      inclusive: false
      severity: critical

    - check: column-less-than-check
      check-id: price-with-margin
      column: cost_price
      limit: selling_price * 0.8
      inclusive: true
      severity: critical

    - check: column-less-than-check
      check-id: score-validation
      column: user_score
      limit: CASE WHEN level='expert' THEN max_score ELSE max_score * 0.9 END
      inclusive: false
      severity: critical

Typical Use Cases
-----------------

* ✅ Enforce correct ordering of timestamps, such as ensuring pickup time is before dropoff time.

* ✅ Detect incorrect or corrupted ranges in financial or numeric fields.

* ✅ Validate business rules with dynamic limits, such as ensuring selling price is within acceptable margins of cost price (e.g., selling_price < cost_price * 1.2).

* ✅ Apply conditional validation logic based on categories or statuses (e.g., score < CASE WHEN level='expert' THEN max_score ELSE max_score * 0.9 END).

* ✅ Prevent invalid or logically inconsistent data entries in business-critical systems.
