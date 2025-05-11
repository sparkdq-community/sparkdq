.. _schema-check:

Schema Check
============

**Check**: ``schema-check``

**Purpose**:
Verifies that the DataFrame matches an expected schema in terms of column names and Spark data types.
Optionally enforces strict matching by rejecting unexpected extra columns.

Supported Data Types
---------------------

The following Spark types are supported and must be specified as lowercase strings:

``string``, ``boolean``, ``int``, ``bigint``, ``float``, ``double``, ``date``
``timestamp``, ``binary``, ``array``, ``map``, ``struct``, ``decimal(precision, scale)`` — e.g.,
``decimal(10,2)``

.. important::

    - For ``decimal`` types, both precision and scale must be specified inside parentheses.
    - No other formats (e.g., ``integer``, ``decimal(10.2)``) are accepted.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import SchemaCheckConfig
   from sparkdq.core import Severity

   SchemaCheckConfig(
       check_id="enforce_schema_contract",
       expected_schema={
           "id": "int",
           "name": "string",
           "amount": "decimal(10,2)",
           "created_at": "timestamp"
       },
       strict=True,
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: schema-check
      check-id: enforce_schema_contract
      expected-schema:
        id: int
        name: string
        amount: decimal(10,2)
        created_at: timestamp
      strict: true
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure schema consistency between ingestion, transformation, and consumption stages.

* ✅ Detect missing or renamed columns early in the pipeline.

* ✅ Catch incorrect data types that may lead to casting errors or incorrect aggregations.

* ✅ Enforce a strict schema contract in production pipelines to prevent silent data corruption.
