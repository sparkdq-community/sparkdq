.. _foreign-key-check:

Foreign Key Check
=================

**Check**: ``foreign-key-check``

**Purpose**:  
Validates that all values in a specified column exist in a reference dataset's column.  
This check ensures referential integrity between two datasets — typically used to verify that foreign keys are resolvable.

This check operates at the **aggregate level**.  
It computes the number and ratio of missing foreign key references and returns a single validation result for the entire dataset.

The check **fails** if any value in the source column is **not found** in the referenced column of the injected reference dataset.

Python Configuration
--------------------

.. code-block:: python

   from sparkdq.checks import ForeignKeyCheckConfig
   from sparkdq.core import Severity

   ForeignKeyCheckConfig(
       check_id="valid_customer_id",
       column="customer_id",
       reference_dataset="customers",
       reference_column="id",
       severity=Severity.CRITICAL
   )

Declarative Configuration
-------------------------

.. code-block:: yaml

    - check: foreign-key-check
      check-id: valid_customer_id
      column: customer_id
      reference-dataset: customers
      reference-column: id
      severity: critical

Typical Use Cases
-----------------

* ✅ Ensure that every `order.customer_id` exists in the `customers` table.
* ✅ Validate that foreign keys in fact tables (e.g., `sales.product_id`) refer to known dimension entries.
* ✅ Guarantee relational integrity between joined datasets in data lakes or data warehouses.
