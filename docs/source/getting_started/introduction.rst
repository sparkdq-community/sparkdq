Introduction
============

To get the most out of **SparkDQ**, it's helpful to understand how its core components fit together. This section
gives you a high-level overview of how data validation works in SparkDQ, what the main building blocks are,
and how they interact in a typical validation flow.

Core Concepts
-------------

SparkDQ is built around four modular components that together provide a clean, scalable, and Spark-native validation pipeline.
This flexible architecture makes it easy to integrate SparkDQ into pipelines of any size, reuse validation logic across projects,
and consistently enforce data quality standards.

CheckConfig
^^^^^^^^^^^

A declarative, type-safe definition of a single validation rule. Each check (e.g., null check, row count constraint)
has its own configuration class. These can be created directly in Python or loaded from structured formats like YAML or JSON.

CheckSet
^^^^^^^^

A container that groups multiple checks into a reusable set. Whether you define one rule or a hundred, they are all managed and executed as part of a CheckSet.

ValidationEngine
^^^^^^^^^^^^^^^^

The engine (e.g. BatchDQEngine) takes a CheckSet and a Spark DataFrame, applies all rules to the data, and returns structured results. It handles both row-level and dataset-level validations in a single run.

ValidationResult
^^^^^^^^^^^^^^^^

The output of the validation process. It contains filtered views of passed, failed, and warning-level records, as well as summary statistics and metadata (e.g., pass rate, check counts, timestamps).

Typical Workflow
----------------

1. Define validation rules using CheckConfig classes (either in Python code or by loading from config files)

2. Add checks to a CheckSet, which acts as the single source of truth for all checks to be run

3. Run the CheckSet against a Spark DataFrame using a validation engine like BatchDQEngine

4. Inspect the result via methods like **.pass_df()**, **.fail_df()**, **.warn_df()**, or **.summary()**

5. Act on the result — filter rows, block execution, raise alerts, or save outcomes

Workflow Example
----------------

.. code-block:: python

    from sparkdq.checks import NullCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    # Step 1: Define a validation rule
    check_set = CheckSet()
    check_set.add_check(NullCheckConfig(check_id="my-null-check", columns=["email"]))

    # Step 2: Read your input data
    df = spark.read.parquet("/path/to/data")

    # Step 3: Run the validation
    result = BatchDQEngine(check_set).run_batch(df)

    # Step 4: Show only valid rows
    result.pass_df().show()

By clearly separating check definitions, execution, and result handling, the framework helps you integrate
quality control into your PySpark pipelines without introducing unnecessary complexity.

.. raw:: html

   <hr>

🚀 **Next Step**: Ready to go deeper? Let's define checks and explore the results.