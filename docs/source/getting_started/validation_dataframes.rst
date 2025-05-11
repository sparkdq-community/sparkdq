Validating DataFrames
=====================

Once you have defined your checks and grouped them into a ``CheckSet``, the next step is to validate your data.
SparkDQ provides a Spark-native, extensible engine that evaluates your rules, annotates your DataFrame, and
delivers a structured result that supports both strict enforcement and flexible data routing.

This section explains how to execute validations and interpret the results.

Running a Validation
--------------------

SparkQ makes it easy to validate your data by combining all configured rules into a single validation pass —
one that is scalable, Spark-native, and ready to integrate into any production pipeline.

With just few lines of code, you can apply all row-level and aggregate-level checks to a Spark DataFrame and
receive a rich, structured result in return.

Here’s how it works:

.. code-block:: python

    from sparkdq.checks import NullCheckConfig
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    check_set = CheckSet()
    check_set.add_check(NullCheckConfig(check_id="my-null-check", columns=["email"]))

    # Validate the dataframe
    result = BatchDQEngine(check_set).run_batch(df)

    # Show clean records
    result.pass_df().show()

    # Show failed records with error messages
    result.fail_df().select("_dq_errors").show(truncate=False)

    # Print summary statistics
    print(result.summary())


What Happens Under the Hood?
----------------------------

When you call ``run_batch()``, the engine will:

* Apply row-level checks (e.g. null checks, value constraints) and mark failures at the row level

* Evaluate aggregate checks (e.g. row count, min/max rules) against the full DataFrame

* Annotate the dataset with helper columns like ``_dq_passed``, ``_dq_errors``, ``_dq_validation_ts``

* Return a result object that you can query, summarize, and route as needed

This design allows you to embed validation logic directly into your ETL or data contract workflows, while
keeping the execution logic clean, reusable, and fully Spark-native.

Exploring the Result
--------------------

The BatchValidationResult provides multiple interfaces to work with the output:

.. csv-table::
    :header: "Method", "Description"

    "**pass_df()**", "Returns only records that passed all critical checks"
    "**fail_df()**", "Returns records that failed at least one critical check"
    "**warn_df()**", "Returns passing records that triggered one or more warning-level violations"
    "**summary()**", "Returns a structured summary with record counts, pass rate, and timestamp"

These methods make it easy to separate valid and invalid data or build dynamic branching logic within your
pipeline.

What's Inside the Result DataFrame?
-----------------------------------

sparkdq automatically annotates your DataFrame with additional columns:

* ``_dq_passed``: Boolean flag indicating whether the row passed all critical checks

* ``_dq_errors``: Array of structured errors for each failed check (name, check-id, severity)

* ``_dq_validation_ts``: Timestamp marking when the validation run was executed

This enriched metadata allows you to:

* ✅ Filter or route data based on severity and check type

* ✅ Track recurring issues over time using timestamps

* ✅ Build dashboards or alerts based on specific failure patterns

.. raw:: html

   <hr>

🚀 **Next Step**: You’ve validated your data—now decide how to respond.
