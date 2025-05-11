Defining Checks
===============

With SparkDQ, data validation starts by defining **checks** that describe what “good data” looks like.
These checks can target either individual records or entire datasets. SparkDQ
supports both Python-native definitions and declarative configurations (such as YAML, JSON, or
database-based setups), allowing you to choose the approach that best fits your team’s workflow.

Types of Checks
---------------

SparkDQ offers two types of checks, each suited to a different level of data validation.

Row-Level Checks
^^^^^^^^^^^^^^^^

These checks validate each record individually — for example, by detecting missing or null values,
matching patterns with regular expressions, or enforcing numeric boundaries such as minimum or
maximum values. If a row fails one of these checks, it is marked as invalid and annotated with
detailed failure context. This makes it easy to isolate problematic records for filtering,
remediation, or quarantine.

Aggregate Checks
^^^^^^^^^^^^^^^^

These checks evaluate the dataset as a whole — or within grouped subsets — by applying rules such
as record count constraints, uniqueness checks across one or more columns, or distributional
metrics like minimum, maximum, average, or ratio validations. If an aggregate check fails, all
rows are marked as invalid, even if they would pass individual checks. This reflects scenarios
where, for example, a dataset with too few records should cause the entire batch to be treated as suspicious.

Check Severity
--------------

Each check can be assigned a severity level to control how violations should be interpreted.

Critical (default)
^^^^^^^^^^^^^^^^^^

Critical checks represent hard failures. If a critical check fails, the affected data is considered invalid
and will be excluded from the result. This level is typically used for mandatory rules — e.g., primary key
constraints, schema integrity, or essential null checks.

Warning
^^^^^^^

Warning-level checks indicate potential issues that should be logged or monitored, but they do not
invalidate the data. Failed rows remain part of the valid output. This is useful for soft constraints,
such as unexpected value distributions or optional completeness requirements.

Python-native Configuration
---------------------------

For dynamic or code-driven use cases (e.g. notebooks, CI pipelines), you can define checks directly in Python
using type-safe config classes. The ``CheckSet`` supports both the classic and the fluent API style.

Fluent API (recommended):
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from sparkdq.checks import NullCheckConfig, RowCountBetweenCheckConfig
    from sparkdq.core import Severity
    from sparkdq.management import CheckSet

    check_set = (
        CheckSet()
        .add_check(
            NullCheckConfig(
                check_id="my-null-check",
                columns=["email"]
            )
        )
        .add_check(
            RowCountBetweenCheckConfig(
                check_id="my-count-check",
                min_count=100,
                max_count=5000
            )
        )
    )

Classic API (equivalent but more verbose):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from sparkdq.checks import NullCheckConfig, RowCountBetweenCheckConfig
    from sparkdq.core import Severity
    from sparkdq.management import CheckSet

    check_set = CheckSet()
    check_set.add_check(
        NullCheckConfig(
            check_id="my-null-check",
            columns=["email"]
        )
    )
    check_set.add_check(
        RowCountBetweenCheckConfig(
            check_id="my-count-check",
            min_count=100,
            max_count=5000
        )
    )

Declarative Configuration
-------------------------

If you use a metadata-driven or config-as-code approach, SparkDQ also supports declarative check
definitions via dictionaries — for example loaded from YAML or JSON files.

.. code-block:: yaml

    # dq_checks.yaml
    - check: null-check
      check-id: my-null-check
      columns:
        - email
      severity: warning

    - check: row-count-between-check
      check-id: my-count-check
      min-count: 100
      max-count: 5000

To load the configuration into SparkDQ, use the following code:

.. code-block:: python

    import yaml  # Optional: SparkDQ does not install pyyaml
    from sparkdq.management import CheckSet

    with open("dq_checks.yaml") as f:
        config = yaml.safe_load(f)

    check_set = CheckSet()
    check_set.add_checks_from_dicts(config)

**Note**: SparkDQ is intentionally designed to process plain Python dictionaries only — avoiding direct
dependencies on YAML, JSON, or database connectors. This lightweight, integration-friendly design ensures
that you stay in full control of how configurations are loaded, making it easy to plug SparkDQ into any
existing system or pipeline.

Unified Check Handling
----------------------

Both definition styles are fully compatible and can even be mixed in the same CheckSet. Internally, SparkDQ handles all checks the same way:

1. Checks are resolved via a central registry

2. Parameters are validated using Pydantic models

3. Each config is turned into a concrete check

4. Checks are executed by the validation engine

This means you can pick the approach that fits your use case — without sacrificing flexibility or consistency.

.. raw:: html

   <hr>

🚀 **Next Step**: Learn how to execute checks and understand results in the next section.