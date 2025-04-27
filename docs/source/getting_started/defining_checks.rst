Defining Checks
===============

To validate data with SparkDQ, you first define a set of data quality rules that describe what "good data"
looks like. These rules are called checks, and they can operate on either individual records or entire datasets.

SparkDQ is built to support both Python-native definitions and declarative configurations
(like YAML, JSON, databases) — depending on your team’s preferences and workflow setup.

Types of Checks
---------------

.. raw:: html

   <hr>

SparkDQ offers two types of checks, each suited to a different level of data validation.

**Row-Level Checks**

These validate each record individually. For example:

* Missing or null values (`NullCheck`)

* Invalid formats using regex (`RegexCheck`)

* Numeric bounds (`MinValueCheck`, `MaxValueCheck`)

If a row fails one of these checks, it is marked as invalid and annotated with failure context. This allows
you to isolate problematic rows for filtering, remediation, or quarantine.

**Aggregate Checks**

These evaluate the dataset as a whole (or grouped subsets), including:

* Record count constraints (`RowCountBetweenCheck`)

* Uniqueness across one or more columns (`DistinctCountCheck`)

* Distributional metrics (e.g. min/max/avg/ratio checks)

If an aggregate check fails, all rows are marked as failed — even if they appear valid individually. This
reflects scenarios like: "if the dataset has too few records, treat the whole batch as suspicious."

Check Severity
--------------

.. raw:: html

   <hr>

Each check can be assigned a severity level to control how violations should be interpreted:

* 🚨 **critical** (default): indicates a strict failure condition

* ⚠️ **warning**: indicates a non-blocking issue that should be recorded but not treated as invalid

The severity determines how the validation engine classifies results later on. Critical checks are typically
used to enforce quality gates, while warnings are useful for logging soft constraints or monitoring potential issues.

Python-native Configuration
---------------------------

.. raw:: html

   <hr>

For dynamic or code-driven use cases (e.g. notebooks, CI pipelines), you can define checks directly in Python
using type-safe config classes:

.. code-block:: python

    from sparkdq.checks import NullCheckConfig, RowCountBetweenCheckConfig
    from sparkdq.core import Severity
    from sparkdq.management import CheckSet

    check_set = CheckSet()
    check_set.add_check(
        NullCheckConfig(
            check_id="my-null-check", columns=["email"]
        )
    )
    check_set.add_check(
        RowCountBetweenCheckConfig(
            check_id="my-count-check", min_count=100, max_count=5000
        )
    )

Declarative Configuration (YAML / JSON)
---------------------------------------

.. raw:: html

   <hr>

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
      min_count: 100
      max_count: 5000

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

Mixed Usage & Internals
-----------------------

.. raw:: html

   <hr>

Both definition styles are fully compatible and can even be mixed in the same CheckSet. Internally, SparkDQ handles all checks the same way:

1. Checks are resolved via a central registry

2. Parameters are validated using Pydantic models

3. Each config is turned into a concrete check

4. Checks are executed by the validation engine

This means you can pick the approach that fits your use case — without sacrificing flexibility or consistency.

.. raw:: html

   <hr>

🚀 **Next Step**: Learn how to execute checks and understand results in the next section.