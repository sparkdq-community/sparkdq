---
hide:
  - navigation
  - toc
---

# SparkDQ — Data Quality Validation for Apache Spark

**SparkDQ** is a lightweight data quality framework built natively for PySpark — no JVM bridge like [PyDeequ](https://github.com/awslabs/python-deequ), no complexity overhead like [Great Expectations](https://github.com/great-expectations/great_expectations), and no platform lock-in like [Databricks dqx](https://github.com/databrickslabs/dqx). Define checks declaratively via YAML/JSON or through a type-safe Python API, validate at row and aggregate level in a single pass, and extend the framework via a plugin system without touching the core.

## Installation

=== "Local Development / Standalone Clusters"

    Install with PySpark included:

    ```bash
    pip install sparkdq[spark]
    ```

=== "Databricks / Managed Platforms"

    Install without PySpark (runtime provided by platform):

    ```bash
    pip install sparkdq
    ```

The framework supports Python 3.11+ and is fully tested with PySpark 3.5.x.

## Quickstart

=== "Declarative"

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": None},
            {"id": 3, "name": "Bob"},
        ]
    )

    check_set = CheckSet()
    check_set.add_checks_from_dicts([
        {"check": "null-check", "check-id": "no-null-name", "columns": ["name"]},
    ])

    result = BatchDQEngine(check_set).run_batch(df)
    print(result.summary())
    # Validation Summary (2024-01-01 00:00:00)
    # Total records:   3
    # Passed records:  2
    # Failed records:  1
    # Warnings:        0
    # Pass rate:       67.00%
    ```

=== "Python-native"

    Full type safety and IDE autocompletion:

    ```python
    from pyspark.sql import SparkSession
    from sparkdq.checks import NullCheckConfig
    from sparkdq.core import Severity
    from sparkdq.engine import BatchDQEngine
    from sparkdq.management import CheckSet

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": None},
            {"id": 3, "name": "Bob"},
        ]
    )

    check_set = (
        CheckSet()
        .add_check(NullCheckConfig(check_id="null-check", columns=["name"], severity=Severity.CRITICAL))
    )

    result = BatchDQEngine(check_set).run_batch(df)
    print(result.summary())
    # Validation Summary (2024-01-01 00:00:00)
    # Total records:   3
    # Passed records:  2
    # Failed records:  1
    # Warnings:        0
    # Pass rate:       67.00%
    ```

SparkDQ ships with 30+ built-in checks across null validation, numeric ranges, string patterns, date boundaries, schema enforcement, uniqueness, and referential integrity.

## Why SparkDQ?

- **Extensible by design** — Add custom checks via a simple plugin system, no changes to the core required
- **Declarative or Pythonic** — YAML/JSON configs or type-safe Python, your choice
- **Severity-aware** — Distinguish between hard failures (`CRITICAL`) and soft constraints (`WARNING`)
- **Row-level and aggregate** — Validate individual records and entire datasets in a single pass
- **Minimal footprint** — Only Pydantic required, PySpark is provided by your platform

---

## Support the Project

SparkDQ is open source and community-driven. If you find it useful, here's how you can help:

- [Star the repository](https://github.com/sparkdq-community/sparkdq) to show your support and help others discover it
- [Report bugs or issues](https://github.com/sparkdq-community/sparkdq/issues) to help us improve
- [Share ideas or feedback](https://github.com/sparkdq-community/sparkdq/discussions) — every suggestion counts
- [Contribute code or docs](https://github.com/sparkdq-community/sparkdq/blob/main/CONTRIBUTING.md) and become part of the project
