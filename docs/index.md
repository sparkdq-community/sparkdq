---
hide:
  - navigation
  - toc
---

# SparkDQ — Data Quality Validation for Apache Spark

Bad data is one of the most common and costly problems in data engineering. It silently corrupts downstream analytics, breaks ML models, and causes hard-to-debug failures deep inside your pipelines — often long after the damage is done.

**SparkDQ** gives you a simple, declarative way to catch these problems early. Define validation rules in Python or YAML, run them directly inside your existing PySpark pipelines, and get structured results you can act on — before bad data reaches production.

No extra infrastructure. No wrappers around your DataFrames. No lock-in. Just data quality checks that run where your data already lives.

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
