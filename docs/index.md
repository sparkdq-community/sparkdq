---
hide:
  - navigation
  - toc
---

# SparkDQ — Data Quality Validation for Apache Spark

**SparkDQ** is a lightweight data quality framework built natively for PySpark. You describe what valid data looks like — declaratively via YAML/JSON or through a type-safe Python API — and it validates your DataFrame at row and aggregate level in a single pass, returning a structured result you can act on.

Its defining trait is what it leaves out. SparkDQ is intentionally small in scope and low in complexity — a focused set of checks, a declarative config layer, and a single-pass engine, with no metadata store, orchestration layer, or profiling engine to operate. That is a deliberate design choice: fewer moving parts to learn, fewer ways to misconfigure, and a codebase you can understand in an afternoon. For the large majority of pipelines — enforcing a known set of rules and routing good and bad records accordingly — this is exactly enough. If you need automated profiling or a full data-quality platform with its own UI and storage, a heavier tool is the better fit; SparkDQ trades that breadth for clarity.

That focus is what sets it apart from the alternatives: no JVM bridge like [PyDeequ](https://github.com/awslabs/python-deequ), no complexity overhead like [Great Expectations](https://github.com/great-expectations/great_expectations), and no platform lock-in like [Databricks dqx](https://github.com/databrickslabs/dqx). And when the built-in checks are not enough, you extend the framework via a plugin system without touching the core.

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

    Checks are plain dictionaries, so they can be loaded from anywhere — YAML or JSON files, a database, or an API:

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
    ```

=== "Python-native"

    Typed config classes give you full type safety, IDE autocompletion, and static analysis support:

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
    ```

Either way, `run_batch` produces the following summary:

```text
Validation Summary (2024-01-01 00:00:00)
Total records:   3
Passed records:  2
Failed records:  1
Warnings:        0
Pass rate:       67.00%
```

SparkDQ ships with a library of over 30 built-in checks, spanning null and completeness validation, numeric and date range constraints, string pattern matching, schema enforcement, uniqueness, and referential integrity.

## Why SparkDQ?

- **Small on purpose** — A focused scope and low complexity: quick to learn, hard to misconfigure, easy to maintain — and enough for most pipelines
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
