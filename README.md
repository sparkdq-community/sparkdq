# SparkDQ — Declarative Data Quality Validation for Apache Spark

Let’s be honest: Most data quality frameworks were never built for PySpark.

* ❌ They're not Spark-native — they treat PySpark as an afterthought, and often rely on `toPandas()` under the hood, causing memory issues on large datasets

* ❌ They don't support declarative pipelines — you're forced to build fragile wrappers around their logic just to use them in production

* ❌ They can't run inside your transformations — validation happens after the fact, so you can’t react dynamically or fail early

That’s where **SparkDQ** comes in. It’s a data quality framework built specifically for PySpark.   You define and run validation checks directly in your Spark pipelines — using Python, not SQL or custom DSLs.<br>
Whether you're checking ingestion data, verifying outputs before persistence, or enforcing assumptions in your dataflow: SparkDQ helps you catch issues early, without adding complexity.

## Quickstart Examples

SparkDQ lets you define checks either using a **Python-native** interface or via **declarative configuration** (e.g. YAML, JSON, or database-driven). Regardless of how you define them, all checks are added to a `CheckSet` — which you pass to the validation engine. That’s it! Choose the style that fits your use case, and SparkDQ takes care of the rest.

### Python-Native Approach

```python
from pyspark.sql import SparkSession

from sparkdq.checks import NullCheckConfig
from sparkdq.engine import BatchDQEngine
from sparkdq.management import CheckSet

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": None},
    {"id": 3, "name": "Bob"},
])

# Define checks using the Python-native interface (no external config needed)
check_set = CheckSet()
check_set.add_check(NullCheckConfig(check_id="my-null-check", columns=["name"]))

result = BatchDQEngine(check_set).run_batch(df)
print(result.summary())
```

### Declarative Approach

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

# Declarative configuration via dictionary
# Could be loaded from YAML, JSON, or any external system
check_definitions = [
    {"check-id": "my-null-check", "check": "null-check", "columns": ["name"]},
]
check_set = CheckSet()
check_set.add_checks_from_dicts(check_definitions)

result = BatchDQEngine(check_set).run_batch(df)
print(result.summary())
```

SparkDQ is designed to integrate seamlessly into real-world systems. Instead of relying on a custom DSL or
rigid schemas, it accepts plain Python dictionaries for check definitions. This makes it easy to load checks
from YAML or JSON files, configuration tables in databases, or even remote APIs — enabling smooth integration
into orchestration tools, CI pipelines, and data contract workflows.

## Installation

Install the latest stable version using pip:

```
pip install sparkdq
```

Alternatively, if you're using uv, a fast and modern Python package manager:

```
uv add sparkdq
```

The framework supports Python 3.10+ and is fully tested with PySpark 3.5.x. No additional Spark installation
is required when running inside environments like Databricks, AWS Glue, or EMR.

## Why SparkDQ?

* ✅ **Robust Validation Layer**: Clean separation of check definition, execution, and reporting

* ✅ **Declarative or Programmatic**: Define checks via config files or directly in Python

* ✅ **Severity-Aware**: Built-in distinction between warning and critical violations

* ✅ **Row & Aggregate Logic**: Supports both record-level and dataset-wide constraints

* ✅ **Typed & Tested**: Built with type safety, testability, and extensibility in mind

* ✅ **Zero Overhead**: Pure PySpark, no heavy dependencies

## Typical Use Cases

SparkDQ is built for modern data platforms that demand trust, transparency, and resilience.
It helps teams enforce quality standards early and consistently — across ingestion, transformation, and delivery layers.

Whether you're building a real-time ingestion pipeline or curating a data product for thousands of downstream users,
SparkDQ lets you define and execute checks that are precise, scalable, and easy to maintain.

Common Scenarios:

* ✅ Validating raw ingestion data

* ✅ Enforcing schema and content rules before persisting to a lakehouse (Delta, Iceberg, Hudi)

* ✅ Asserting quality conditions before analytics or ML training jobs

* ✅ Flagging critical violations in batch pipelines via structured summaries and alerts

* ✅ Driving Data Contracts: Use declarative checks in CI pipelines to catch issues before deployment

## Let’s Build Better Data Together

⭐️ Found this useful? Give it a star and help spread the word!

📣 Questions, feedback, or ideas? Open an issue or discussion — we’d love to hear from you.

🤝 Want to contribute? Check out [CONTRIBUTING.md](CONTRIBUTING.md) to get started.
