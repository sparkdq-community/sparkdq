[![CI Pipeline](https://github.com/sparkdq-community/sparkdq/actions/workflows/ci.yaml/badge.svg)](https://github.com/sparkdq-community/sparkdq/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/sparkdq-community/sparkdq/branch/main/graph/badge.svg?token=3TVZE8J2DN)](https://codecov.io/gh/sparkdq-community/sparkdq)
[![Docs](https://img.shields.io/badge/docs-online-green.svg)](https://sparkdq-community.github.io/sparkdq/)
[![PyPI version](https://badge.fury.io/py/sparkdq.svg)](https://pypi.org/project/sparkdq/)
[![Python Versions](https://img.shields.io/badge/python-3.11+%20-blue.svg)](https://github.com/sparkdq-community/sparkdq)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/sparkdq?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=BLUE&left_text=downloads)](https://pepy.tech/projects/sparkdq)

# SparkDQ — Data Quality Validation for Apache Spark

**SparkDQ** is a lightweight data quality framework built specifically for PySpark. Define checks declaratively or in Python, run them directly inside your Spark pipelines, and catch data issues before they reach production.

**No wrappers. No heavy dependencies. Just Python and Spark.**


## Quickstart

**Declarative** — define checks as dicts and load them from YAML, JSON, or any external system:

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
    {"check-id": "null-check", "check": "null-check", "columns": ["name"]},
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

**Python-native** — full type safety and IDE autocompletion:

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

🚀  See the [official documentation](https://sparkdq-community.github.io/sparkdq/) to learn more.

## Installation

### For Local Development / Standalone Clusters

Install with PySpark included:

```bash
pip install sparkdq[spark]
```

### For Databricks / Managed Platforms

Install without PySpark (runtime provided by platform):

```bash
pip install sparkdq
```

The framework supports Python 3.11+ and is fully tested with PySpark 3.5.x. SparkDQ will automatically check for PySpark availability on import and provide clear error messages if PySpark is missing in your environment.

## Why SparkDQ?

- ✅ **Extensible by design**: Add custom checks via a simple plugin system — no changes to the core required

- ✅ **Declarative or Pythonic**: YAML/JSON configs or type-safe Python — your choice

- ✅ **Severity-aware**: Distinguish between hard failures (CRITICAL) and soft constraints (WARNING)

- ✅ **Row-level and aggregate**: Validate individual records and entire datasets in a single pass

- ✅ **Minimal footprint**: Only Pydantic required — PySpark is provided by your platform

## Typical Use Cases

- **Data Ingestion**: Validate schema, check for nulls, enforce value ranges, and detect format violations before bad data enters your platform.

- **Lakehouse Quality**: Assert completeness, uniqueness, and referential integrity before writing to Delta, Iceberg, or Hudi tables — keep your lakehouse clean at the source.

- **ML & Analytics**: Validate feature completeness, numeric boundaries, and row counts before model training or reporting. Catch data issues before they silently corrupt your results.

- **Pipeline Assertions**: Enforce data contracts between pipeline stages. Fail fast on critical violations, log warnings for soft constraints, and keep your pipelines observable.

## Let’s Build Better Data Together

⭐️ Found this useful? Give it a star and help spread the word!

📣 Questions, feedback, or ideas? Open an issue or discussion — we’d love to hear from you.

🤝 Want to contribute? Check out [CONTRIBUTING.md](https://github.com/sparkdq-community/sparkdq/blob/main/CONTRIBUTING.md) to get started.
