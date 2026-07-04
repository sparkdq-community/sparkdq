# Introduction

SparkDQ follows a simple idea: you describe what valid data looks like, and the framework checks it for you — directly inside your Spark pipeline.

## Deliberately small

SparkDQ's defining trait is what it leaves out. It is intentionally small in scope and low in complexity: a focused set of row- and aggregate-level checks, a declarative configuration layer, and a single-pass engine — and little else. There is no metadata store, no orchestration layer, no profiling engine, and no data-docs pipeline to operate. You can read and understand the whole framework in an afternoon.

That smaller surface is a design choice, not a limitation to apologize for. It means fewer moving parts to learn, fewer ways to misconfigure, and a codebase that stays easy to extend and maintain. For the large majority of pipelines — where the job is to enforce a known set of rules and route good and bad records accordingly — this is exactly enough, and the reduced complexity is a feature in itself.

It is honest about where it stops: if you need automated data profiling, statistical anomaly detection, or a full data-quality platform with its own UI and storage, a heavier tool is the better fit. SparkDQ trades that breadth for clarity.

## How it fits into your pipeline

It is built for data engineers who work with PySpark and want a lightweight, non-invasive way to enforce data quality. There is no extra infrastructure, no external services, and no wrappers around your existing code. SparkDQ runs alongside your pipeline, validates your data in a single pass, and returns a structured result you can act on — whether that means stopping the pipeline, routing bad records to a quarantine zone, or simply logging what went wrong.

To do that, it relies on a few components that build on each other.

## Core Concepts

You start by defining a **CheckConfig** — a single validation rule, like "this column must not be null" or "the row count must be between 1,000 and 10,000". Each rule has its own config class with typed, validated parameters.

Multiple configs are collected in a **CheckSet**, which acts as the single source of truth for everything you want to validate in one run.

The **ValidationEngine** (e.g. `BatchDQEngine`) takes the CheckSet and a Spark DataFrame, applies all rules, and returns a **ValidationResult** — a structured object that gives you filtered views of passed, failed, and warning-level records, plus summary statistics like pass rate and timestamps.

## Example

```python
from sparkdq.checks import NullCheckConfig
from sparkdq.engine import BatchDQEngine
from sparkdq.management import CheckSet

check_set = CheckSet()
check_set.add_check(NullCheckConfig(check_id="no-nulls-in-email", columns=["email"]))

df = spark.read.parquet("/path/to/data")
result = BatchDQEngine(check_set).run_batch(df)

result.pass_df().show()
result.fail_df().show()
print(result.summary())
```
