# Integration Patterns

After running the validation engine, you need to decide what to do with the results. The right approach depends on how much trust your downstream systems require and how you want to handle bad data.

SparkDQ supports two common patterns: stopping the pipeline entirely when data quality is critical, or separating valid and invalid records so both can be handled independently. Both can be implemented with just a few lines of code.

## Fail-Fast

Stop the pipeline immediately if any critical check fails. No data is written downstream.

```python
if not result.summary().all_passed:
    raise RuntimeError("Critical checks failed — stopping pipeline.")
```

Use this when your downstream consumers require complete trust in the data, or when you operate in regulated domains where partial data is worse than no data.

## Quarantine

Route valid and invalid records to separate destinations. Failing records are enriched with `_dq_errors`, `_dq_passed`, and `_dq_validation_ts` — giving you full context for debugging and monitoring.

```python
result.pass_df().write.format("delta").save("/trusted-zone")
result.fail_df().write.format("delta").save("/quarantine-zone")
```

Use this when you want clean data to flow forward uninterrupted while preserving invalid records for inspection, remediation, or alerting.
