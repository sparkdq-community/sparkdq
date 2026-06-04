# Aggregate Checks

Aggregate checks evaluate the dataset as a whole. If an aggregate check fails, all rows are marked as invalid.

| Check                                                                     | Description                                                    |
| ------------------------------------------------------------------------- | -------------------------------------------------------------- |
| [Columns Are Complete](checks/completeness/columns_are_complete_check.md) | All specified columns contain no null values.                  |
| [Column Presence](checks/schema/column_presence_check.md)                 | All required columns exist in the DataFrame.                   |
| [Completeness Ratio](checks/completeness/completeness_ratio_check.md)     | Ratio of non-null values meets a minimum threshold.            |
| [Count Min](checks/count/count_min_check.md)                              | DataFrame contains at least a minimum number of rows.          |
| [Count Max](checks/count/count_max_check.md)                              | DataFrame does not exceed a maximum number of rows.            |
| [Count Between](checks/count/count_between_check.md)                      | Row count falls within a defined range.                        |
| [Count Exact](checks/count/count_exact_check.md)                          | DataFrame contains exactly the expected number of rows.        |
| [Distinct Ratio](checks/uniqueness/distinct_ratio_check.md)               | Ratio of distinct non-null values exceeds a defined threshold. |
| [Freshness](checks/freshness/freshness_check.md)                          | Most recent timestamp is within a defined freshness window.    |
| [Foreign Key](checks/integrity/foreign_key_check.md)                      | Column values exist in a reference dataset.                    |
| [Schema Check](checks/schema/schema_check.md)                             | DataFrame matches an expected schema.                          |
| [Unique Ratio](checks/uniqueness/unique_ratio_check.md)                   | Column maintains a minimum ratio of unique values.             |
| [Unique Rows](checks/uniqueness/unique_rows_check.md)                     | All rows in the DataFrame are unique.                          |
