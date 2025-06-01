Aggregate Checks
================

.. toctree::
   :maxdepth: 1
   :caption: Built-in Checks
   :hidden:

   checks/completeness/columns_are_complete_check
   checks/schema/column_presence_check
   checks/completeness/completeness_ratio_check
   checks/count/count_between_check
   checks/count/count_exact_check
   checks/count/count_min_check
   checks/count/count_max_check
   checks/freshness/freshness_check
   checks/integrity/foreign_key_check
   checks/uniqueness/distinct_ratio_check
   checks/schema/schema_check
   checks/uniqueness/unique_ratio_check
   checks/uniqueness/unique_rows_check

.. csv-table::
    :header: "Check", "Description"
    :widths: 20, 80

   ":ref:`columns-are-complete-check`", "Validates that a set of columns are fully populated. If any nulls are detected in the specified columns, the entire DataFrame is marked as invalid."
   ":ref:`column-presence-check` ", "Verifies the existence of required columns in the DataFrame, independent of their data types."
   ":ref:`completeness-ratio-check`", "Validates that the ratio of non-null values in a column meets a minimum threshold, enabling soft completeness validation and early detection of partially missing data."
   ":ref:`count-min-check` ", "Ensures that the DataFrame contains at least a defined minimum number of rows."
   ":ref:`count-max-check` ", "Ensures that the DataFrame does not exceed a defined maximum number of rows."
   ":ref:`count-between-check` ", "Ensures that the number of rows in the dataset falls within a defined inclusive range."
   ":ref:`count-exact-check` ", "Ensures that the dataset contains exactly the specified number of rows."
   ":ref:`distinct-ratio-check`", "Validates that the ratio of distinct non-null values in a column exceeds a defined threshold, helping to detect overly uniform or low-cardinality fields."
   ":ref:`freshness-check`", "Validates that the most recent timestamp in a given column is within a defined freshness window relative to the current system time, helping detect outdated or stale data."
   ":ref:`foreign-key-check`", "Ensures that values in a given column exist in a reference dataset column to enforce referential integrity."
   ":ref:`schema-check` ", "Ensures that a DataFrame matches an expected schema by verifying column names and data types, with optional strict enforcement against unexpected columns."
   ":ref:`unique-ratio-check`", "Validates that a specified column maintains a minimum ratio of unique (non-null) values, helping to detect excessive duplication and assess data entropy or feature distinctiveness."
   ":ref:`unique-rows-check`", "Validates that all rows in a DataFrame are unique, either across all columns or a defined subset, helping to detect unintended duplication and enforce row-level uniqueness."
