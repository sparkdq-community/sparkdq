Aggregate Checks
================

.. toctree::
   :maxdepth: 1
   :caption: Built-in Checks
   :hidden:

   checks/schema/column_presence_check
   checks/count/count_between_check
   checks/count/count_exact_check
   checks/count/count_min_check
   checks/count/count_max_check
   checks/schema/schema_check
   checks/uniqueness/unique_rows_check

.. csv-table::
    :header: "Check", "Description"
    :widths: 20, 80

    ":ref:`count-min-check` ", "Ensures that the DataFrame contains at least a defined minimum number of rows."
    ":ref:`count-max-check` ", "Ensures that the DataFrame does not exceed a defined maximum number of rows."
    ":ref:`count-between-check` ", "Ensures that the number of rows in the dataset falls within a defined inclusive range."
    ":ref:`count-exact-check` ", "Ensures that the dataset contains exactly the specified number of rows."
    ":ref:`column-presence-check` ", "Verifies the existence of required columns in the DataFrame, independent of their data types."
    ":ref:`schema-check` ", "Ensures that a DataFrame matches an expected schema by verifying column names and data types, with optional strict enforcement against unexpected columns."
   ":ref:`unique-rows-check`", "Validates that all rows in a DataFrame are unique, either across all columns or a defined subset, helping to detect unintended duplication and enforce row-level uniqueness."
