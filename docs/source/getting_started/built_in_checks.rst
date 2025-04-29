Built-In Checks
===============

SparkDQ includes various built-in checks for validating the integrity and quality of your data.
The following table lists all available checks with their identifiers and a brief
description — click on a check name to see details and examples.

All configuration examples are shown in YAML format for better readability.
You can still define checks via JSON, or external sources — as long as the
configuration is provided as a Python dictionary at runtime.

.. toctree::
   :maxdepth: 1
   :caption: Built-in Checks
   :hidden:

   checks/count/count_between_check
   checks/count/count_exact_check
   checks/count/count_min_check
   checks/count/count_max_check

   checks/date/date_between_check
   checks/date/date_min_check
   checks/date/date_max_check

   checks/null/not_null_check
   checks/null/null_check
   checks/numeric/numeric_min_check
   checks/numeric/numeric_max_check
   checks/numeric/numeric_between_check

   checks/timestamp/timestamp_min_check
   checks/timestamp/timestamp_max_check
   checks/timestamp/timestamp_between_check

Count Checks
------------

.. csv-table::
    :header: "Check", "Description"
    :widths: 20, 80

    ":ref:`count-min-check` ", "Ensures that the DataFrame contains at least a defined minimum number of rows."
    ":ref:`count-max-check` ", "Ensures that the DataFrame does not exceed a defined maximum number of rows."
    ":ref:`count-between-check` ", "Ensures that the number of rows in the dataset falls within a defined inclusive range."
    ":ref:`count-exact-check` ", "Ensures that the dataset contains exactly the specified number of rows."

Null Checks
-----------

.. csv-table::
    :header: "Check", "Description"
    :widths: 20, 80

    ":ref:`null_check` ", "Verifies whether a given column contains any null values."
    ":ref:`not_null_check` ", "Checks whether the specified column contains at least one non-null value."

Range Checks
------------

.. csv-table::
    :header: "Check", "Description"
    :widths: 20, 80

    ":ref:`numeric-min-check` ", "Ensures that numeric column values are greater than a defined minimum."
    ":ref:`numeric-max-check` ", "Ensures that numeric column values are less than a defined maximum."
    ":ref:`numeric-between-check` ", "Ensures that numeric column values are within a defined inclusive range."
    ":ref:`date-min-check` ", "Ensures that date column values are greater than a defined minimum date."
    ":ref:`date-max-check` ", "Ensures that date column values are less than a defined maximum date."
    ":ref:`date-between-check` ", "Ensures that date column values are within a defined range."
    ":ref:`timestamp-min-check` ", "Ensures that timestamp column values are greater than a defined timestamp."
    ":ref:`timestamp-max-check` ", "Ensures that timestamp column values are less than a defined maximum timestamp."
    ":ref:`timestamp-between-check` ", "Ensures that timestamp column values are within a defined inclusive range between a minimum and maximum timestamp."
